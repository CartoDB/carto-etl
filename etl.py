import csv
import configparser
import logging
from builtins import range

from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient
from carto.sql import BatchSQLClient

config = configparser.RawConfigParser()
config.read("etl.conf")

CARTO_BASE_URL = config.get('carto', 'base_url')
CARTO_API_KEY = config.get('carto', 'api_key')
CARTO_TABLE_NAME = config.get('carto', 'table_name')
CARTO_DELIMITER = config.get('carto', 'delimiter')
CARTO_COLUMNS = config.get('carto', 'columns')
FILE_ENCODING = config.get('etl', 'file_encoding')
CHUNK_SIZE = int(config.get('etl', 'chunk_size'))
MAX_ATTEMPTS = int(config.get('etl', 'max_attempts'))
FORCE_NO_GEOMETRY = config.getboolean('etl', 'force_no_geometry')
LOG_FILE = config.get('log', 'file')
LOG_LEVEL = int(config.get('log', 'level'))

api_auth = APIKeyAuthClient(CARTO_BASE_URL, CARTO_API_KEY)
sql = SQLClient(api_auth)
bsql = BatchSQLClient(api_auth)
UTF8 = "utf-8"
DEFAULT_COORD = None
MAX_LON = 180
MAX_LAT = 90
NULL_VALUE = "NULL"

logging.basicConfig(filename=LOG_FILE, filemode='w', level=LOG_LEVEL)
logger = logging.getLogger('carto-etl')


def chunks(full_list, chunk_size, start_chunk=1, end_chunk=None):
    finished = False
    while finished is False:
        chunk = []
        for chunk_num in range(chunk_size):
            if chunk_num < (start_chunk - 1):
                continue

            if end_chunk is not None and chunk_num >= end_chunk:
                return

            try:
                chunk.append(next(full_list))
            except StopIteration:
                finished = True
                if len(chunk) > 0:
                    continue
                else:
                    return
        yield chunk


class UploadJob(object):
    def __init__(self, csv_file_path, x_column="longitude",
                 y_column="latitude", srid=4326, file_encoding=FILE_ENCODING,
                 force_no_geometry=FORCE_NO_GEOMETRY):
        self.csv_file_path = csv_file_path
        self.x_column = x_column
        self.y_column = y_column
        self.srid = srid
        self.file_encoding = file_encoding
        self.force_no_geometry = force_no_geometry

    def run(self):
        raise NotImplemented

    def regenerate_overviews(self):
        query = 'select CDB_CreateOverviews(\'{table}\'::regclass)'.\
            format(table=CARTO_TABLE_NAME)
        job_result = bsql.create(query)
        return job_result['job_id']

    def check_job(self, job_id):
        return bsql.read(job_id)

    def create_geom_query(self, record):
        null_result = NULL_VALUE + ","
        if self.force_no_geometry:
            return null_result

        longitude = self.get_longitude(record)
        latitude = self.get_latitude(record)

        if longitude is None or latitude is None \
            or longitude is DEFAULT_COORD or latitude is DEFAULT_COORD:
            return null_result

        return "st_transform(st_setsrid(st_makepoint(" + \
            "{longitude}, {latitude}), {srid}), 4326),".\
            format(longitude=longitude, latitude=latitude, srid=self.srid)

    def parse_column_value(self, record, column):
        null_result = NULL_VALUE + ","

        try:
            value = record[column]
        except KeyError:
            return null_result

        try:
            result = "{value},".format(value=float(value))
        except (ValueError, TypeError):
            if value is None or not value.strip():
                result = null_result
            else:
                result = "'{value}',".format(value=value)
        return result


    def get_longitude(self, record):
        try:
            longitude = self.get_coord(record, self.x_column)
            if abs(longitude) > MAX_LON:
                return None
        except TypeError:
            return DEFAULT_COORD
        return longitude

    def get_latitude(self, record):
        try:
            latitude = self.get_coord(record, self.y_column)
            if abs(latitude) > MAX_LAT:
                return None
        except TypeError:
            return DEFAULT_COORD
        return latitude

    def get_coord(self, record, type):
        try:
            coord = float(record[type]) or DEFAULT_COORD
        except (ValueError, KeyError):
            coord = DEFAULT_COORD
        return coord

    def send(self, query, file_encoding, chunk_num):
        query = query.decode(file_encoding).encode(UTF8)
        logger.info("Chunk #{chunk_num}: {query}".
                    format(chunk_num=(chunk_num + 1), query=query))
        for retry in range(MAX_ATTEMPTS):
            try:
                sql.send(query)
            except Exception as e:
                logger.warning("Chunk #{chunk_num}: Retrying ({error_msg})".
                               format(chunk_num=(chunk_num + 1), error_msg=e))
            else:
                logger.info("Chunk #{chunk_num}: Success!".
                            format(chunk_num=(chunk_num + 1)))
                break
        else:
            logger.error("Chunk #{chunk_num}: Failed!)".
                         format(chunk_num=(chunk_num + 1)))


class InsertJob(UploadJob):
    def run(self, start_chunk=1, end_chunk=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=CARTO_DELIMITER)

            for chunk_num, record_chunk in enumerate(
                    chunks(csv_reader, CHUNK_SIZE, start_chunk, end_chunk)):
                cols = CARTO_COLUMNS.lower()
                query = "insert into {table_name} (the_geom,{columns}) values".\
                    format(table_name=CARTO_TABLE_NAME, columns=cols)
                for record in record_chunk:
                    query += " (" + self.create_geom_query(record)
                    for column in CARTO_COLUMNS.split(","):
                        query += self.parse_column_value(record, column)
                    query = query[:-1] + "),"

                query = query[:-1]
                self.send(query, self.file_encoding, chunk_num)


class UpdateJob(UploadJob):
    def __init__(self, id_column, *args, **kwargs):
        self.id_column = id_column
        super(UpdateJob, self).__init__(*args, **kwargs)

    def run(self, start_row=1, end_row=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=CARTO_DELIMITER)

            for row_num, record in enumerate(csv_reader):
                if row_num < (start_row - 1):
                    continue

                if end_row is not None and row_num >= end_row:
                    break

                query = "update {table_name} set ".\
                        format(table_name=CARTO_TABLE_NAME)
                query += " the_geom = " + self.create_geom_query(record)
                for column in CARTO_COLUMNS.split(","):
                    if column == self.id_column:
                        continue

                    value = self.parse_column_value(record, column)
                    query += "{column} = ".format(column=column) + value
                try:
                    id_value = record[self.id_column]
                    float(id_value)
                except ValueError:
                    query = query[:-1] + " where {id_column} = '{id}'".\
                        format(id_column=self.id_column, id=id_value)
                else:
                    query = query[:-1] + " where {id_column} = {id}".\
                        format(id_column=self.id_column, id=id_value)

                self.send(query, self.file_encoding, row_num)


class DeleteJob(UploadJob):
    def __init__(self, id_column, *args, **kwargs):
        self.id_column = id_column
        super(DeleteJob, self).__init__(*args, **kwargs)

    def run(self, start_chunk=1, end_chunk=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=CARTO_DELIMITER)

            for chunk_num, record_chunk in enumerate(
                    chunks(csv_reader, CHUNK_SIZE, start_chunk, end_chunk)):
                id_column = self.id_column.lower()
                query = "delete from {table_name} where {column} in (".\
                    format(table_name=CARTO_TABLE_NAME, column=id_column)
                for record in record_chunk:
                    query += self.parse_column_value(record, self.id_column)
                query = query[:-1] + ")"

                self.send(query, self.file_encoding, chunk_num)
