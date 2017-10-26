import csv
import sys
import logging
from builtins import range
from datetime import datetime

from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient
from carto.sql import BatchSQLClient

UTF8 = "utf-8"
DEFAULT_COORD = None
MAX_LON = 180
MAX_LAT = 90
NULL_VALUE = "NULL"
CARTO_DATE_FORMAT = "%Y-%m-%d %H:%M:%S+00"

DEFAULT_DELIMITER = ","
DEFAULT_X_COLUMN = "lon"
DEFAULT_Y_COLUMN = "lat"
DEFAULT_SRID = 4326
DEFAULT_CHUNK_SIZE=1000
DEFAULT_MAX_ATTEMPTS=3
DEFAULT_FILE_ENCOFING=UTF8
DEFAULT_FORCE_NO_GEOMETRY=False
DEFAULT_FORCE_THE_GEOM=None
DEFAULT_DATE_FORMAT=None
DEFAULT_DATETIME_FORMAT=None
DEFAULT_FLOAT_COMMA_SEPARATOR=None
DEFAULT_FLOAT_THOUSAND_SEPARATOR=None
DEFAULT_DATE_COLUMNS=None

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

def _count(stream):
    lines = 0
    for line in stream:
        lines += 1
    stream.seek(0)
    return lines

def reencode(file, file_encoding):
    for line in file:
        yield line.decode(file_encoding).encode(UTF8)


class InsensitiveDictReader(csv.DictReader):
    @property
    def fieldnames(self):
        return [field.strip().lower() for field in csv.DictReader.fieldnames.fget(self)]

    def next(self):
        return InsensitiveDict(csv.DictReader.next(self))


class InsensitiveDict(dict):
    # This class overrides the __getitem__ method to automatically strip() and lower() the input key

    def __getitem__(self, key):
        return dict.__getitem__(self, key.strip().lower())


class UploadJob(object):
    def __init__(self, csv_file_path, **kwargs):
        self.__set_max_csv_length()
        self.__set_defaults()
        for key, value in kwargs.items():
            try:
                setattr(self, key, int(value))
            except (ValueError, TypeError):
                if value in ("true", "True"):
                    setattr(self, key, True)
                elif value in ("false", "False"):
                    setattr(self, key, False)
                else:
                    setattr(self, key, value)

        self.__trim_columns()

        self.csv_file_path = csv_file_path

        if self.api_key:
            self.api_auth = APIKeyAuthClient(self.base_url, self.api_key)
            self.sql = SQLClient(self.api_auth)
            self.bsql = BatchSQLClient(self.api_auth)

    def __set_defaults(self):
        self.delimiter = DEFAULT_DELIMITER
        self.x_column = DEFAULT_X_COLUMN
        self.y_column = DEFAULT_Y_COLUMN
        self.srid = DEFAULT_SRID
        self.chunk_size = DEFAULT_CHUNK_SIZE
        self.max_attempts = DEFAULT_MAX_ATTEMPTS
        self.file_encoding = DEFAULT_FILE_ENCOFING
        self.force_no_geometry = DEFAULT_FORCE_NO_GEOMETRY
        self.force_the_geom = DEFAULT_FORCE_THE_GEOM
        self.date_format = DEFAULT_DATE_FORMAT
        self.datetime_format = DEFAULT_DATETIME_FORMAT
        self.float_comma_separator = DEFAULT_FLOAT_COMMA_SEPARATOR
        self.float_thousand_separator = DEFAULT_FLOAT_THOUSAND_SEPARATOR
        self.date_columns = DEFAULT_DATE_COLUMNS
        self.observer = None

    def __set_max_csv_length(self):
        maxInt = sys.maxsize
        decrement = True

        while decrement:
            # decrease the maxInt value by factor 10
            # as long as the OverflowError occurs.
            decrement = False
            try:
                csv.field_size_limit(maxInt)
            except OverflowError:
                maxInt = int(maxInt/10)
                decrement = True

    def __trim_columns(self):
        if self.columns is not None:
            self.columns = self.columns.replace(' ', '')

        if self.date_columns is not None:
            self.date_columns = self.date_columns.replace(' ', '')

    def run(self, start_chunk=1, end_chunk=None):
        if not isinstance(self.csv_file_path, str):
            self.do_run(self.csv_file_path, start_chunk, end_chunk)
        else:
            if sys.version_info <= (3, 0):
                with open(self.csv_file_path) as f:
                    self.do_run(f, start_chunk, end_chunk)
            else:
                with open(self.csv_file_path, encoding=self.file_encoding) as f:
                    self.do_run(f, start_chunk, end_chunk)

    def notify(self, message_type, message):
        observer = getattr(self, "observer", None)
        if callable(observer):
            observer({"type": message_type, "msg": str(message)})
            return True
        return False

    def regenerate_overviews(self):
        query = 'select CDB_CreateOverviews(\'{table}\'::regclass)'.\
            format(table=self.table_name)
        job_result = self.bsql.create(query)
        return job_result['job_id']

    def check_job(self, job_id):
        return self.bsql.read(job_id)

    def create_geom_query(self, record):
        null_result = NULL_VALUE + ","
        if self.force_the_geom:
            return self.parse_column_value(record, self.force_the_geom, parse_float=False)

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

    def parse_column_value(self, record, column, parse_float=True):
        null_result = NULL_VALUE + ","

        try:
            value = self.escape_value(record[column])
        except Exception:
            return null_result

        try:
            if self.is_date_column(column):
                try:
                    result = "'{value}',".format(value=self.parse_date_column(record, column))
                except ValueError:
                    result = null_result
            elif parse_float:
                result = "{value},".format(value=self.parse_float_value(value))
            else:
                raise TypeError
        except (ValueError, TypeError):
            if value is None or not value.strip():
                result = null_result
            else:
                result = "'{value}',".format(value=value)
        return result

    def is_date_column(self, column):
        return column is not None and self.date_columns is not None and column in self.date_columns.split(',')

    def parse_date_column(self, record, column):
        if not self.date_format or not self.datetime_format:
            raise ValueError
        try:
            return datetime.strptime(record[column], self.datetime_format).strftime(CARTO_DATE_FORMAT)
        except Exception:
            try:
                return datetime.strptime(record[column], self.date_format).strftime(CARTO_DATE_FORMAT)
            except Exception:
                raise ValueError

    def escape_value(self, value):
        return value.replace("'", "''")

    def get_longitude(self, record):
        try:
            longitude = self.get_coord(record, self.x_column)
            if abs(longitude) > MAX_LON:
                return None
        except TypeError:
            return DEFAULT_COORD
        else:
            return longitude

    def get_latitude(self, record):
        try:
            latitude = self.get_coord(record, self.y_column)
            if abs(latitude) > MAX_LAT:
                return None
        except TypeError:
            return DEFAULT_COORD
        else:
            return latitude

    def get_coord(self, record, type):
        try:
            coord = self.parse_float_value(record[type]) or DEFAULT_COORD
        except (ValueError, KeyError):
            coord = DEFAULT_COORD
        else:
            return coord

    def parse_float_value(self, value):
        if self.float_thousand_separator:
            value = value.replace(self.float_thousand_separator, "")
        if self.float_comma_separator:
            value = value.replace(self.float_comma_separator, ".")
        return float(value)

    def send(self, query, file_encoding, chunk_num):
        if sys.version_info <= (3, 0):
            query = query.decode(file_encoding).encode(UTF8)
        logger.debug("Chunk #{chunk_num}: {query}".
                    format(chunk_num=(chunk_num + 1), query=query))
        for retry in range(self.max_attempts):
            try:
                self.sql.send(query)
            except Exception as e:
                logger.warning("Chunk #{chunk_num}: Retrying ({error_msg})".
                               format(chunk_num=(chunk_num + 1), error_msg=e))
                self.notify('error', e)
            else:
                logger.info("Chunk #{chunk_num}: Success!".
                            format(chunk_num=(chunk_num + 1)))
                self.notify('progress', chunk_num + 1)
                break
        else:
            logger.error("Chunk #{chunk_num}: Failed!)".
                         format(chunk_num=(chunk_num + 1)))
            self.notify('error', "Failed " + str(chunk_num + 1))


class InsertJob(UploadJob):
    def do_run(self, stream, start_chunk, end_chunk):
        self.notify('total_rows', _count(stream) / int(self.chunk_size))
        csv_reader = InsensitiveDictReader(stream, delimiter=self.delimiter)
        for chunk_num, record_chunk in enumerate(
                    chunks(csv_reader, self.chunk_size, start_chunk, end_chunk)):
            cols = self.columns.lower()
            query = "insert into {table_name} (the_geom,{columns}) values".\
                format(table_name=self.table_name, columns=cols)
            for record in record_chunk:
                query += " (" + self.create_geom_query(record)
                for column in self.columns.split(","):
                    query += self.parse_column_value(record, column)
                query = query[:-1] + "),"

            query = query[:-1]
            self.send(query, self.file_encoding, chunk_num)


class UpdateJob(UploadJob):
    def __init__(self, id_column, *args, **kwargs):
        self.id_column = id_column
        super(UpdateJob, self).__init__(*args, **kwargs)

    def do_run(self, stream, start_row=1, end_row=None):
        import ipdb; ipdb.set_trace(context=30)
        self.notify('total_rows', _count(stream))
        csv_reader = InsensitiveDictReader(stream, delimiter=self.delimiter)

        for row_num, record in enumerate(csv_reader):
            if row_num < (start_row - 1):
                continue

            if end_row is not None and row_num >= end_row:
                break

            query = "update {table_name} set ".\
                    format(table_name=self.table_name)
            query += " the_geom = " + self.create_geom_query(record)
            for column in self.columns.split(","):
                if column == self.id_column:
                    continue

                value = self.parse_column_value(record, column)
                query += "{column} = ".format(column=column) + value
            try:
                id_value = record[self.id_column]
                self.parse_float_value(id_value)
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

    def do_run(self, stream, start_chunk, end_chunk):
        self.notify('total_rows', _count(stream) / int(self.chunk_size))
        csv_reader = InsensitiveDictReader(stream, delimiter=self.delimiter)

        for chunk_num, record_chunk in enumerate(
                chunks(csv_reader, self.chunk_size, start_chunk, end_chunk)):
            id_column = self.id_column.lower()
            query = "delete from {table_name} where {column} in (".\
                format(table_name=self.table_name, column=id_column)
            for record in record_chunk:
                query += self.parse_column_value(record, self.id_column)
            query = query[:-1] + ")"

            self.send(query, self.file_encoding, chunk_num)
