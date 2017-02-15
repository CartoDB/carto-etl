import csv
import ConfigParser
import logging

from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient
from carto.sql import BatchSQLClient


logger = logging.getLogger('carto-etl')

config = ConfigParser.RawConfigParser()
config.read("etl.conf")

CARTO_BASE_URL = config.get('carto', 'base_url')
CARTO_API_KEY = config.get('carto', 'api_key')
CARTO_TABLE_NAME = config.get('carto', 'table_name')
CARTO_COLUMNS = config.get('carto', 'columns')
CHUNK_SIZE = int(config.get('etl', 'chunk_size'))
MAX_ATTEMPTS = int(config.get('etl', 'max_attempts'))

api_auth = APIKeyAuthClient(CARTO_BASE_URL, CARTO_API_KEY)
sql = SQLClient(api_auth)
bsql = BatchSQLClient(api_auth)


def chunks(full_list, chunk_size, start_chunk=1, end_chunk=None):
    finished = False
    while finished is False:
        chunk = []
        for chunk_num in xrange(chunk_size):
            if chunk_num < (start_chunk - 1):
                continue

            if end_chunk is not None and chunk_num >= end_chunk:
                return

            try:
                chunk.append(full_list.next())
            except StopIteration:
                finished = True
                if len(chunk) > 0:
                    continue
                else:
                    return
        yield chunk


class UploadJob(object):
    def __init__(self, csv_file_path, x_column="longitude", y_column="latitude", srid=4326):
        self.csv_file_path = csv_file_path
        self.x_column = x_column
        self.y_column = y_column
        self.srid = srid

    def run(self):
        raise NotImplemented

    def regenerate_overviews(self):
        query = 'select CDB_CreateOverviews(\'{table}\'::regclass)'.format(table=CARTO_TABLE_NAME)
        job_result = bsql.create(query)
        return job_result['job_id']

    def check_job(self, job_id):
        return bsql.read(job_id)


class InsertJob(UploadJob):
    def run(self, start_chunk=1, end_chunk=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f)

            for chunk_num, record_chunk in enumerate(chunks(csv_reader, CHUNK_SIZE, start_chunk, end_chunk)):
                query = "insert into {table_name} (the_geom,{columns}) values".format(table_name=CARTO_TABLE_NAME, columns=CARTO_COLUMNS.lower())
                for record in record_chunk:
                    query += " (st_transform(st_setsrid(st_makepoint({longitude}, {latitude}), {srid}), 4326),".format(longitude=record[self.x_column], latitude=record[self.y_column], srid=self.srid)
                    for column in CARTO_COLUMNS.split(","):
                        try:
                            float(record[column])
                        except ValueError:
                            query += "'{value}',".format(value=record[column])
                        else:
                            query += "{value},".format(value=record[column])
                    query = query[:-1] + "),"

                query = query[:-1]
                logger.debug("Chunk #{chunk_num}: {query}".format(chunk_num=(chunk_num + 1), query=query))
                for retry in xrange(MAX_ATTEMPTS):
                    try:
                        sql.send(query)
                    except Exception as e:
                        logger.warning("Chunk #{chunk_num}: Retry ({error_msg})".format(chunk_num=(chunk_num + 1), error_msg=e))
                    else:
                        logger.info("Chunk #{chunk_num}: Success!".format(chunk_num=(chunk_num + 1)))
                        break
                else:
                    logger.error("Chunk #{chunk_num}: Failed ({error_msg})".format(chunk_num=(chunk_num + 1), error_msg=e))


class UpdateJob(UploadJob):
    def __init__(self, id_column, *args, **kwargs):
        self.id_column = id_column
        super(UpdateJob, self).__init__(*args, **kwargs)

    def run(self, start_row=1, end_row=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f)

            for row_num, record in enumerate(csv_reader):
                if row_num < (start_row - 1):
                    continue

                if end_row is not None and row_num >= end_row:
                    break

                query = "update {table_name} set ".format(table_name=CARTO_TABLE_NAME)
                query += " the_geom = st_transform(st_setsrid(st_makepoint({longitude}, {latitude}), {srid}), 4326),".format(longitude=record[self.x_column], latitude=record[self.y_column], srid=self.srid)
                for column in CARTO_COLUMNS.split(","):
                    if column == self.id_column:
                        continue

                    try:
                        float(record[column])
                    except ValueError:
                        query += "{column} = '{value}',".format(column=column, value=record[column])
                    else:
                        query += "{column} = {value},".format(column=column, value=record[column])
                query = query[:-1] + " where {id_column} = {id}".format(id_column=self.id_column, id=record[self.id_column])

                logger.debug("Row #{row_num}: {query}".format(row_num=(row_num + 1), query=query))
                for retry in xrange(MAX_ATTEMPTS):
                    try:
                        sql.send(query)
                    except Exception as e:
                        logger.error("Row #{row_num}: Retry ({error_msg})".format(row_num=(row_num + 1), error_msg=e))
                    else:
                        logger.info("Row #{row_num}: Success!".format(row_num=(row_num + 1)))
                        break
                else:
                    logger.error("Row #{row_num}: Failed ({error_msg})".format(row_num=(row_num + 1), error_msg=e))


class DeleteJob(UploadJob):
    def __init__(self, id_column, *args, **kwargs):
        self.id_column = id_column
        super(DeleteJob, self).__init__(*args, **kwargs)

    def run(self, start_chunk=1, end_chunk=None):
        with open(self.csv_file_path) as f:
            csv_reader = csv.DictReader(f)

            for chunk_num, record_chunk in enumerate(chunks(csv_reader, CHUNK_SIZE, start_chunk, end_chunk)):
                query = "delete from {table_name} where {column} in (".format(table_name=CARTO_TABLE_NAME, column=self.id_column.lower())
                for record in record_chunk:
                    try:
                        float(record[self.id_column])
                    except ValueError:
                        query += "'{value}',".format(value=record[self.id_column])
                    else:
                        query += "{value},".format(value=record[self.id_column])
                query = query[:-1] + ")"

                logger.debug("Chunk #{chunk_num}: {query}".format(chunk_num=(chunk_num + 1), query=query))
                for retry in xrange(MAX_ATTEMPTS):
                    try:
                        sql.send(query)
                    except Exception as e:
                        logger.warning("Chunk #{chunk_num}: Retry ({error_msg})".format(chunk_num=(chunk_num + 1), error_msg=e))
                    else:
                        logger.info("Chunk #{chunk_num}: Success!".format(chunk_num=(chunk_num + 1)))
                        break
                else:
                    logger.error("Chunk #{chunk_num}: Failed ({error_msg})".format(chunk_num=(chunk_num + 1), error_msg=e))
