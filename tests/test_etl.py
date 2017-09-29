import pytest

from etl import UploadJob


def test_config_ok():
    assert 1 == 1

def test_upload_job(upload_job):
    assert upload_job is not None

def test_upload_job_no_geometry(upload_job_no_geometry):
    assert upload_job_no_geometry is not None

def test_escape_single_quote(upload_job, record):
    assert upload_job.escape_value(record["escape_col"]) == "t''est"

def test_escape_value(upload_job, record):
    assert upload_job.escape_value(record["text_col"]) == record["text_col"]

def test_get_longitude(upload_job, record):
    assert upload_job.get_longitude(record) == 1.0

def test_get_latitude(upload_job, record):
    assert upload_job.get_latitude(record) == 2.0

def test_wrong_longitude(upload_job_wrong_geom, record):
    assert upload_job_wrong_geom.get_longitude(record) == None

def test_wrong_latitude(upload_job_wrong_geom, record):
    assert upload_job_wrong_geom.get_latitude(record) == None

def test_parse_text_column(upload_job, record):
    assert upload_job.parse_column_value(record, "text_col") == "'a',"

def test_parse_non_existent_column(upload_job, record):
    assert upload_job.parse_column_value(record, "non_existent") == "NULL,"

def test_parse_unescapable_column(upload_job, record):
    assert upload_job.parse_column_value(record, "unescapable") == "NULL,"

def test_parse_int_column(upload_job, record):
    assert upload_job.parse_column_value(record, "int_col") == "1.0,"

def test_parse_float_column(upload_job, record):
    assert upload_job.parse_column_value(record, "float_col") == "1.0,"

def test_parse_float_comma_column(upload_job_float, record):
    assert upload_job_float.parse_column_value(record, "float_comma_col") == "1.5,"

def test_create_geom_query_no_geometry(upload_job_no_geometry, record):
    assert upload_job_no_geometry.create_geom_query(record) == "NULL,"

def test_create_wrong_geom_query(upload_job_wrong_geom, record):
    assert upload_job_wrong_geom.create_geom_query(record) == "NULL,"

def test_create_geom_query(upload_job, record):
    assert upload_job.create_geom_query(record) == "st_transform(st_setsrid(st_makepoint(1.0, 2.0), 4326), 4326),"

def test_create_the_geom_query(upload_job_force_the_geom, record):
    assert upload_job_force_the_geom.create_geom_query(record) == "'123123123',"
def test_parse_date(upload_job, record):
    assert upload_job.parse_column_value(record, "date_col") == "'2017-09-01 02:47:25+00',"

def test_parse_wrong_date(upload_job, record):
    assert upload_job.parse_column_value(record, "wrong_date_col") == "NULL,"

def test_parse_wrong_date2(upload_job, record):
    assert upload_job.parse_column_value(record, "wrong_date_col2") == "NULL,"
