import pytest

from etl.etl import UploadJob

config = {
    "carto": {
        "base_url": "http://wronguser123456.carto.com",
        "api_key": "",
        "table_name": "MYTABLE",
        "delimiter": ",",
        "columns": "",
        "date_columns": "date_col,date_col2,date_col3,date_col4,wrong_date_col,wrong_date_col2"
},
    "etl": {
        "chunk_size": 500,
        "max_attempts": 3,
        "file_encoding": "utf-8",
        "force_no_geometry": False,
        "force_the_geom": None,
        "date_format": "%d/%m/%Y",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
        "float_comma_separator": None,
        "float_thousand_separator": None,
        "x_column": "lon",
        "y_column": "lat",
        "srid": "4326"
},
    "log": {
        "file": "etl.log",
        "level": 30
    }
}

config_no_geometry = {
    "carto": {
        "base_url": "http://wronguser123456.carto.com",
        "api_key": "",
        "table_name": "MYTABLE",
        "delimiter": ",",
        "columns": "",
        "date_columns": "date_col,date_col2,date_col3,date_col4,wrong_date_col,wrong_date_col2"
},
    "etl": {
        "chunk_size": 500,
        "max_attempts": 3,
        "file_encoding": "utf-8",
        "force_no_geometry": True,
        "force_the_geom": None,
        "date_format": "%d/%m/%Y",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
        "float_comma_separator": None,
        "float_thousand_separator": None,
        "x_column": "lon",
        "y_column": "lat",
        "srid": "4326"
},
    "log": {
        "file": "etl.log",
        "level": 30
    }
}

config_wrong_geom = {
    "carto": {
        "base_url": "http://wronguser123456.carto.com",
        "api_key": "",
        "table_name": "MYTABLE",
        "delimiter": ",",
        "columns": "",
        "date_columns": "date_col,date_col2,date_col3,date_col4,wrong_date_col,wrong_date_col2"
},
    "etl": {
        "chunk_size": 500,
        "max_attempts": 3,
        "file_encoding": "utf-8",
        "force_no_geometry": False,
        "force_the_geom": None,
        "date_format": "%d/%m/%Y",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
        "float_comma_separator": None,
        "float_thousand_separator": None,
        "x_column": "wrong_lon",
        "y_column": "wrong_lat",
        "srid": "4326"
},
    "log": {
        "file": "etl.log",
        "level": 30
    }
}

config_force_the_geom = {
    "carto": {
        "base_url": "http://wronguser123456.carto.com",
        "api_key": "",
        "table_name": "MYTABLE",
        "delimiter": ",",
        "columns": "",
        "date_columns": "date_col,date_col2,date_col3,date_col4,wrong_date_col,wrong_date_col2"
},
    "etl": {
        "chunk_size": 500,
        "max_attempts": 3,
        "file_encoding": "utf-8",
        "force_no_geometry": False,
        "force_the_geom": "the_geom",
        "date_format": "%d/%m/%Y",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
        "float_comma_separator": None,
        "float_thousand_separator": None,
        "x_column": "lon",
        "y_column": "lat",
        "srid": "4326"
},
    "log": {
        "file": "etl.log",
        "level": 30
    }
}

config_float = {
    "carto": {
        "base_url": "http://wronguser123456.carto.com",
        "api_key": "",
        "table_name": "MYTABLE",
        "delimiter": ",",
        "columns": "",
        "date_columns": "date_col,date_col2,date_col3,date_col4,wrong_date_col,wrong_date_col2"
},
    "etl": {
        "chunk_size": 500,
        "max_attempts": 3,
        "file_encoding": "utf-8",
        "force_no_geometry": False,
        "force_the_geom": None,
        "date_format": "%d/%m/%Y",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
        "float_comma_separator": ",",
        "float_thousand_separator": ".",
        "x_column": "lon",
        "y_column": "lat",
        "srid": "4326"
},
    "log": {
        "file": "etl.log",
        "level": 30
    }
}

def flatten(config, kwargs):
    for key in config:
        if isinstance(config[key], dict):
            flatten(config[key], kwargs)
        else:
            kwargs[key] = config[key]
    return kwargs

@pytest.fixture(scope="session")
def upload_job():
    kwargs = flatten(config, {})
    return UploadJob("test.csv", **kwargs)

@pytest.fixture(scope="session")
def upload_job_args():
    return UploadJob("test.csv", base_url=None, api_key=None, table_name=None, delimiter=",", columns=None, date_columns=None, float_comma_separator=".", float_thousand_separator=None, file_encoding="utf-8", chunk_size=10000, max_attempts=3, force_no_geometry=False, force_the_geom=False, x_column="longitude", y_column="latitude", srid=4326, file="carto-etl.log", level=30)

@pytest.fixture(scope="session")
def upload_job_no_geometry():
    kwargs = flatten(config_no_geometry, {})
    return UploadJob("test.csv", **kwargs)

@pytest.fixture(scope="session")
def upload_job_wrong_geom():
    kwargs = flatten(config_wrong_geom, {})
    return UploadJob("test.csv", **kwargs)

@pytest.fixture(scope="session")
def upload_job_force_the_geom():
    kwargs = flatten(config_force_the_geom, {})
    return UploadJob("test.csv", **kwargs)

@pytest.fixture(scope="session")
def upload_job_float():
    kwargs = flatten(config_float, {})
    return UploadJob("test.csv", **kwargs)

@pytest.fixture(scope="session")
def record():
    return {
        "lon": "1",
        "lat": "2",
        "text_col": "a",
        "int_col": "1",
        "float_col": "1.0",
        "float_comma_col": "1,5",
        "escape_col": "t'est",
        "wrong_lon": "181",
        "wrong_lat": "91",
        "unescapable": 1,
        "the_geom": "123123123",
        "date_col": "01/09/2017 2:47:25",
        "date_col2": "01/09/2017",
        "date_col3": "01/09/2017 22:47:25",
        "date_col4": "01-09-2017 2:47:25",
        "wrong_date_col": "zzz",
        "wrong_date_col2": ""
    }
