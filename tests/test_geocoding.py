import pytest

from geocoding import *

def test_error():
    request_id = 'g109VcAbAFutjv18y9a1blWYQ7JexZGv'
    geocoding_job = HereGeocodingJob(csv_file_path='/Users/alrocar/Documents/CartoDB/carto-etl/test_files/sample_bbva.csv', email='alrocar@cartodb.com', request_id=request_id)
    geocoding_job.download()