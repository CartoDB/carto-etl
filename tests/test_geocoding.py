import pytest
import os

from geocoding import *

def test_error():
    #request_id = 'g109VcAbAFutjv18y9a1blWYQ7JexZGv'
    dir_path = os.path.dirname(os.path.realpath(__file__))
    request_id = None
    geocoding_job = HereGeocodingJob(csv_file_path=dir_path + '/../test_files/sample.csv', email='alrocar@cartodb.com', request_id=request_id)
    geocoding_job.download()