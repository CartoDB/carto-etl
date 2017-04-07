import pytest
import os
import time

from geocoding import *

def test_error():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    request_id = None
    geocoding_job = HereGeocodingJob(csv_file_path=dir_path + '/../test_files/sample.csv', email='alrocar@cartodb.com', request_id=request_id)
    if request_id is not None:
        assert geocoding_job.status is None
    else:
        assert geocoding_job.status == 'accepted' or geocoding_job.status == 'completed'
    
    if geocoding_job.status != 'completed':
        # give 10 seconds to finish
        time.sleep(10)
        geocoding_job.refresh()

        assert geocoding_job.status == 'completed' or geocoding_job.status == 'running'