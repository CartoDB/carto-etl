import pytest
import os
import time

from geocoding import *

def test_geocoding():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    request_id = None
    geocoding_job = HereGeocodingJob(csv_file_path=dir_path + '/../test_files/random3', email='alrocar@cartodb.com', request_id=request_id)
    if request_id is not None:
        assert geocoding_job.status is None
    else:
        assert geocoding_job.status == 'accepted' or geocoding_job.status == 'completed'
    
    i = 0
    count = 3
    while geocoding_job.status != 'completed' and i != count:
        # give 10 seconds to finish
        time.sleep(10)
        i += 1
        geocoding_job.refresh()

    geocoding_job.download()

    assert geocoding_job.status == 200