# SDK for ETL with CARTO's SQL API

## Installation and usage

Ideally, the SDK should be installed on a separate Python virtual environment, for instance:

```
$ virtualenv env
```

Before working with the SDK, the envirenment needs to be activated:

```
$ source env/bin/activate
```

The first time, you need to install the dependencies:

```
$ pip install -r requirements.txt
```

Now, you can start an interactive session with _IPython_ or create a _.py_ file to be run separately.

## Configuration file

There is a template file `etl.conf.example` that can be used to get the final `etl.conf` file. Example of a `etl.conf` file:

```
[carto]
base_url=https://cartouser.carto.com/api/
api_key=5565dgfgfd2b8ajjhgjgfa94d311aa60lk89vnber45errfg5gb
table_name=samples
columns=object_id,privacy,resource_type,country_code

[etl]
chunk_size=500
max_retries=3

[log]
file=etl.log
level=debug

[geocoding]
input_delimiter=,
output_delimiter=,
output_columns=recId,displayLatitude,displayLongitude,locationLabel,houseNumber,street,district,city,postalCode,county,state,country,relevance
max_results=1
```

Parameters:

* Related to the CARTO account:
  * `base_url`: CARTO API endpoint root for the user.
  * `api_key`: API key for the CARTO user.
  * `table_name`: Name of the target table in CARTO.
  * `columns`: Columns of the CSV file that will be transferred to CARTO.
* Related to ETL:
  * `chunk_size`: Number of items to be grouped on a single INSERT or DELETE request. POST requests can deal with several MBs of data (i.e. characters), so this number can go quite high if you wish.
  * `max_attempts`: Number of attempts before giving up on a API request to CARTO.
* Related to logging:
  * `file`: File name (or path) to the log file.
  * `level`: Log level for the log file, one of "debug", "info", "warn", "error" or "critical".
* Related to geocoding:
  * `input_delimiter`: The field delimiter in the input CSV for the batch geocoding job
  * `output_delimiter`: The field delimiter to be used for the output geocoded CSV
  * `output_columns`: The output columns that will appear in the output geocoded CSV. See (HERE API docs)[https://developer.here.com/rest-apis/documentation/batch-geocoder/topics/data-output.html]
  * `max_results`: Max number of results per address in the input CSV

## ETL

**Important notice**: The ETL script assumes the target table already exists in CARTO, and has the appropriate column definitions. The best way to achieve this is by uploading a small sample directly with Builder and then truncating the table before actually using the ETL script.

### Insert new items into CARTO

```python
from etl import *

job = InsertJob("my_new_samples.csv")
job.run()
```

`InsertJob` can be created with these parameters:
* `csv_file_path`: Path to the CSV file.
* `x_column`: CSV column where the X coordinate can be found. Defaults to "longitude".
* `y_column`: CSV column where the Y coordinate can be found. Defaults to "latitude".
* `srid`: SRID of the coordinates. Defaults to "4326".

The `run` method can be called with this parameters:
* `start_chunk`: First chunk to load from the CSV file. Defaults to "1", i.e., start from the beginning.
* `end_chunk`: Last chunk to load from the CSV file. Defaults to "None", i.e., keep going until the end of the file.

### Update existing items in CARTO

```python
from etl import *

job = UpdateJob("object_id", "my_existing_samples.csv")
job.run()
```

`UpdateJob` can be created with these parameters:
* `id_column`: Name of the column that will be used to match the records in CARTO.
* `csv_file_path`: Path to the CSV file.
* `x_column`: CSV column where the X coordinate can be found. Defaults to "longitude".
* `y_column`: CSV column where the Y coordinate can be found. Defaults to "latitude".
* `srid`: SRID of the coordinates. Defaults to "4326".

The `run` method can be called with this parameters:
* `start_row`: First row to load from the CSV file. Defaults to "1", i.e., start from the beginning.
* `end_row`: Last row to load from the CSV file. Defaults to "None", i.e., keep going until the end of the file.

It is recommended for the column referred to in `id_column` to be indexed in CARTO.

### Delete existing items in CARTO

```python
from etl import *

job = DeleteJob("object_wid", "my_existing_samples_to_be_deleted.csv")
job.run()
```

`DeleteJob` can be created with these parameters:
* `id_column`: Name of the column that will be used to match the records in CARTO. Actually, only this column needs to be present in the file, although it does not hurt if there are others.
* `csv_file_path`: Path to the CSV file.

The `run` method can be called with this parameters:
* `start_chunk`: First chunk to load from the CSV file. Defaults to "1", i.e., start from the beginning.
* `end_chunk`: Last chunk to load from the CSV file. Defaults to "None", i.e., keep going until the end of the file.

It is recommended for the column referred to in `id_column` to be indexed in CARTO.

## Creating and regenerating overviews

There is a small utility to create or regenerate [overviews](https://carto.com/docs/tips-and-tricks/back-end-data-performance) for large point datasets. Once the ETL job is finished you can run the following methods:

* `job.regenerate_overviews()`: will start an asynchronous CARTO SQL execution, returning the identifier of the corresponding batch job.
* `job.check_job(batch_job_id)`: will return a dictionary with information about the batch SQL execution, including a `status` property.

An example of running this process and waiting until it is finished:

```python
import time

#.... job.run(), etc

batch_job_id = job.regenerate_overviews()

while job.check_job(batch_job_id)['status'] != 'done':
    time.sleep(5)

#...
```

Caveats:

* If you are going to run more than one ETL job, overviews should be regenerated only **after** all of them have finished.
* Mind that generating overviews can take a **long time**, that's the reason of using CARTO's [Batch SQL PI](https://carto.com/docs/carto-engine/sql-api/batch-queries/) so this process is run asynchronously.

## Geocoding

See ```test_geocoding.py``` for a usage example

To run tests do the following:

```
cp etl.conf.example etl.conf
# you should configure properly the etl.conf file, specially your HERE API keys
virtualenv env
source env/bin/activate
pip install -r requirements.txt
pip install pytest
pip install .
py.test tests
```