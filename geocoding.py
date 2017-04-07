import csv
import logging
import ConfigParser
import requests
from os.path import dirname, join
from datetime import datetime
from lxml import etree
from zipfile import ZipFile
from io import BytesIO

from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient


logger = logging.getLogger('carto-geocoding')

config = ConfigParser.RawConfigParser()
config.read("etl.conf")

HERE_APP_CODE = config.get('here', 'app_code')
HERE_APP_ID = config.get('here', 'app_id')
CARTO_BASE_URL = config.get('carto', 'base_url')
CARTO_API_KEY = config.get('carto', 'api_key')
MAX_ATTEMPTS = int(config.get('etl', 'max_attempts'))


sql = SQLClient(APIKeyAuthClient(CARTO_BASE_URL, CARTO_API_KEY))


class HereGeocodingJob(object):
    request_id = None
    status = None

    def __init__(self, csv_file_path=None, email=None, request_id=None):
        if request_id is not None:
            self.request_id = request_id
            self.target_dir = "."
        else:
            self.target_dir = dirname(csv_file_path)

            params = {
                "action": "run",
                "gen": 9,
                "header": True,
                "indelim": ";",
                "outdelim": ";",
                "mailto": email,
                "outcols": "displayLatitude,displayLongitude,locationLabel,houseNumber,street,district,city,postalCode,county,state,country",
                "outputCombined": False,
                "maxresults": 1,
                "app_code": HERE_APP_CODE,
                "app_id": HERE_APP_ID
            }

            with open(csv_file_path) as csv_file:
                r = requests.post("https://batch.geocoder.cit.api.here.com/6.2/jobs", data=csv_file, params=params)

            tree = etree.fromstring(r.text.encode("utf-8"))
            try:
                self.request_id = tree.xpath("//RequestId")[0].text
            except IndexError:
                logger.error("Error creating job: {error_detail}".format(error_detail=tree.xpath("Details")[0].text))
            else:
                self.status = tree.xpath("//Status")[0].text

    def refresh(self):
        params = {
            "action": "status",
            "app_code": HERE_APP_CODE,
            "app_id": HERE_APP_ID
        }

        r = requests.get("https://batch.geocoder.cit.api.here.com/6.2/jobs/{request_id}".format(request_id=self.request_id), params=params)

        tree = etree.fromstring(r.text.encode("utf-8"))
        self.status = tree.xpath("//Status")[0].text

    def download(self):
        params = {
            "app_code": HERE_APP_CODE,
            "app_id": HERE_APP_ID
        }

        r = requests.get("https://batch.geocoder.cit.api.here.com/6.2/jobs/{request_id}/all".format(request_id=self.request_id), params=params)

        if r.status_code == requests.codes.not_found:
            return

        # Clean sequence columns from HERE's response
        with BytesIO(r.content) as downloaded_file:
            with ZipFile(downloaded_file, "r") as original_zipfile:
                zipinfos = original_zipfile.infolist()

                with ZipFile("{file_path_without_extension}sss.zip".format(file_path_without_extension=join(self.target_dir, self.request_id)), "w") as clean_zipfile:
                    for zipinfo in zipinfos:
                        if zipinfo.filename.endswith("_out.txt") or zipinfo.filename.endswith("_err.txt"):
                            csv_reader = csv.DictReader(original_zipfile.open(zipinfo.filename))
                            with BytesIO() as clean_csv:
                                csv_writer = csv.writer(clean_csv)
                                csv_writer.writerow(["recId", "displayLatitude", "displayLongitude"])
                                for row in csv_reader:
                                    csv_writer.writerow([row["recId"], row["displayLatitude"], row["displayLongitude"]])
                                clean_zipfile.writestr(zipinfo.filename, clean_csv.getvalue())
                        else:
                            clean_zipfile.writestr(zipinfo.filename, original_zipfile.read(zipinfo.filename))


class CartoGeocodingJob(object):
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    def download(self):
        target_dir = dirname(self.csv_file_path)

        with open(self.csv_file_path) as input_file:
            csv_reader = csv.DictReader(input_file)
            found_addresses = ["recId,displayLatitude,displayLongitude"]
            not_found_addresses = ["recId,searchText,country"]
            invalid_addresses = ["recId,searchText,country"]
            for row_num, record in enumerate(csv_reader):
                query = "with geocoding as (select cdb_geocode_street_point('{address}', country => '{country}') as the_geom) select st_x(the_geom) as longitude, " \
                        "st_y(the_geom) as latitude from geocoding".format(address=record["searchText"], country=record["country"])
                for retry in xrange(MAX_ATTEMPTS):
                    try:
                        q = sql.send(query)
                    except Exception as e:
                        logger.error("Row #{row_num}: Retry ({error_msg})".format(row_num=(row_num + 1), error_msg=e))
                    else:
                        logger.info("Row #{row_num}: Success!".format(row_num=(row_num + 1)))
                        break
                else:
                    logger.error("Row #{row_num}: Failed ({error_msg})".format(row_num=(row_num + 1), error_msg=e))
                try:
                    result = q["rows"][0]
                    if result["latitude"] is None or result["longitude"] is None:
                        not_found_addresses.append("{id},{address},{country}".format(id=record["recId"], address=record["searchText"], country=record["country"]))
                    else:
                        found_addresses.append("{id},{latitude},{longitude}".format(id=record["recId"], latitude=q["rows"][0]["latitude"],
                                                                                    longitude=q["rows"][0]["longitude"]))
                except (AttributeError, IndexError, UnboundLocalError):
                    invalid_addresses.append("{id},{address},{country}".format(id=record["recId"], address=record["searchText"], country=record["country"]))

            now = datetime.now()
            with ZipFile(join(target_dir, now.strftime("result_%Y%m%d-%H-%M.zip")), "w") as z:
                z.writestr(now.strftime("result_%Y%m%d-%H-%M__out.txt"), "\n".join(found_addresses))
                z.writestr(now.strftime("result_%Y%m%d-%H-%M__err.txt"), "\n".join(not_found_addresses))
                z.writestr(now.strftime("result_%Y%m%d-%H-%M__inv.txt"), "\n".join(invalid_addresses))
