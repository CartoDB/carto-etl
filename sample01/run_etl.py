import logging
import sys
import ConfigParser

sys.path.insert(0, '..')
from etl import UpdateJob, InsertJob, DeleteJob


config = ConfigParser.RawConfigParser()
config.read("etl.conf")

LOG_FILE = config.get('log', 'file')
LOG_LEVEL = config.get('log', 'level')

logger = logging.getLogger('carto-etl')
logger.setLevel(logging.DEBUG)

# File handler
try:
    fh = logging.FileHandler(LOG_FILE)
except IOError:
    pass
else:
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

# Console handler for non-debug messages
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

if len(sys.argv) < 2:
    logger.error("Please provide an action: insert, update or delete")
    exit(1)
action = sys.argv[1]

if action == 'insert':
    job = InsertJob("sample01.csv", "lon", "lat", "4326")
elif action == 'update':
    job = UpdateJob("a", "sample01.csv", "lon", "lat", "4326")
elif action == 'delete':
    job = DeleteJob("a", "sample01.csv", "lon", "lat", "4326")

if job is not None:
    job.run()
else:
    logger.error("Please provide an action: insert, update or delete")
