import logging
import sys
try:
    import ConfigParser
except Exception:
    import configparser as ConfigParser

sys.path.insert(0, '..')
from etl.etl import UpdateJob, InsertJob, DeleteJob


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

def flatten(config, kwargs):
    for key in config:
        if isinstance(config[key], dict):
            flatten(config[key], kwargs)
        else:
            kwargs[key] = config[key]
    return kwargs

kwargs = flatten(config._sections, {})
if action == 'insert':
    job = InsertJob("sample01.csv", **kwargs)
elif action == 'update':
    job = UpdateJob("a", "sample01.csv", **kwargs)
elif action == 'delete':
    job = DeleteJob("a", "sample01.csv", **kwargs)

if job is not None:
    job.run()
else:
    logger.error("Please provide an action: insert, update or delete")
