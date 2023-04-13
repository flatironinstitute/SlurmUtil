import json,logging,os,sys
from logging.handlers import HTTPHandler
import MyTool

if sys.argv[0]:
   APP_DIR           = os.path.dirname(os.path.realpath(sys.argv[0]))
   APP_NAME          = os.path.basename(sys.argv[0]).split('.')[0]
else:  # from python
   APP_DIR           = os.path.realpath(sys.argv[0])
   APP_NAME          = 'python'

HTML_DIR          = os.path.join(APP_DIR, 'html')
APP_CONFIG        = {}
CSV_DIR           = "./data/"
SUMMARY_TABLE_COL = ['node', 'status', 'delay', 'node_mem_M', 'job', 'user', 'alloc_cpus', 'run_time', 'proc_count', 'cpu_util', 'avg_cpu_util', 'rss', 'vms', 'io', 'fds', 'alloc_gpus', 'gpu_util', 'avg_gpu_util']


def fillDefSettings (conf):
    defSettings = getUserSettings("default")
    if "settings" not in conf:
       conf["settings"] = {}
    settings = conf["settings"]
       
    if "low_util_job"  not in settings:  #percentage
       settings["low_util_job"]      = defSettings["low_util_job"]
    if "low_util_node" not in settings:
       settings["low_util_node"]     = defSettings["low_util_node"]
    if "summary_column" not in settings:
       settings["summary_column"]    = defSettings["summary_column"]
    if "summary_low_util" not in settings:
       settings["summary_low_util"]  = defSettings["summary_low_util"]
    if "summary_high_util" not in settings:
       settings["summary_high_util"] = defSettings["summary_high_util"]
    if "heatmap_avg" not in settings:
       settings["heatmap_avg"]       = defSettings["heatmap_avg"]
    if "heatmap_weight" not in settings:
       settings["heatmap_weight"]    = defSettings["heatmap_weight"]
    if "part_avail" not in settings:
       settings["part_avail"]        = defSettings["part_avail"]

def getUserSettings (user):
    filename = os.path.join(APP_DIR, 'config/{}_settings.json'.format(user))
    if os.path.isfile(filename):
       with open(filename) as setting_file:
            return json.load(setting_file)
    else:
       return APP_CONFIG["display_default"]

def readConfigFile (configFile):
   configFile = os.path.join(APP_DIR, configFile)
   if os.path.isfile(configFile):
      with open(configFile) as config_file:
          cfg= json.load(config_file)
      for key, val in cfg.items():
          APP_CONFIG[key] = val    #overwrite the old value if there is any
      #fillDefSettings (APP_CONFIG)
      if APP_NAME in APP_CONFIG['log']:
         logLevel = eval(APP_CONFIG['log'][APP_NAME])
      else:
         logLevel = eval(APP_CONFIG['log'].get('level', 'logging.DEBUG'))
      logger.setLevel (logLevel)
   else:
      logger.error("Configuration file {} does not exist!".format(configFile))
   logger.info("APP_CONFIG={}".format(APP_CONFIG))

def addHttpLog (url):
    http_handler =  HTTPHandler(url, "/data/log", method='POST')
    http_handler.setLevel(logging.ERROR)
    logger.addHandler (http_handler)
    #h=logging.handlers.HTTPHandler("scclin011:8126", "/log", method='GET', secure=False, credentials=None, context=None)

import os
logger        = MyTool.getFileLogger(APP_NAME, logging.DEBUG) 
if not os.path.isfile('config/config.json'):
   print("no config file")
readConfigFile('config/config.json')        #default config file
addHttpLog    ('localhost:{}'.format(APP_CONFIG["port"]))

def savUserSettings (user, settings):
    filename = os.path.join(APP_DIR, 'config/{}_settings.json'.format(user))
    with open(filename, "w") as setting_file:
        json.dump(settings, setting_file)
def savConfig(cfg):
    filename = os.path.join(APP_DIR, 'config/config.json')
    with open(filename, "w") as setting_file:
        json.dumps(APP_CONFIG)

