import json,logging,os,sys
from logging.handlers import HTTPHandler
import cherrypy
import MyTool

APP_DIR           = os.path.dirname(os.path.realpath(sys.argv[0]))
APP_NAME          = os.path.basename(sys.argv[0]).split('.')[0]
APP_CONFIG        = {}
SUMMARY_TABLE_COL = ['node', 'status', 'delay', 'node_mem_M', 'job', 'user', 'alloc_cpus', 'run_time', 'proc_count', 'cpu_util', 'avg_cpu_util', 'rss', 'vms', 'io', 'fds', 'alloc_gpus', 'gpu_util', 'avg_gpu_util']

logger            = MyTool.getFileLogger(APP_NAME, logging.INFO) 

def fillDefSettings (conf):
    if "settings" not in conf:
       conf["settings"] = {}
    settings = conf["settings"]
       
    if "low_util_job"  not in settings:  #percentage
       settings["low_util_job"]      = {"cpu":1, "gpu":10, "mem":30, "run_time_hour":24, "alloc_cpus":2, "email":False}
    if "low_util_node" not in settings:
       settings["low_util_node"]     = {"cpu":1, "gpu":10, "mem":30, "alloc_time_min":60}
    if "summary_column" not in settings:
       settings["summary_column"]    = dict(zip(SUMMARY_TABLE_COL, [True]*len(SUMMARY_TABLE_COL)))
       settings["summary_column"]['avg_gpu_util'] = False  #disable avg_gpu_util
    if "summary_low_util" not in settings:
       settings["summary_low_util"]  = {"cpu_util":1, "avg_cpu_util":1, "rss":1, "gpu_util":10, "avg_gpu_util":10, "type":"inform"}
    if "summary_high_util" not in settings:
       settings["summary_high_util"] = {"cpu_util":90,"avg_cpu_util":90, "rss":90, "gpu_util":90, "avg_gpu_util":90, "type":"alarm"}
    if "heatmap_avg" not in settings:
       settings["heatmap_avg"]       = {"cpu":0,"gpu":5}
    if "heatmap_weight" not in settings:
       settings["heatmap_weight"]    = {"cpu":50,"mem":50}
    if "part_avail" not in settings:
       settings["part_avail"]        = {"node":10,"cpu":10,"gpu":10}
    return settings

def readConfigFile (configFile):
   configFile = os.path.join(APP_DIR, configFile)
   if os.path.isfile(configFile):
      with open(configFile) as config_file:
          cfg= json.load(config_file)
      for key, val in cfg.items():
          APP_CONFIG[key] = val    #overwrite the old value if there is any
      fillDefSettings (APP_CONFIG)
      logLevel = eval(APP_CONFIG['log'].get('level', 'logging.DEBUG'))
      logger.setLevel (logLevel)
   else:
      logger.error("Configuration file {} does not exist!".format(configFile))
   logger.info("APP_CONFIG={}".format(APP_CONFIG))

def addHttpLog (url):
    http_handler =  HTTPHandler(url, "/log")
    http_handler.setLevel(logging.ERROR)
    logger.addHandler (http_handler)
    #h=logging.handlers.HTTPHandler("scclin011:8128", "/log", method='GET', secure=False, credentials=None, context=None)

readConfigFile('config/config.json')        #default config file
addHttpLog    ("localhost:8128")

def getUserSettings (user):
    filename = os.path.join(APP_DIR, 'config/{}_settings.json'.format(user))
    if os.path.isfile(filename):
       with open(filename) as setting_file:
            return json.load(setting_file)
    else:
       return APP_CONFIG["settings"]
def getSetting(attr):
    if 'user' in cherrypy.session:
       return cherrypy.session["settings"][attr];
    else:
       return APP_CONFIG["settings"][attr]
def getSettings():
    if 'user' in cherrypy.session:
       return cherrypy.session["settings"];
    else:
       return APP_CONFIG["settings"]
def savUserSettings (user, settings):
    filename = os.path.join(APP_DIR, 'config/{}_settings.json'.format(user))
    with open(filename, "w") as setting_file:
        json.dumps(setting_file)
def savConfig(cfg):
    filename = os.path.join(APP_DIR, 'config/config.json')
    with open(filename, "w") as setting_file:
        json.dumps(APP_CONFIG)
def setSetting (key,settings):
    if 'user' in cherrypy.session:
       cherry.session['settings'][key] = settings
       savUserSettings (cherry.session['user'],cherry.session['settings'])
    else:
       APP_CONFIG["settings"][key]= settings


