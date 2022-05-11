import json, os, ssl, re, sys 
import sched, requests, threading, time
from collections import defaultdict
from flask import Flask, request
import config
from RWLock import RWLock_2_1

requests.packages.urllib3.disable_warnings()

BRIGHT_URL = config.APP_CONFIG["bright"]["url"]
CERT_DIR   = config.APP_CONFIG["bright"]["cert_dir"]
BRIGHT_CERT= ('{}/cert.pem'.format(CERT_DIR), '{}/cert.key'.format(CERT_DIR))
SEVEN_DAYS = 7 * 24 * 3600
QUERY_INTERVAL = 300   # query bright at least every 3 minutes
INC_INTERVAL   = 50    # query bright if last query returns 0
FLUSH_INTERVAL = 7200  # flush memory every 2 hours
FIVE_MINUTES   = 300
TS             = 0
VAL            = 1
START          = 0
STOP           = 1

app      = Flask(__name__)
logger   = config.logger
s_print_lock = threading.Lock()


def s_print(*a, **b):
    """Thread safe print function"""
    with s_print_lock:
        print(*a, **b)

class QueryBrightThread (threading.Thread):
    def __init__ (self):
        threading.Thread.__init__(self)
        self.last_ts    = int(time.time()) - SEVEN_DAYS
        self.last_cut   = self.last_ts
        self.gpu_data   = defaultdict(lambda:defaultdict(list))  # {'workergpu001':{'gpu0':[[ts, val], ...], ...}...}
        self.mem_data   = defaultdict(lambda:defaultdict(list))  # {'workergpu001':{'gpu0':[[ts, val], ...], ...}...}
                                                                 # above two has different ts
        self.data       = {"gpu_utilization": self.gpu_data, "gpu_fb_used": self.mem_data}
        self.rwlock     = RWLock_2_1()

    def run(self):
        while True:
            self.queryBright()
            if int(time.time()) - self.last_ts > INC_INTERVAL:   # normal situation this value is 0
               s_print("Interval {}: sleep inc".format(int(time.time()) - self.last_ts ) )
               time.sleep (INC_INTERVAL)
            else:
               s_print("Interval {}: sleep query".format(int(time.time()) - self.last_ts ) )
               time.sleep (QUERY_INTERVAL)
            if int(time.time()) - self.last_cut > SEVEN_DAYS + FLUSH_INTERVAL:
               self.flushGPULoads()
            
    # get rid of older data
    def flushGPULoads(self):
        self.rwlock.writer_acquire()
        logger.info("flushGPULoads")

        cut_ts = int(time.time()) - SEVEN_DAYS
        for measure, m_data in self.data.items():
          logger.info("measure={}".format(measure))
          for node, node_data in m_data.items():
            for gpu_id, data_lst in node_data.items():
                for idx in range(0, len(data_lst)):
                    if data_lst[idx][TS] > cut_ts:
                       break
                #0, idx-1 <= cut_ts
                count = idx-1       # remove 0 .. idx-2
                logger.info("remove {} from {}".format(count, len(data_lst)))
                while count > 0:
                   data_lst.pop(0)
                   count -= 1
                logger.info("done {}".format(len(data_lst)))
        self.last_cut = cut_ts

        self.rwlock.writer_release()
        return
        
    def queryBright(self):
        s_print("***query bright***")
        q_str = '{}/dump?measurable=gpu_utilization:gpu[0-9],gpu_fb_used:gpu[0-9]&start={}&epoch=1'.format(BRIGHT_URL, self.last_ts-1)
        s_print("query {}".format(q_str))
        try:
           r  = requests.get(q_str, verify=False, cert=BRIGHT_CERT)
           ts = int(time.time())
        except Exception as e:
           logger.error("Cannot connect to Bright. Exception {}".format(e))
           return []
    
        rlt      = r.json().get('data', [])
        logger.info("After interval {}, return {} rlts".format(ts-self.last_ts, len(rlt)))
        if len(rlt) == 0:              # No data
           return

        #preprare data
        gpu_data = defaultdict(lambda:defaultdict(list))  # {'workergpu001':{'gpu0':[[ts, val], ...], ...}...}
        mem_data = defaultdict(lambda:defaultdict(list))  # {'workergpu001':{'gpu0':[[ts, val], ...], ...}...}
        new_data = {"gpu_utilization": gpu_data, "gpu_fb_used": mem_data}
        for item in rlt:
            if item['raw'] == None: continue
            # data is not empty
            m, gpu_id   = item['measurable'].split(':')
            lst         = new_data[m][item['entity']][gpu_id]
            lst.append([int(item['time']/1000), item['raw']])
            
        if len(rlt):
           self.incGPULoad (ts, new_data)
        logger.info("***query bright done***")
 
    # incrementally add to the self.data
    def incGPULoad(self, ts, new_data):
        self.rwlock.writer_acquire()

        self.last_ts = ts
        for measure, m_data in new_data.items():
          logger.info("measure={}".format(measure))
          sav_data = self.data[measure]
          for node, gpuData in m_data.items():
            for gpu_id, new_lst in gpuData.items():
                lst        = sav_data[node][gpu_id]
                new_lst.sort()
                if len(lst) == 0:
                   lst.extend(new_lst)
                   continue

                # find insert point reversively
                insert_idx = len(lst)-1
                while (insert_idx>=0) and (new_lst[0][TS] <= lst[insert_idx][TS]): insert_idx -= 1
                #new_ts > lst[insert_idx][TS], new_ts <= lst[insert_idx+1][TS]
                insert_idx += 1                      # start inserting new_lst
                new_idx    = 0
                #merge sort lst[insert_idx:], new_lst[new_idx:]
                while insert_idx<len(lst) and new_idx < len(new_lst):
                      if lst[insert_idx][TS] == new_lst[new_idx][TS]:  # same ts, keep orginal value, ignore new one
                         if lst[insert_idx][VAL] != new_lst[new_idx][VAL]:
                            logger.error("Ignore different values in new data at same time {} {}".format(lst[insert_idx], new_lst[new_idx])) 
                         #else:
                         #   logger.info("Ignore same values in new data at same time {} {}".format(lst[insert_idx], new_lst[new_idx])) 
                         insert_idx += 1
                         new_idx    += 1
                      elif lst[insert_idx][TS] < new_lst[new_idx][TS]: # keep samller original one
                         insert_idx += 1
                      else:         #lst[insert_idx][TS] > new_lst[new_idx][TS], if same value, modifiy original ts to include new one, otherwise, insert new one
                         if lst[insert_idx][VAL] == new_lst[new_idx][VAL]:
                            logger.info("Extend same values in old data at differnt time {} {}".format(lst[insert_idx], new_lst[new_idx])) 
                            lst[insert_idx][TS] = new_lst[new_idx][TS]
                         else:
                            lst.insert(insert_idx, new_lst[new_idx])
                         new_idx    += 1
                         insert_idx += 1
                if new_idx < len(new_lst):
                   lst.extend(new_lst[new_idx:]) 
                         
        self.rwlock.writer_release()

    #not thread safe
    def getGPUNodes_unsafe (self, node_list=None, node_regex=None):
        if node_list:
           nodes = node_list
        else:
           nodes = self.gpu_data.keys()
           if node_regex:
              p     = re.compile(node_regex)
              nodes = [n for n in nodes if p.fullmatch(n)]

        logger.info("Nodes are {}".format(nodes))
        return nodes

    def getGPULoads (self, start, stop, step, node_regex=None):
        self.rwlock.reader_acquire()

        rlt   = defaultdict(dict)
        nodes = self.getGPUNodes_unsafe (node_regex=node_regex)
        for node in nodes:
            for gpu_id, data_lst in self.gpu_data[node].items():
                curr_lst = []
                curr_ts  = start
                idx      = 0
                while idx < len(data_lst):
                    while idx < len(data_lst) and data_lst[idx][TS] <= curr_ts:
                       idx += 1
                    #idx > curr_ts or idx==len(data_lst)
                    if idx > 0:
                       curr_lst.append ([curr_ts, data_lst[idx-1][VAL]])
                    # else: no data point for curr_ts
                    curr_ts += step
                    if (curr_ts > stop):
                       break
                rlt[node][gpu_id] = curr_lst       
        print('{}'.format(rlt))

        self.rwlock.reader_release()
        return rlt

    def getGPULoads (self, start, stop, step, node_regex=None):
        self.rwlock.reader_acquire()
        self.rwlock.reader_release()


    
    # return averge GPU utlizations during period defined by nodes_period {'node':[start,stop]}
    def getNodesGPUAvg (self, nodes_period): 
        rlt = defaultdict(dict)

        for node, [start,stop] in nodes_period.items():
            rlt[node] = self.getGPULoads_safe(start,stop,node_list=[node])[node]

        return rlt
   
    # return the averge GPU utilizatoin of nodes matching node_regex or in node_list from start to stop
    # node_list has a prioiry over node_regex
    # if both node_list and node_regex are None, return all GPU nodes
    # mem=True will get mem load
    def getGPULoads_safe(self, start, stop=None, node_regex=None, node_list=None, mem=False):
        self.rwlock.reader_acquire()

        gpu_data=self.gpu_data if not mem else self.mem_data

        if stop and (int(time.time())-stop < 10):       # if stop is < 10 seconds earlier, then just search all the way to now
           stop = None
        rlt   = defaultdict(dict)
        nodes = self.getGPUNodes_unsafe (node_regex=node_regex, node_list=node_list)
        #print('matched nodes={}'.format(nodes))
        for node in nodes:
            for gpu_id, data_lst in gpu_data[node].items():
                start_idx = 0
                # locate ts after start. 
                while start_idx < len(data_lst) and data_lst[start_idx][TS] <= start:
                    start_idx += 1
                if start_idx > 0:
                    start_idx -= 1
                # data_lst[start_idx] <= start && data_lst[start_idx+1][TS] > start

                # locate ts before stop
                if not stop:
                    stop_idx = len(data_lst)-1
                else:
                    stop_idx  = start_idx + 1
                    while stop_idx < len(data_lst) and data_lst[stop_idx][TS] <= stop:
                        stop_idx += 1
                    if stop_idx > 0:
                        stop_idx -= 1
                # data_lst[stop_idx] <= stop && data_lst[stop_idx+1][TS] > start
                
                curr_lst = data_lst[start_idx:stop_idx+1]
                if curr_lst[0][TS] < start:         # reset ts for first item to start
                   curr_lst[0][TS] = start
                if not stop:                       # add one item at last if necessary
                   curr_lst.append([int(time.time()), curr_lst[-1][VAL]])
                elif curr_lst[-1][TS] < stop:
                   curr_lst.append([stop, curr_lst[-1][VAL]])
                rlt[node][gpu_id] = curr_lst      
             
        self.rwlock.reader_release()

        return rlt

q_thrd = QueryBrightThread ()
q_thrd.start ()

#calculate the avg of the gpu_load for each node and gpu_id
def calculateAvg (gpu_load):  
    rlt = defaultdict(dict)
    for node, gpu_data in gpu_load.items():
        for gpu_id, data_lst in gpu_data.items():
            if not data_lst:
               rlt[node][gpu_id] = 0
            else:
               total = 0
               for idx in range(1, len(data_lst)):
                   total += data_lst[idx-1][VAL] * (data_lst[idx][TS]-data_lst[idx-1][TS])
               rlt[node][gpu_id] = total / (data_lst[-1][TS] - data_lst[0][TS])
               if (rlt[node][gpu_id] > 1) or (rlt[node][gpu_id] < 0) :
                  logger.warning("gpu_id={}\ndata_lst={}\ntotal={},period={}".format(gpu_id, data_lst, total, data_lst[-1][TS] - data_lst[0][TS]))

    return rlt

def getParam (request_args, def_start=3600):
    start, stop = int(request_args.get('start',0)), int(request_args.get('stop',0))
    if start == 0:
       stop_tmp = stop if stop else int(time.time())
       start    = stop_tmp - def_start                                # default start is one hour earlier
    nodes       = request.args.get('nodes',None)
    if nodes:
       nodes    = json.loads(nodes)

    return start,stop,nodes

@app.route('/', methods=['GET', 'POST'])
def index():
    return "Hello, world"

#return GPU utilization of nodes matching regex expression defined by nodesRegex during period [start, stop] at interval step
#default value of stop  is current time
#default value of start is one hour earlier than stop
#default value of nodes is all GPU nodes
@app.route('/getGPULoads', methods=['GET', 'POST'])
def getGPULoads():
    s_print("getGPULoads {}".format(request.args))
    start, stop, step = int(request.args.get('start',0)), int(request.args.get('stop',0)), int(request.args.get('step', FIVE_MINUTES))
    nodesRegex        = request.args.get('nodesRegex',None)
    if stop == 0:
       stop = int(time.time())
    if start == 0:
       start = stop - 3600                                # default start is one hour earlier
    rlt               = q_thrd.getGPULoads(start, stop, step, nodesRegex)

    return rlt

@app.route('/getLatestGPUAvg', methods=['GET', 'POST'])
def getLatestGPUAvg ():
    minutes   = int(request.args.get('minutes',5))           # default is 5 minutes
    start     = int(time.time()) - minutes * 60
    node_list = request.args.get('nodes',None)
    logger.info ("nodes={}, type={}".format(node_list, type(node_list)))
    if node_list:
       node_list = json.loads(node_list)

    gpu_load = q_thrd.getGPULoads_safe(start, stop=None, node_regex=request.args.get('nodesRegex',None), node_list=node_list)
    rlt      = calculateAvg (gpu_load)

    return rlt

@app.route('/getGPULoads_Raw', methods=['GET', 'POST'])
def getGPULoads_Raw ():
    start, stop = int(request.args.get('start',0)), int(request.args.get('stop',0))
    if start == 0:
       stop_tmp = stop if stop else int(time.time())
       start    = stop_tmp - 3600                                # default start is one hour earlier

    rlt         = q_thrd.getGPULoads_safe(start, stop, node_regex=request.args.get('nodes',None))

    return rlt

#return the gpu average during the period [start, stop] for a list of nodes 
#default value of stop  is current time
#default value of start is one hour earlier than stop
#default value of nodes is all GPU nodes
@app.route('/getNodesGPUAvg', methods=['GET', 'POST'])
def getNodesGPUAvg ():
    start, stop = int(request.args.get('start',0)), int(request.args.get('stop',0))
    if start == 0:
       stop_tmp = stop if stop else int(time.time())
       start    = stop_tmp - 3600                                # default start is one hour earlier
    nodes       = request.args.get('nodes',None)
    if nodes:
       nodes    = json.loads(nodes)

    gpu_load    = q_thrd.getGPULoads_safe(start,stop,node_list=nodes)
    rlt         = calculateAvg (gpu_load)
    return rlt

#return the gpu history during the period [start, stop] for a list of nodes 
@app.route('/getNodesGPULoad', methods=['GET', 'POST'])
def getNodesGPULoad ():
    start,stop,node_list = getParam(request.args, def_start=24*3600) # default start is one day earlier

    gpu_load    = q_thrd.getGPULoads_safe(start,stop,node_list=node_list)
    mem_load    = q_thrd.getGPULoads_safe(start,stop,node_list=node_list, mem=True)

    return {"gpu":gpu_load, "mem":mem_load}

#return the gpu history during the period defined by {node: [start, stop]} for a list of nodes 
#no default value for start
@app.route('/getNodesGPULoad_1', methods=['GET', 'POST'])
def getNodesGPULoad_1 ():
    nodes       = request.args.get('nodeDict',None)
    if nodes:
       nodes    = json.loads(nodes)

    gpu_load    = {}
    mem_load    = {}
    for node, period in nodes.items():
        start, stop = period
        gpu_load.update (q_thrd.getGPULoads_safe(start,stop,node_list=[node]))
        mem_load.update (q_thrd.getGPULoads_safe(start,stop,node_list=[node], mem=True))

    return {"gpu":gpu_load, "mem":mem_load}

#test
@app.route('/test', methods=['GET', 'POST'])
def test():
    node='workergpu001'
    try:
       last_ts  = q_thrd.last_ts
       gpu_data = q_thrd.gpu_data
    except Exception as e:
        s_print('{}'.format(e))
    return '{} {}: {}'.format(node, last_ts, gpu_data[node])

app.run(host='0.0.0.0', port=12345)
