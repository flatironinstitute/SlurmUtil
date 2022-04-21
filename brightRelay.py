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
FLUSH_INTERVAL = 3600  # flush memory every 1 hour
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
        self.rwlock       = RWLock_2_1()

    def run(self):
        while True:
            self.queryBright()
            if int(time.time()) - self.last_ts > INC_INTERVAL:   # normal situation this value is 0
               s_print("Interval {}: sleep inc".format(int(time.time()) - self.last_ts ) )
               time.sleep (INC_INTERVAL)
            else:
               s_print("Interval {}: sleep query".format(int(time.time()) - self.last_ts ) )
               time.sleep (QUERY_INTERVAL)
            if self.last_cut - int(time.time()) > SEVEN_DAYS + FLUSH_INTERVAL:
               self.flushGPULoads()
            
    # get rid of older data
    def flushGPULoads(self):
        self.rwlock.writer_acquire()

        cut_ts = int(time.time()) - SEVEN_DAYS
        for node, node_data in self.gpu_data.values():
            for gpu_id, data_lst in node_data.values():
                for idx in range(0, len(data_lst)):
                    if data_lst[idx][TS] > cut_ts:
                       break
                #0, idx-1 <= cut_ts
                while idx-2 > 0:
                   val.pop(0)
                   idx -= 1
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
    
        rlt     = r.json().get('data', [])
        s_print("After interval {}, return {} rlts".format(ts-self.last_ts, len(rlt)))
        if len(rlt):
           self.savGPULoads (ts, rlt)
 
    def savGPULoads(self, ts, q_rlt):
        self.rwlock.writer_acquire()

        self.last_ts = ts
        for item in q_rlt:
            m, gpu_id   = item['measurable'].split(':')  # remove gpu_utilization: gpu0, gpu1...
            if item['raw'] != None:
               if m == 'gpu_utilization':
                  self.gpu_data[item['entity']][gpu_id].append([int(item['time']/1000), item['raw']])
               else:       # gpu_fb_used
                  self.mem_data[item['entity']][gpu_id].append([int(item['time']/1000), item['raw']])
                  
        self.rwlock.writer_release()

    def getGPULoads (self, start, stop, step, regex_nodes=None):
        self.rwlock.reader_acquire()

        rlt = defaultdict(dict)
        nodes = self.gpu_data.keys()
        if regex_nodes:
            p     = re.compile(regex_nodes)
            nodes = [n for n in nodes if p.fullmatch(n)]
            s_print("nodes match {}:{}".format(regex_nodes, nodes))
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

    def getGPULoads_Raw (self, start, stop=None, regex_nodes=None):
        rlt = self.getGPULoads_safe(start, stop, regex_nodes=regex_nodes)
        return rlt

    # return averge GPU utlizations during [start, now]
    def getLatestGPUAvg (self, start, regex_nodes=None, node_list=None): 
        rlt = self.getGPULoads_safe(start, stop=None, regex_nodes=regex_nodes, node_list=node_list)
        for node, gpu_data in rlt.items():
            for gpu_id, data_lst in gpu_data.items():
                if not data_lst:
                   rlt[node][gpu_id] = 0
                else:
                   total = 0
                   for idx in range(1, len(data_lst)):
                       total += data_lst[idx-1][VAL] * (data_lst[idx][TS]-data_lst[idx-1][TS])
                   rlt[node][gpu_id] = total / (data_lst[-1][TS] - data_lst[0][TS])

        return rlt

    
    
    # return averge GPU utlizations during period defined by nodes_period {'node':[start,stop]}
    def getNodesGPUAvg (self, nodes_period): 
        rlt = defaultdict(dict)

        for node, [start,stop] in nodes_period.items():
            rlt[node] = self.getGPULoads_safe(start,stop,node_list=[node])[node]

        return rlt
   
    # return the averge GPU utilizatoin of nodes matching regex_nodes or in node_list from start to stop
    # node_list has a prioiry over regex_nodes
    # mem=True will get mem load
    def getGPULoads_safe(self, start, stop=None, regex_nodes=None, node_list=None, mem=False):
        self.rwlock.reader_acquire()

        gpu_data=self.gpu_data if not mem else self.mem_data

        rlt   = defaultdict(dict)
        if node_list:
           nodes = node_list
        else:
           nodes = gpu_data.keys()
           print('all nodes={}'.format(nodes))
           if regex_nodes:
              p     = re.compile(regex_nodes)
              nodes = [n for n in nodes if p.fullmatch(n)]
       
        print('matched nodes={}'.format(nodes))
        for node in nodes:
            for gpu_id, data_lst in gpu_data[node].items():
                start_idx = 0
                # locate ts after start. 
                while start_idx < len(data_lst) and data_lst[start_idx][TS] <= start:
                    start_idx += 1
                if start_idx > 0:
                    start_idx -= 1
                print('\t\tstart_idx={}'.format(start_idx))
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
                print('\t\tstop_idx={}'.format(stop_idx))
                # data_lst[stop_idx] <= stop && data_lst[stop_idx+1][TS] > start
                
                curr_lst = data_lst[start_idx:stop_idx+1]
                if curr_lst[0][TS] < start:         # reset ts for first item to start
                   curr_lst[0][TS] = start
                if not stop:                       # add one item at last if necessary
                   curr_lst.append([int(time.time()), curr_lst[-1][VAL]])
                elif curr_lst[-1][TS] < stop:
                   curr_lst.append([stop, curr_lst[-1][VAL]])
                rlt[node][gpu_id] = curr_lst      
                print('\t\tlst={}'.format(curr_lst))
             
        print('{}'.format(rlt))
        self.rwlock.reader_release()

        return rlt

q_thrd = QueryBrightThread ()
q_thrd.start ()

@app.route('/', methods=['GET', 'POST'])
def index():
    return "Hello, world"

@app.route('/getGPULoads', methods=['GET', 'POST'])
def getGPULoads():
    s_print("getGPULoads {}".format(request.args))
    start, stop, step = int(request.args.get('start',0)), int(request.args.get('stop',0)), int(request.args.get('step', FIVE_MINUTES))
    nodes             = request.args.get('nodes',None)
    if stop == 0:
       stop = int(time.time())
    if start == 0:
       start = stop - 3600                                # default start is one hour earlier
    s_print("getGPULoads {} {} {} {}".format(start, stop, step, nodes))
    rlt               = q_thrd.getGPULoads(start, stop, step, nodes)

    return rlt

@app.route('/getLatestGPUAvg', methods=['GET', 'POST'])
def getLatestGPUAvg ():
    nodes   = request.args.get('nodes',   None)
    node_lst= request.args.get('nodeList',None)
    minutes = int(request.args.get('minutes',5))           # default is 5 minutes
    start   = int(time.time()) - minutes * 60
    rlt     = q_thrd.getLatestGPUAvg(start, regex_node=nodes, node_list=node_lst)

    return rlt

@app.route('/getGPULoads_Raw', methods=['GET', 'POST'])
def getGPULoads_Raw ():
    nodes       = request.args.get('nodes',None)
    start, stop = int(request.args.get('start',0)), int(request.args.get('stop',0))
    if start == 0:
       stop_tmp = stop if stop else int(time.time())
       start    = stop_tmp - 3600                                # default start is one hour earlier

    rlt         = q_thrd.getGPULoads_Raw(start, stop, regex_nodes=nodes)

    return rlt

@app.route('/getNodesGPUAvg', methods=['GET', 'POST'])
def getNodesGPUAvg ():
    nodes       = request.args.get('nodes',None)
    nodes_period= json.loads(nodes)
    rlt = q_thrd.getNodesGPUAvg (nodes_period)
    return rlt

@app.route('/getNodesGPUMemLoads_Raw', methods=['GET', 'POST'])
def getNodesGPUMemLoads_Raw ():
    nodes        = request.args.get('nodes',None)
    nodes_period = json.loads(nodes)
    return nodes

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
