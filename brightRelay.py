import os, ssl, re, sys 
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
        self.lock       = RWLock_2_1()

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
               self.lock.writer_acquire()
               self.flushGPULoads ()
               self.lock.writer_release()
            
    # get rid of older data
    def flushGPULoads (self):
        cut_ts = int(time.time()) - SEVEN_DAYS
        s_print ("***flushGPULoads before {}".format(cut_ts))

        for node, node_data in self.gpu_data.values():
            for gpu_id, data_lst in node_data.values():
                for idx in range(0, len(data_lst)):
                    if data_lst[idx][0] > cut_ts:
                       break
                #0, idx-1 <= cut_ts
                while idx-2 > 0:
                   val.pop(0)
                   idx -= 1

        self.last_cut = cut_ts
        return
        
    def savGPULoads(self, ts, q_rlt):
        self.last_ts = ts
        for item in q_rlt:
            gpu_id   = item['measurable'].split(':')[1]  # remove gpu_utilization: gpu0, gpu1...
            if item['raw'] != None:
               self.gpu_data[item['entity']][gpu_id].append([int(item['time']/1000), item['raw']])

    def queryBright(self):
        s_print("***query bright***")
        q_str = '{}/dump?measurable=gpu_utilization:gpu[0-9]&start={}&epoch=1'.format(BRIGHT_URL, self.last_ts-1)
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
           self.lock.writer_acquire()
           self.savGPULoads (ts, rlt)
           self.lock.writer_release()
 
    def getGPULoads (self, start, stop, step, regex_nodes=None):

        self.lock.reader_acquire()

        s_print("getGPULoads {} {} {} {}".format(start, stop, step, regex_nodes))
        rlt = defaultdict(dict)
        nodes = self.gpu_data.keys()
        s_print("all nodes:{}".format(nodes))
        if regex_nodes:
            p     = re.compile(regex_nodes)
            nodes = [n for n in nodes if p.fullmatch(n)]
            s_print("nodes match {}:{}".format(regex_nodes, nodes))
        for node in nodes:
            s_print("--{}".format(node))
            for gpu_id, data_lst in self.gpu_data[node].items():
                curr_lst = []
                curr_ts  = start
                idx      = 0
                while idx < len(data_lst):
                    while idx < len(data_lst) and data_lst[idx][0] <= curr_ts:
                       idx += 1
                    #idx > curr_ts or idx==len(data_lst)
                    if idx > 0:
                       curr_lst.append ([curr_ts, data_lst[idx-1][1]])
                    # else: no data point for curr_ts
                    curr_ts += step
                    if (curr_ts > stop):
                       break
                rlt[node][gpu_id] = curr_lst       
                       
        print('{}'.format(rlt))
        self.lock.reader_release()
        return rlt

q_thrd = QueryBrightThread ()
q_thrd.start ()

@app.route('/', methods=['GET', 'POST'])
def index():
    return "Hello, world"

@app.route('/getGPULoads', methods=['GET', 'POST'])
def getGPULoads():
    s_print("getGPULoads {}".format(request.args))
    start, stop, step = int(request.args.get('start',0)), int(request.args.get('stop',0)), int(request.args.get('step',0))
    nodes             = request.args.get('nodes',None)
    if stop == 0:
       stop = int(time.time())
    if start == 0:
       start = stop - 3600
    if step == 0:
       step = FIVE_MINUTES
    s_print("getGPULoads {} {} {} {}".format(start, stop, step, nodes))
    rlt               = q_thrd.getGPULoads(start, stop, step, nodes)

    return '{} {} {}: {}'.format(start, stop, step, rlt)

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
