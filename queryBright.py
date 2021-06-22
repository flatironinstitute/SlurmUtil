#!/usr/bin/env python00

import time
t1=time.time()
import os, requests, sys
import urllib3
from collections import defaultdict
from statistics  import mean   #fmean faster than mean, but not until 3.8

import MyTool, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger  = config.logger
url     = config.APP_CONFIG["bright"]["url"]
cert_dir= config.APP_CONFIG["bright"]["cert_dir"]

class BrightRestClient:
    bright_base_url  = url
    bright_cert      = ('./prometheus.cm/cert.pem', './prometheus.cm/cert.key')
    def __init__(self):
        #? use session
        self.base_url  = url
        self.cert      = ('./prometheus.cm/cert.pem', './prometheus.cm/cert.key')
        self.gpu_ts    = 0
        self.mem_ts    = 0
        self.gpu_data  = None
        self.mem_data  = None
  
    #latest compared with dump, dump, then average, is easier to get consistent result
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:gpu0', verify=False, cert=('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key'))
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?entity={}&measurable=gpu_utilization:{}'.format(node,gpuId), verify=False, cert=self.cert)
    #r       = requests.get('https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable=gpu_utilization:{}&start=-{}h'.format(node,gpuId,hours), verify=False, cert=cert_files)
    #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'},

    # query bright all gpu data on node_list
    def _getAllGPU_raw (self, node_list, minutes=5, max_gpu_cnt=4, intervalFlag=False):
        start    = time.time()
        entities = ','.join(node_list)
        measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        ts       = int(time.time())
        if intervalFlag:
           intervals= minutes * 6            # bright returns one sample per 10 seconds at most, use intervals will leave None at the end, this is for comparison and test purpose 
           q_str    = '{}/dump?entity={}&measurable={}&start=-{}m&intervals={}&epoch=1'.format(self.base_url,entities,measures,minutes,intervals)
        else:
           # intervals=0 (default, = raw data), that is  
           q_str    = '{}/dump?entity={}&measurable={}&start=-{}m&epoch=1'.format(self.base_url,entities,measures,minutes)
                              #epoch: time stamp as unix epoch
        logger.info("query_str={}".format(q_str))
        r     = requests.get(q_str, verify=False, cert=self.cert)
        # divide raw data by node and gpu
        d     = defaultdict(lambda:defaultdict(list)) 
        for item in r.json().get('data',[]):
            #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'},
            gpu_id   = item['measurable'].split(':')[1]  # remove gpu_utilization: gpu0, gpu1...
            d[gpu_id][item['entity']].append(item)
        logger.debug("query take time {}".format(time.time()-start))
        return ts, dict(d)

    def _calculateRawAvg (seq, startTS, stopTS):
        if not seq:      
           return 0
        if len(seq) < 2: 
           return seq[0]['raw']   # only one data
        total   = 0
        maxP    = 0
        minP    = 3600
        lastIdx = 0
        startIdx= 0
        while (startIdx < len(seq)) and (seq[startIdx]['time']/1000 < startTS+1):  
            startIdx+= 1    
        if startIdx>0:    # seq[startIdx]['time'] > startTS
            preTS    = startTS
        else:             # startIdx = 0 and seq[0]['time'] > startTS, should not happen
            total   += seq[0]['raw'] * (seq[0]['time']/1000 - startTS)
            preTS    = seq[0]['time']/1000
            startIdx = 1
        for idx in range(startIdx,len(seq)):
            if seq[idx]['raw'] != None:
               period = seq[idx]['time']/1000 - preTS
               if period < minP:  minP=period
               if period > maxP:  maxP=period; maxIdx = idx
               total += seq[idx]['raw'] * period
               preTS  = seq[idx]['time']/1000
               lastIdx= idx
        if seq[lastIdx]['time']/1000 < stopTS :     # last time period
            total += seq[lastIdx]['raw'] * (stopTS - seq[lastIdx]['time']/1000)
            #print("calculateRawAvg late last={}".format(stopTS - seq[lastIdx]['time']/1000))
              
        #print("calculateRawAvg min={},max={}".format(minP, maxP)) # preMax and max should have the same raw
        return total/(stopTS-startTS)
            
    # get all gpu data on node_list, return avg util of last {minutes} minutes
    # reture ['query_time': , {'gpu0':{'workergpu00':0.34 ... },} ]
    # called by heatmap
    def getAllGPUAvg (self, node_list, minutes=5, max_gpu_cnt=4, intervalFlag=False):
        if (int(time.time())- self.gpu_ts) < 60:
            logger.info ("less than 60 seconds from last query, return saved gpu data")
            return self.gpu_ts, self.gpu_data

        ts,d  = self._getAllGPU_raw (node_list, minutes, max_gpu_cnt, intervalFlag)
        rlt   = defaultdict(dict)
        for gpu, gpu_nodes in d.items():
            for node, seq in gpu_nodes.items():
                # calculate average, notice that the data is not even intervaled
                rlt[gpu][node]  = BrightRestClient._calculateRawAvg(seq, ts-minutes*60, ts)
        rlt   = dict(rlt)
        self.gpu_ts, self.gpu_data = ts,rlt
        return ts, rlt

    #node_dict {'workergpu00':{'gpu0':job}...}
    #called by index
    def getAllGPUAvg_jobs (self, node_dict, minutes=5, max_gpu_cnt=4):
        ts,d  = self._getAllGPU_raw (list(node_dict.keys()), minutes, max_gpu_cnt)

        rlt   = defaultdict(lambda:defaultdict(int))
        for gpu, gpu_nodes in d.items():
            for node, seq in gpu_nodes.items():
                # calculate average
                if gpu in node_dict[node]:
                   job = node_dict[node][gpu]    
                   rlt[node][job['job_id']]  += BrightRestClient._calculateRawAvg(seq, job['start_time'], ts)
        return ts, dict(rlt)

    #{'node': node, 'time': int(time.time()), 'data': [{'gpu0':[], ...}}]}
    #used in queryGPU.py
    def getNodeGPU (self, node, start_ts, gpu_list=[0,1,2,3], msec=True):
        return self.getGPU([node], start_ts, gpu_list, msec=msec)

    #called by queryGPU.py
    def getGPU (self, node_list, start_ts, gpu_list=[], max_gpu_id=3, msec=True):
        nodes     = ','.join(node_list)
        if not gpu_list:
           gpu_list = list(range(0, max_gpu_id+1))
        gpus_util = ','.join(['gpu_utilization:gpu{}'.format(i) for i in gpu_list])
        req_str   = 'https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable={}&start={}&epoch=1'.format(nodes,gpus_util,start_ts)
        r         = requests.get(req_str, verify=False, cert=self.cert)
        d         = r.json()['data']   #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'}, 
        rlt       = defaultdict(list)                 #{'workergpu16.gpu0':[[ts,val],]
        for item in d:
            if msec:
               rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([item['time'], item['raw']])
            else:
               rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([int(item['time']/1000), item['raw']])

        return dict(rlt)

    # get last hours data for node_list
    def getNodesGPU_Mem (self, node_list, start, gpu_list=[], max_gpu_id=3, msec=True):
        if not gpu_list:
           gpu_list = list(range(max_gpu_id+1))
        mea_list  = ['gpu_utilization', 'gpu_fb_used']
        req_str   = self.getNodeGPURequest (node_list, gpu_list, mea_list, start)
        r         = requests.get(req_str, verify=False, cert=self.cert)
        d         = r.json()['data']   #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'}, 
        rlt       = {}
        for m in mea_list:
            rlt[m] = defaultdict(list)                #{'workergpu16.gpu0':[[ts,val],]
        for item in d:
            m,gid  =item['measurable'].split(':')
            ts     =item['time'] if msec else int(item['time']/1000)
            rlt[m]['{}.{}'.format(item['entity'],gid)].append([ts, item['raw']])
        # check the start, stop time
        for m in mea_list:
            for seq in rlt[m].values():
                start_ts = start * 1000 if msec else start_ts
                if seq[0][0]<start_ts:                # smaller means the same value last
                   seq[0][0] = start_ts

        return dict(rlt)

    def getNodeGPURequest (self, node_list, gpu_list, measure_list, start):
        entities = ','.join(node_list)
        lst      = [','.join(['{}:gpu{}'.format(m,i) for i in gpu_list]) for m in measure_list]
        measures = ','.join(lst)
        q_str    = '{}/dump?entity={}&measurable={}&start={}&epoch=1'.format(self.base_url,entities,measures,start)
        logger.debug ("query={}".format(q_str))
        return q_str

def test1():
    client = BrightRestClient()

def test2(node, hours=1):
    client = BrightRestClient()
    rlt    = client.getDumpNodeGPU(node, hours=hours)
    cnt    = sum([len(item['data']) for item in rlt['data']])
    print('{}: {} samples'.format(node, cnt))
    print('{}'.format(rlt))
    return rlt

def test3():
    client = BrightRestClient()
    cnt    = 0
    for i in range(0, 43):
        node = 'workergpu{:0>2d}'.format(i)
        rlt  = test2(node)
        cnt += sum([len(item['data']) for item in rlt['data']])

    print('Total: {} samples'.format(cnt))
        
def test5(minutes, flag):
    client = BrightRestClient()
    node_list = ['workergpu{}'.format(idx) for idx in range(17,18)]
    rlt       = client.getAllGPUAvg(node_list, minutes, intervalFlag=flag) 
    print(rlt)
    
def test6():
    client = BrightRestClient()
    node_list = ['workergpu{}'.format(idx) for idx in range(17,18)]
    d = client._getAllGPU_raw (node_list)
    print(d)

def test7():
    client = BrightRestClient()
    node_list = ['workergpu46']
    d      = client.getNodeGPU_Mem (node_list, 1)
    print(d)

def main():
    t1=time.time()
    test7 ()
    #if len(sys.argv) < 3:
    #   test5(int(sys.argv[1]), False)
    #else:
    #   test5(int(sys.argv[1]), True)
    #test2(sys.argv[1])
    #test3()
    #test4(sys.argv[1])
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
