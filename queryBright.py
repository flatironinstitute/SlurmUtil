#!/usr/bin/env python00

import time
t1=time.time()
import os, requests, sys
import MyTool, config

from collections import defaultdict
from statistics  import mean   #fmean faster than mean, but not until 3.8

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger  = config.logger
class BrightRestClient:
    def __init__(self):
        #? use session
        self.base_url  = "https://ironbcm:8081/rest/v1/monitoring/"
        self.cert      = ('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key')
  
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:gpu0', verify=False, cert=('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key'))
    def getLatestGPU (self, gpuId='gpu0'): #TODO: use dump instead of latest to get latest value
        r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:{}'.format(gpuId), verify=False, cert=self.cert)
        j = r.json() #j['data'][0]={'age': 144.381, 'entity': 'workergpu00', 'measurable': 'gpu_utilization', 'raw': 0.0, 'time': 1580872073796, 'value': '0.0%'}
        if j['data']:
           d   = dict([(item['entity'], item['raw']) for item in j['data']])
           return d
        else:
           return None

    def getNodeLatestGPU (self, node, gpuId='gpu0'):
        r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?entity={}&measurable=gpu_utilization:{}'.format(node,gpuId), verify=False, cert=self.cert)
        j = r.json() #{'data': [{'age': 46.366, 'entity': 'workergpu00', 'measurable': 'gpu_utilization', 'raw': 1.0, 'time': 1582647355478, 'value': '100.0%'}]}
        if j['data']:
           return j['data'][0]
        else:
           return None

    def getDumpGPU (self, node, gpuId='gpu0', hours=72):
        #default is about 1 value per 5 minute and one hour of history
        r       = requests.get('https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable=gpu_utilization:{}&start=-{}h'.format(node,gpuId,hours), verify=False, cert=self.cert)
        startTS = int(time.time()) - hours*60*60
        j       = r.json() #{'entity': 'workergpu00', 'measurable': 'gpu_utilization:gpu1', 'raw': 1.0, 'time': '2020/02/10 16:51:21', 'value': '100.0%'}
        if j['data']:
           d   = [[MyTool.str2ts(item['time']), item['raw']] for item in j['data']]
           if d[0][0] < startTS - 60*60:  # if first value is 1 hour earlier than requested
              del d[0]
           return d
        else:
           return None

    # query bright all gpu data on node_list
    def getAllGPU_raw (self, node_list, minutes=5, max_gpu_cnt=4, intervalFlag=False):
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
        r     = requests.get(q_str, verify=False, cert=self.cert)
        #print("getAllGPU_raw return {}".format(r.json()))
        # divide raw data by node and gpu
        d     = defaultdict(lambda:defaultdict(list)) 
        for item in r.json()['data']:
            gpu_id   = item['measurable'].split(':')[1]  # remove gpu_utilization: gpu0, gpu1...
            d[gpu_id][item['entity']].append(item)
        #print("getAllGPU_raw len={}, took {}".format(len(r.json()['data']), time.time() - start))
        return ts, d

    def getRawAvg (seq, startTS, stopTS):
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
            #print("getRawAvg early first={}".format(seq[0]['time']/1000 - startTS))
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
            #print("getRawAvg late last={}".format(stopTS - seq[lastIdx]['time']/1000))
              
        #print("getRawAvg min={},max={}".format(minP, maxP)) # preMax and max should have the same raw
        return total/(stopTS-startTS)
            
    # get all gpu data on node_list, return avg util of last {minutes} minutes
    # reture ['query_time': , {'gpu0':{'workergpu00':0.34 ... },} ]
    def getAllGPUAvg (self, node_list, minutes=5, max_gpu_cnt=4, intervalFlag=False):
        ts,d  = self.getAllGPU_raw (node_list, minutes, max_gpu_cnt, intervalFlag)

        rlt   = defaultdict(dict)
        for gpu, gpu_nodes in d.items():
            logger.info("gpu_nodes.key={}".format(gpu_nodes.keys()))
            for node, seq in gpu_nodes.items():
                # calculate average
                rlt[gpu][node]  = BrightRestClient.getRawAvg(seq, ts-minutes*60, ts)
                #rlt[gpu][node] = mean([item['raw'] for item in seq if item['raw']!=None])  # some lastest ts will have raw None if use interval
        return ts, dict(rlt)

    #node_dict {'workergpu00':{'gpu0':job}...}
    def getAllGPUAvg_jobs (self, node_dict, minutes=5, max_gpu_cnt=4):
        ts,d  = self.getAllGPU_raw (list(node_dict.keys()), minutes, max_gpu_cnt)

        rlt   = defaultdict(lambda:defaultdict(int))
        for gpu, gpu_nodes in d.items():
            for node, seq in gpu_nodes.items():
                # calculate average
                if gpu in node_dict[node]:
                   job = node_dict[node][gpu]    
                   rlt[node][job['job_id']]  += BrightRestClient.getRawAvg(seq, job['start_time'], ts)
        return ts, dict(rlt)

    def getDumpRequest (self, node_list, max_gpu_cnt=4, minutes=None):
        entities = ','.join(node_list)
        measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        q_str    = '{}/dump?entity={}&measurable={}&epoch=1'.format(self.base_url,entities,measures)
        if minutes:
           q_str = '{}&start=-{}m'.format(q_str, minutes)
        return q_str

    #{'node': node, 'time': int(time.time()), 'data': [{'gpu0':[], ...}}]}
    def getNodeGPU (self, node, start_ts, gpu_list=[0,1,2,3]):
        return self.getGPU([node], start_ts, gpu_list)

    #reture {'time': , 'gpu0':{'workergpu00':0.34 ... }, }
    def getLatestAllGPU (self):
        idx = 0
        rlt = {'time': int(time.time())}
        while True:
           gpuId = 'gpu{}'.format(idx)
           d = self.getLatestGPU(gpuId)
           if d:
              rlt[gpuId]=d
              idx += 1
           else:
              return rlt
           
    def getGPU (self, node_list, start_ts, gpu_list=[], max_gpu_id=3):
        nodes     = ','.join(node_list)
        if not gpu_list:
           gpu_list = list(range(0, max_gpu_id+1))
        gpus_util = ','.join(['gpu_utilization:gpu{}'.format(i) for i in gpu_list])
        req_str   = 'https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable={}&start={}&epoch=1'.format(nodes,gpus_util,start_ts)
        r         = requests.get(req_str, verify=False, cert=self.cert)
        d         = r.json()['data']   #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'}, 
        rlt       = defaultdict(list)                 #{'workergpu16.gpu0':[[ts,val],]
        for item in d:
            rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([item['time'], item['raw']])
        return dict(rlt)

def test1():
    client = BrightRestClient()
    rlt    = client.getLatestAllGPU()
    print(rlt)

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
        
def test4(node):
    client = BrightRestClient()
    sav    = {}
    for i in range(20):
       rlt = client.getNodeLatestGPU(node,'gpu0')
       sav[round(rlt['time']/1000)]= rlt['raw']
       time.sleep(5)
    rlt = client.getDumpNodeGPU(node)
    print(sav)
    print(rlt['data'][0]['data'][-20:])

def test5(minutes, flag):
    client = BrightRestClient()
    node_list = ['workergpu{}'.format(idx) for idx in range(17,18)]
    rlt       = client.getAllGPUAvg(node_list, minutes, intervalFlag=flag) 
    print(rlt)
    
def test6():
    client = BrightRestClient()
    node_list = ['workergpu{}'.format(idx) for idx in range(17,18)]
    d = client.getAllGPU_raw (node_list)
    print(d)

def main():
    t1=time.time()
    test6 ()
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
