#!/usr/bin/env python00

import time
t1=time.time()
import os, requests, sys
import MyTool

from collections import defaultdict
from statistics  import mean   #fmean faster than mean, but not until 3.8

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

    # get the avg utilization of last {minutes} minutes
    #reture {'time': , 'gpu0':{'workergpu00':0.34 ... }, }
    def getAllGPUAvg (self, node_list, minutes=5, max_gpu_cnt=4):
        entities = ','.join(node_list)
        measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        ts       = int(time.time())
        q_str    = '{}/dump?entity={}&measurable={}&start=-{}m&epoch=1'.format(self.base_url,entities,measures,minutes)
                              #epoch: time stamp as unix epoch
        r        = requests.get(q_str, verify=False, cert=self.cert)
        # deal with the data
        # divide by node and gpu
        d       = defaultdict(lambda:defaultdict(list)) 
        for item in r.json()['data']:
            gpu_id   = item['measurable'].split(':')[1]
            d[gpu_id][item['entity']].append(item)

        # calculate average
        rlt      = defaultdict(dict)
        for gpu, gpu_nodes in d.items():
            for node, seq in gpu_nodes.items():
                rlt[gpu][node] = mean([item['raw'] for item in seq])
        return ts, dict(rlt)

    def getDumpRequest (self, node_list, max_gpu_cnt=4, minutes=None):
        entities = ','.join(node_list)
        measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        q_str    = '{}/dump?entity={}&measurable={}&epoch=1'.format(self.base_url,entities,measures)
        if minutes:
           q_str = '{}&start=-{}m'.format(q_str, minutes)
        return q_str

    def getDumpNodeGPU (self, node, hours=72, max_gpu_cnt=4):
        q_str    = self.getDumpRequest([node], minutes=hours * 60, max_gpu_cnt=max_gpu_cnt)
        ts       = int(time.time())
        q_rlt    = requests.get(q_str, verify=False, cert=self.cert)
        print('---{}'.format(q_str))
        # deal with the data
        # divide by gpu
        q_d      = defaultdict(list)  #{'gpu0':[[time,value],...],...}
        for item in q_rlt.json()['data']:
            gpu_id  = item['measurable'].split(':')[1]
            q_d[gpu_id].append([item['time'], item['raw']])
        print('---{}'.format(q_d))
        # deal with 1st time, if less than start time, then cute
        
        idx = 0
        rlt = {'node': node, 'time': int(time.time()), 'data': []}
        while True:
           gpuId = 'gpu{}'.format(idx)
           d = self.getDumpGPU(node, gpuId, hours=hours)
           if d:
              rlt['data'].append({'name': gpuId, 'data': d})
              idx += 1
           else:
              return rlt

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
           
    def getGPU (self, node_list, start_ts, max_gpu_id=3):
        nodes     = ','.join(node_list)
        gpus_util = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_id+1)])
        req_str   = 'https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable={}&start={}&epoch=1'.format(nodes,gpus_util,start_ts)
        #req_str   = 'https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable={}&start={}&epoch=1&intervals=10000'.format(nodes,gpus_util,start_ts) #intervals not very useful
        r         = requests.get(req_str, verify=False, cert=self.cert)
        print('---{}'.format(r.json()))
        d         = r.json()['data']   #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'}, 
        rlt       = defaultdict(list)                 #{'workergpu16.gpu0':[[ts,val],]
        for item in d:
            rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([item['time'], item['raw']])
        return dict(rlt)
    #@staticmethod

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

def main():
    t1=time.time()
    
    #test2(sys.argv[1])
    #test3()
    test4(sys.argv[1])
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
