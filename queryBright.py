#!/usr/bin/env python00

import time
t1=time.time()
import subprocess
import os
import pyslurm
import MyTool
import requests

from collections import defaultdict
from datetime import datetime, date, timezone, timedelta

class BrightRestClient:
    def __init__(self):
        #? use session
        self.base_url  = "https://ironbcm:8081/rest/v1/monitoring/"
        self.cert      = ('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key')
  
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:gpu0', verify=False, cert=('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key'))
    def getLatestGPU (self, gpuId='gpu0'):
        r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:{}'.format(gpuId), verify=False, cert=self.cert)
        j = r.json() #j['data'][0]={'age': 144.381, 'entity': 'workergpu00', 'measurable': 'gpu_utilization', 'raw': 0.0, 'time': 1580872073796, 'value': '0.0%'}
        if j['data']:
           d   = dict([(item['entity'], item['raw']) for item in j['data']])
           return d
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

    def getDumpAllGPU (self, node):
        idx = 0
        rlt = {'node': node, 'time': int(time.time()), 'data': []}
        while True:
           gpuId = 'gpu{}'.format(idx)
           d = self.getDumpGPU(node, gpuId)
           if d:
              rlt['data'].append({'name': gpuId, 'data': d})
              idx += 1
           else:
              return rlt

    #reture [{'workergpu00':0.34 ... }, ]
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
           
    #@staticmethod

def test1():
    client = BrightRestClient()
    rlt    = client.getLatestAllGPU()
    print(rlt)

def test2(node):
    client = BrightRestClient()
    rlt    = client.getDumpAllGPU(node)
    cnt    = sum([len(item['data']) for item in rlt['data']])
    print('{}: {} samples'.format(node, cnt))
    return rlt

def test3():
    client = BrightRestClient()
    cnt    = 0
    for i in range(0, 43):
        node = 'workergpu{:0>2d}'.format(i)
        rlt  = test2(node)
        cnt += sum([len(item['data']) for item in rlt['data']])

    print('Total: {} samples'.format(cnt))
        


def main():
    t1=time.time()
    
    test3()
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
