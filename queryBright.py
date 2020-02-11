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

    def getDumpGPU (self, node, gpuId='gpu0'):
        #default is about 1 value per 5 minute and one hour of history
        r = requests.get('https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable=gpu_utilization:{},start=-72h'.format(node,gpuId), verify=False, cert=self.cert)
        j = r.json() #{'entity': 'workergpu00', 'measurable': 'gpu_utilization:gpu1', 'raw': 1.0, 'time': '2020/02/10 16:51:21', 'value': '100.0%'}
        if j['data']:
           d   = [[MyTool.str2ts(item['time']), item['raw']] for item in j['data']]
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

def test2():
    client = BrightRestClient()
    rlt    = client.getDumpGPU('workergpu00')

def test3():
    client = BrightRestClient()


def main():
    t1=time.time()
    test2()
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
