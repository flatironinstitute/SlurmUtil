#!/usr/bin/env python00

import time
import os, requests, sys
import urllib3
from collections import defaultdict

import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger      = config.logger
bright_url  = config.APP_CONFIG["bright"]["url"]
cert_dir    = config.APP_CONFIG["bright"]["cert_dir"]
bright_cert = ('{}/cert.pem'.format(cert_dir), '{}/cert.key'.format(cert_dir))
gpu_avg_period = config.APP_CONFIG["display_default"]["heatmap_avg"]["gpu"]

relay_url   = config.APP_CONFIG["bright"]["relay_url"]
class BrightRelayClient:
    def __init__(self, url_input=relay_url, gpu_avg_period=gpu_avg_period):
        self.base_url  = url_input
        self.gpu_avg_period=gpu_avg_period
        print("URL is {}".format(self.base_url))

    # query bright all gpu data 
    def getLatestGPUAvg (self, node_list=None, node_regex=None, minutes=gpu_avg_period):
        url = "http://{}/getLatestGPUAvg?minutes={}".format(self.base_url, self.gpu_avg_period)
        if node_list:
           url = "{}&nodeList={}".format(url, node_list)
        elif node_regex:
           url = "{}&nodes={}".format(url, node_regex)

        try:
            r     = requests.get(url)
        except Exception as e:
            print("Cannot connect to Bright Relay Server {}. Exception {}".format(url, e))
            return None
        print("Return {}".format(r.json()))
        
        return int(time.time()), r.json()

class BrightRestClient:
    _instance        = None

    @staticmethod
    def getInstance (url_input=None, gpu_avg_period=gpu_avg_period):
        if BrightRestClient._instance == None:
           BrightRestClient (url_input)
        ins = BrightRestClient._instance
        
        return ins

    def __init__(self, url_input=None, gpu_avg_period=gpu_avg_period):
        #? use session
        if BrightRestClient._instance != None:
           raise Exception("This class is a singleton!")
        else:
           self.base_url  = bright_url if not url_input else url_input
           print("URL is {}".format(self.base_url))
           self.cert      = bright_cert
           self.gpu_ts    = 0        # cache data of getAllGPUAvg
           self.gpu_data  = None
           self.gpu_avg_period=gpu_avg_period
           BrightRestClient._instance = self
  
    def set_gpu_avg_period (self, m):
        self.gpu_avg_period= m
        self.gpu_ts     = 0      # 

    #latest compared with dump, dump, then average, is easier to get consistent result
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/latest?measurable=gpu_utilization:gpu0', verify=False, cert=('/mnt/home/yliu/projects/bright/prometheus.cm/cert.pem', '/mnt/home/yliu/projects/bright/prometheus.cm/cert.key'))
    #r = requests.get('https://ironbcm:8081/rest/v1/monitoring/dump?entity={}&measurable=gpu_utilization:{}&start=-{}h'.format(node,gpuId,hours), verify=False, cert=cert_files)
    #REST API: page 56 of https://support.brightcomputing.com/manuals/9.0/developer-manual.pdf
    #Fixed time format: [YY/MM/DD]HH:MM[:SS], enclised in double quotes, unix epoch time
    #                   now
    #                   realtive time: startime can use "-" (earlier than the fixed end time), endtime can use "+" (time later to the fixed start time), seconds(s), minutes(m), hours(h), days(d)
    #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'},
    # intervals=0 (default, = raw data), that is  

    def query (self, query):
        ts       = int(time.time())
        q_str    = '{}/{}&epoch=1'.format(self.base_url,query)
                              #epoch: time stamp as unix epoch

        logger.info("query_str={}".format(q_str))
        try:
           r     = requests.get(q_str, verify=False, cert=self.cert)
        except Exception as e:
           logger.error("Cannot connect to Bright. Exception {}".format(e))
           return ts, []
        return ts, r.json().get('data', [])
        
    # query bright all gpu data 
    def _getAllGPU_raw (self, max_gpu_cnt=4, intervalFlag=False):
        ts       = int(time.time())
        if (int(ts)- self.gpu_ts) < 60:
            logger.info ("less than 60 seconds from last query, return saved gpu data")
            return self.gpu_ts, self.gpu_data

        #measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        if intervalFlag:
           intervals= self.gpu_avg_period* 6            # bright returns one sample per 10 seconds at most, use intervals will leave None at the end, this is for comparison and test purpose 
           q_str    = 'dump?measurable=gpu_utilization:gpu[0-9]&start=-{}m&intervals={}'.format(self.gpu_avg_period,intervals)
        else:
           # intervals=0 (default, = raw data), that is  
           q_str    = 'dump?measurable=gpu_utilization:gpu[0-9]&start=-{}m'.format(self.gpu_avg_period)
                              #epoch: time stamp as unix epoch
        
        ts, q_rlt = self.query(q_str)

        # divide raw data by node and gpu
        d     = defaultdict(lambda:defaultdict(list)) 
        for item in q_rlt:
            gpu_id   = item['measurable'].split(':')[1]  # remove gpu_utilization: gpu0, gpu1...
            d[gpu_id][item['entity']].append(item)

        self.gpu_ts, self.gpu_data = ts, dict(d)
        logger.debug("query take time {}".format(time.time()-ts))
        return ts, dict(d)

    # query bright gpu data on node_list
    def _getGPU_raw (self, node_list, start_ts, max_gpu_cnt=4):
        start    = time.time()

        entities = ','.join(node_list)
        #measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
        q_str    = 'dump?entity={}&measurable=gpu_utilization:gpu[0-9]&start={}'.format(entities,int(start_ts))
        ts, q_rlt= self.query(q_str)
        
        d        = defaultdict(lambda:defaultdict(list)) 
        for item in q.rlt:
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
    # called by index and heatmap
    def getLatestGPUAvg (self, node_list, minutes=gpu_avg_period, max_gpu_cnt=4, intervalFlag=False):
        if (minutes != self.gpu_avg_period):
           self.set_gpu_avg_period (minutes)

        ts,d  = self._getAllGPU_raw (max_gpu_cnt, intervalFlag)
        rlt   = defaultdict(dict)
        for gpu, gpu_nodes in d.items():
            for node in node_list:
            #for node, seq in gpu_nodes.items():
                seq = gpu_nodes.get(node,[])
                if not seq:  # no data
                   rlt[gpu][node] = 0
                else: # calculate average, notice that the data is not even intervaled
                   rlt[gpu][node]  = BrightRestClient._calculateRawAvg(seq, ts-minutes*60, ts)

        rlt   = dict(rlt)
        return ts, rlt

    #node_dict {'workergpu00':{'gpu0':job}...}
    #called by index
    def getAllGPUAvg_jobs (self, node_dict, start_ts, max_gpu_cnt=4):
        ts,d  = self._getGPU_raw (list(node_dict.keys()), start_ts, max_gpu_cnt)

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
        #gpus_util = ','.join(['gpu_utilization:gpu{}'.format(i) for i in gpu_list])
        req_str   = '{}/dump?entity={}&measurable=gpu_utilization:gpu[0-9]&start={}&epoch=1'.format(self.base_url, nodes, start_ts)
        r         = requests.get(req_str, verify=False, cert=self.cert)
        d         = r.json()['data']   #[{'entity': 'workergpu16', 'measurable': 'gpu_utilization:gpu0', 'raw': 0.3096027944984667, 'time': 1584396000000, 'value': '31.0%'}, 
        rlt       = defaultdict(list)                 #{'workergpu16.gpu0':[[ts,val],]
        for item in d:
            if msec:
               rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([item['time'], item['raw']])
            else:
               rlt['{}.{}'.format(item['entity'],item['measurable'].split(':')[1])].append([int(item['time']/1000), item['raw']])

        return dict(rlt)

    # get gpu and mem usage starting from start in seconds
    def getNodesGPU_Mem (self, node_list, start, gpu_list=[], msec=True):
        entities = ','.join(node_list)
        mea_list = ['gpu_utilization', 'gpu_fb_used']
        measures = ','.join(['{}:gpu[0-9]'.format(m) for m in mea_list])
        q_str    = 'dump?entity={}&measurable={}&start={}&epoch=1'.format(entities,measures,start)
        q_ts, d  = self.query(q_str)
        
        rlt      = {}
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
                idx = 0
                while seq[idx][0]<start_ts:                # smaller means the same value last
                   seq[idx][0] = start_ts
                   idx += 1
                while idx > 1:                             # remove earlier ones
                   seq.pop(0)
                   idx -= 1

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
    q_str  = 'dump?measurable=gpu_utilization:gpu[0-9],gpu_fb_used:gpu[0-9]&start=-5m'
    print('query: {}'.format(q_str))
    r = client.query(q_str)
    print('result: {}'.format(r))

def test2():
    client = BrightRestClient()
    curr   = int(time.time())
    rlt    = client.getNodesGPU_Mem(['workergpu34'], 1647882272)
    print('{}'.format(rlt))
    return rlt

def test3():
    client = BrightRestClient()
    node_list = ['workergpu{}'.format(idx) for idx in range(34,39)]
    r = client.getLatestGPUAvg (node_list)
    print('result: {}'.format(r))
    time.sleep(5)
    r = client.getLatestGPUAvg (node_list)
    print('result: {}'.format(r))
        
def test5(minutes, flag):
    client = BrightRestClient()
    rlt       = client.getLatestGPUAvg(node_list, minutes, intervalFlag=flag) 
    print(rlt)
    
def test6():
    client = BrightRestClient()
    d = client._getAllGPU_raw ()
    print(d)

def test7():
    client = BrightRestClient()
    node_list = ['workergpu46']
    d      = client.getNodeGPU_Mem (node_list, 1)
    print(d)

def test8():
    client = BrightRestClient.getInstance()

    minutes = 5
    max_gpu_cnt = 4
    start    = time.time()
    measures = ','.join(['gpu_utilization:gpu{}'.format(i) for i in range(max_gpu_cnt)])
    query    = 'dump?measurable={}&start=-{}m&epoch=1'.format(measures,minutes)

    client.query(query)

def test9():
    client = BrightRelayClient()
    client.getLatestGPUAvg (node_regex='workergpu00.*')

def main():
    t1=time.time()
    test9 ()
    #if len(sys.argv) < 3:
    #   test5(int(sys.argv[1]), False)
    #else:
    #   test5(int(sys.argv[1]), True)
    #test3()
    #test4(sys.argv[1])
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
