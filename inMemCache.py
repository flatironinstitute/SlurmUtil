#!/usr/bin/env python

import ast
import json
import logging
import os.path
import pdb
import sys
import threading, time
import urllib.request

from collections import defaultdict, deque
from datetime    import datetime, timezone, timedelta
from bisect      import bisect_right, bisect_left
from statistics  import mean 

import MyTool
import querySlurm
import config

logger   = config.logger

STATE_IDX,DELTA_IDX,TS_IDX,USER_INFO_IDX = range(4)  #incoming data and self.data {node: [status, delta, ts, users_procs, ...]...}
USER_PROC_IDX      = 7  # in users_procs [uname, uid, user_alloc_cores, proc_cnt, totCPURate, totRSS, totVMS, procs, totIO, totCPU]
PID_IDX,CPU_UTIL_IDX,CREATE_TIME_IDX,USER_TIME_IDX,SYS_TIME_IDX,RSS_IDX,VMS_IDX,CMD_IDX,IO_IDX,JID_IDX,FDS_IDX,READ_IDX,WRITE_IDX,UID_IDX,THREADS_IDX   = range(15)
                        # in procs [[pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, in    tervalIOByteAvg, jid],...]



#keep a in-mem cache that can be queried about running jobs and other jobs in the past 3(?) days
class InMemCache:
    TIME_WINDOW   = 3 * 24 * 3600             # 3 days' time window
    CPU_IDX       = 7
    RSS_IDX       = 9
    IO_IDX        = 10
    READ_IDX      = 12
    WRITE_IDX     = 13
    def __init__(self, debugMode = False):
        self.node_job      = defaultdict(lambda:defaultdict(list))   # node:  {jid: JobNodeSeries}, ...
        self.node_user     = defaultdict(lambda:defaultdict(list))   # node: {uid:  JobNodeSeries}, ...
        self.nodes         = defaultdict(lambda:{'last_ts':0, 'first_ts':0, 'seq':[]})   # node: {'first_ts':, 'last_ts':, 'seq':[(ts, status)]}
        self.jobs          = defaultdict(lambda:{'submit_time':0, 'start_time':0, 'preempt_time':0, 'suspend_time':0, 'resize_time':0, 'nodes':[], 'seq':[]})   # jid: {'first_ts':, 'last_ts':, 'seq':[(ts, status)]}
        self.start         = time.time()

    # return users and job usage on the node in a dict based on incoming nodeInfo
    def retrieveUsageOnNode (self, nodeName, nodeInfo, pyslurmJob):
        result = {}       #{uid: []}
        jobRlt = {}       #{jid: []}
        ts     = int(nodeInfo[TS_IDX])
        # create a usage unit
        #if 'ALLOCATED' in nodeInfo[0] or 'MIXED' in nodeInfo[0]: # node is allocated
        if len(nodeInfo) > USER_INFO_IDX:                         # node reports values 
           # loop over every user
           for user, uid, coreNum, procNum, cpuUtil, rss, vms, procs, io, cpuTime, *etc in nodeInfo[USER_INFO_IDX:]:
               jids = set()
               if len(procs)>0 and len(procs[0])>11:
                  jids   = set([p[JID_IDX] for p in procs])
                  jids.discard(-1)
                  for jid in jids:
                      job_procs   = [ p for p in procs if p[JID_IDX]==jid]
                      j_cpuUtil   = sum([ p[CPU_UTIL_IDX] for p in job_procs])
                      j_cpuTime   = sum([ p[USER_TIME_IDX]+p[SYS_TIME_IDX] for p in job_procs])
                      j_rss       = sum([ p[RSS_IDX]>>10  for p in job_procs])
                      j_vms       = sum([ p[VMS_IDX]>>10  for p in job_procs])
                      j_ioBps     = sum([ p[IO_IDX]       for p in job_procs])
                      j_read      = sum([ p[READ_IDX]     for p in job_procs])
                      j_write     = sum([ p[WRITE_IDX]    for p in job_procs])
                      if jid in pyslurmJob:
                          jobRlt[jid] = [ts, nodeName, uid, jid, nodeInfo[STATE_IDX], pyslurmJob[jid]['cpus_allocated'].get(nodeName,0), len(job_procs), j_cpuUtil, j_cpuTime, j_rss, j_vms, j_ioBps, j_read, j_write]
                      else:
                         logger.warning("Job {} is not in pyslurmJob when retrieveUsageOnNode {}".format(jid, nodeName))
               result[uid] = [ts, nodeName, uid, jids, nodeInfo[STATE_IDX], coreNum, procNum, cpuUtil, cpuTime, int(rss>>10), int(vms>>10), io]

        return result, jobRlt

    # add data from updateSlurmData
    def append (self, nodeData, jobTS, pyslurmJob, uid2jid=None):
        # loop over every node and every user on node
        for nodename, nodeInfo in sorted(nodeData.items()):
            ts = int(nodeInfo[TS_IDX])
            # compare with last value, make sure ts is sorted
            if ts > self.nodes[nodename]['last_ts']: # ignore the older information
               nodeUserUsage, nodeJobUsage = self.retrieveUsageOnNode(nodename, nodeInfo, pyslurmJob)
               # add the usage to the series
               for uid, userUsage in nodeUserUsage.items():
                   self.node_user[nodename][uid].append((ts, userUsage))
               for jid, jobUsage in nodeJobUsage.items():
                   self.node_job[nodename][jid].append ((ts, jobUsage))
               self.nodes[nodename]['last_ts'] = ts
               self.nodes[nodename]['seq'].append((ts, nodeInfo[0]))
               if not self.nodes[nodename]['first_ts']:
                  self.nodes[nodename]['first_ts'] = ts
            elif ts < self.nodes[nodename]['last_ts']:
               logger.warning("Ignore {}'s information at {}, which is older than the one in record {}".format(nodename, ts, self.nodes[nodename]['last_ts']))

        #remove the old ones
        cut_ts = time.time() - InMemCache.TIME_WINDOW
        #remove old data from self.nodes
        cut_nodes = [ (nodename, node) for nodename, node in self.nodes.items() if node['first_ts']<cut_ts]
        for nodename,node in cut_nodes:
            idx          = bisect_right(node['seq'], (cut_ts,))
            node['seq']  = node['seq'][idx:]
            if len(node['seq']):
               node['first_ts'] = node['seq'][0][0]
            else:
               node['first_ts'] = 0
        #remove old data from self.node_user
        for nodename, userSeries in self.node_user.items():
            cut_users = [ (uid, series) for uid, series in userSeries.items() if len(series)>0 and series[0][0]<cut_ts]
            for uid, series in cut_users:
                idx                           = bisect_right(series, (cut_ts,))
                self.node_user[nodename][uid] = self.node_user[nodename][uid][idx:]

        # deal with job data
        activeJob  = [jid for jid, job in pyslurmJob.items() if job['job_state'] in ['PENDING', 'RUNNING', 'PREEMPTED', 'SUSPENDED', 'RESIZING']]
        #logger.debug("active jobs {}".format(activeJob))
        for jid in activeJob:
            self.jobs[jid]['seq'].append((jobTS, pyslurmJob[jid]['job_state'], pyslurmJob[jid].get('job_inst_util',-1), pyslurmJob[jid].get('job_io_bps',-1), pyslurmJob[jid].get('job_mem_util',-1)))
            if not self.jobs[jid]['submit_time']:
               self.jobs[jid]['submit_time']  = pyslurmJob[jid]['submit_time']
            if (not self.jobs[jid]['start_time']) and pyslurmJob[jid]['start_time']:
               self.jobs[jid]['start_time']   = pyslurmJob[jid]['start_time']
            if (not self.jobs[jid]['nodes']) and pyslurmJob[jid]['cpus_allocated']:
               self.jobs[jid]['nodes']  = list(pyslurmJob[jid]['cpus_allocated'].keys())
            if pyslurmJob[jid]['suspend_time']:
               self.jobs[jid]['suspend_time'] = pyslurmJob[jid]['suspend_time']
            #if pyslurmJob[jid]['preempt_time']:
            #   self.jobs[jid]['preempt_time'] = pyslurmJob[jid]['preempt_time']
            if pyslurmJob[jid]['resize_time']:
               self.jobs[jid]['resize_time']  = pyslurmJob[jid]['resize_time']
            #logger.debug("active jid {} done".format(jid))
            
        #remove data of not running jobs from self.job_node
        doneJob   = [jid for jid in pyslurmJob              if jid not in activeJob and jid in self.jobs]
        if doneJob:
           logger.debug('remove done jobs {} from self.jobs'.format(doneJob))
           for jid in doneJob:
               if jid not in self.jobs:
                  logger.warning("Job {}({}-{}) is not in self.jobs. {}".format(jid, pyslurmJob[jid]['start_time'], pyslurmJob[jid]['end_time'], pyslurmJob[jid]))
               else:
                  self.jobs.pop(jid)
                  for node in pyslurmJob[jid]['cpus_allocated'].keys():
                      if jid not in self.node_job[node]:
                         logger.warning("Job {}({}-{}) is not in self.node_job[{}]={}".format(jid, pyslurmJob[jid]['start_time'], pyslurmJob[jid]['end_time'], node, list(self.node_job[node].keys())))
                      else:
                         self.node_job[node].pop(jid)
            
    # query job's history from start_time to now
    def queryJob (self, jid, start_time='', stop_time=''):
        logger.debug("queryJob {}".format(jid))
        cpu_rlt, mem_rlt, read_rlt, write_rlt = [], [], [], []
        if jid not in self.jobs:
           logger.warning("Job {} is not in self.jobs={}".format(jid, list(self.jobs.keys())))
           return None
        
        #logger.debug("\t{}".format(self.jobs[jid]))
        for node in self.jobs[jid]['nodes']:
            usage_seq = self.node_job[node][jid] 
            if usage_seq and (start_time or stop_time):        #start, stop constraint
               firstTS   = usage_seq[0][0]
               lastTS    = usage_seq[-1][0]
               if start_time and start_time > firstTS + 60 :  # relax 60 seconds
                  usage_seq = [(ts, usage) for (ts, usage) in usage_seq if ts >= start_time]
               if stop_time and stop_time < lastTS - 60:      # relax 60 seconds
                  usage_seq = [(ts, usage) for (ts, usage) in usage_seq if ts <= stop_time]
            #if not usage_seq:                  #no data
            #   continue
            #empty usage_seq will return a empty []
            cpu_rlt.append  ({'name':node, 'data':[ [ts, usage[InMemCache.CPU_IDX]]   for (ts, usage) in usage_seq ]})
            mem_rlt.append  ({'name':node, 'data':[ [ts, usage[InMemCache.RSS_IDX]]   for (ts, usage) in usage_seq ]})
            read_rlt.append ({'name':node, 'data':[ [ts, usage[InMemCache.READ_IDX]]  for (ts, usage) in usage_seq ]})
            write_rlt.append({'name':node, 'data':[ [ts, usage[InMemCache.WRITE_IDX]] for (ts, usage) in usage_seq ]})

        return self.jobs[jid], cpu_rlt, mem_rlt, read_rlt, write_rlt

    # query worker's avg utilization, must be shorter than 3 days:
    # get node's average resource util
    def queryNodeAvg (self, node, minutes=5):
        cpu_rlt, mem_rlt = 0, 0
        start_ts         = self.nodes[node]['last_ts'] - minutes*60
        if node not in self.nodes or not self.nodes[node]:
           logger.warning("queryNodeAvg: Node {} is not in cache".format(node, list(self.nodes.keys())))
           return 0
        if start_ts > self.nodes[node]['first_ts']+minutes*60*0.2:  # 20% relax on period inclusion 
           logger.warning("queryNodeAvg: Node {} requested period {}- is not completely in cache ({}-{})".format(node, start_ts, self.nodes[node]['first_ts'], self.nodes[node]['last_ts']))

        for uid, user_usage in self.node_user[node].items():
            #logger.debug("\t{}:{}".format(node, self.node_user[node][uid]))
            user = MyTool.getUser (uid)
            idx  = bisect_left(user_usage, (start_ts,))
            seq  = user_usage[idx:]
            
            try:
               cpu_rlt += mean([usage[InMemCache.CPU_IDX] for (ts, usage) in seq])      #usage is evenly distributed, thus just mean, TODO: have problem when node is down and not sending data 
            #mem_rlt += mean([usage[InMemCache.RSS_IDX] for (ts, usage) in seq])      #usage is evenly distributed, thus just mean 
            except BaseException as e:
               print("ERROR {} uid={} usage={} start={} idx={} ".format(e, uid, user_usage, start_ts, idx))
        logger.debug("\tnode={}:cpu_rlt={}, mem_rlt={}".format(self.nodes[node], cpu_rlt, mem_rlt))
        return cpu_rlt 

    # query worker's history, must be shorter than 3 days:
    def queryNode (self, node, start_ts=None, end_ts=None):
        cpu_rlt, mem_rlt, io_rlt= [], [], []

        if node not in self.nodes:
           logger.info("queryNode: Node {} is not in cache".format(node, list(self.nodes.keys())))
           return None, [], [], []
        if start_ts and start_ts < self.nodes[node]['first_ts']-300:  # five minutes gap is allowed
           logger.info("queryNode: Node {} period {}-{} is not completely in cache ({}-{})".format(node, start_ts, end_ts, self.nodes[node]['first_ts'], self.nodes[node]['last_ts']))
           return None, [], [], []
        # else start_ts==None or start_ts >= self.nodes[node]['first_ts']-300

        for uid, user_usage in self.node_user[node].items():
            #logger.debug("\t{}:{}".format(node, self.node_user[node][uid]))
            user = MyTool.getUser (uid)
            seq  = user_usage
            if start_ts and start_ts >= self.nodes[node]['first_ts']-300:
               idx = bisect_left(seq, (start_ts,))
               seq = user_usage[idx:]
            if end_ts and end_ts >= self.nodes[node]['last_ts']-300:
               idx = bisect_right(seq, (end_ts,))
               seq = user_usage[:idx-1]
            cpu_rlt.append ({'name':user, 'data':[ [ts, usage[InMemCache.CPU_IDX]] for (ts, usage) in seq ]})       
            mem_rlt.append ({'name':user, 'data':[ [ts, usage[InMemCache.RSS_IDX]] for (ts, usage) in seq ]})       
            io_rlt.append  ({'name':user, 'data':[ [ts, usage[11]] for (ts, usage) in seq ]})       

        logger.debug("\tnode={}:cpu_rlt={}".format(self.nodes[node], cpu_rlt))
        return self.nodes[node], cpu_rlt, mem_rlt, io_rlt

    # query user's history, must be shorter than 3 days:
    def queryUser (self, user, start_time):
        cpu_rlt, mem_rlt, io_rlt = [], [], []
        if user not in self.users:
           return cpu_rlt, mem_rlt, io_rlt
        
        userSeries = self.users[user]  # {jid: (ts, usage), ...}
        for node, user_usage in userSeries.items():
            cpu_usage = [ [ts*1000, usage[0]] for (ts, data) in user_usage ]
            cpu_rlt.append({'name':node, 'data':cpu_usage})       

        return cpu_rlt, mem_rlt, io_rlt

class InMemLog:
    #var o_title    = {{'source':'Source', 'ts':'Update Time', 'msg':'Message'}}
 
    MAX_COUNT     = 1024
    def __init__(self, max_len=MAX_COUNT):
        self.log           = deque(maxlen=max_len)               # logRecord {'source':, 'funcName':, 'ts':, 'msg:'}
    def append(self, logRecord):
        #print("---logRecord={}".format(logRecord))
        self.log.append({'source':'{}.{}'.format(logRecord.get('module','unknown'),logRecord.get('funcName','unknown')), 'ts':logRecord.get('created',time.time()), 'msg':logRecord.get('msg','unknown')})
    #revTS if want in the reverse order of timestamp
    def getAllLogs(self, reverse=True):
        if reverse:
           dq = self.log.copy()
           dq.reverse()
           return list(dq)
        return list(self.log)

def getAll_uid2jid (jobData):
    uid2jid = defaultdict(lambda: defaultdict(list)) 
    runJob  = [jid for jid,jinfo in jobData.items() if jinfo['job_state']=='RUNNING']
    for jid in runJob:
        for nodeName, coreCount in jobData[jid].get('cpus_allocated', {}).items():
            if jobData[jid].get('user_id', None):
               uid2jid[nodeName][jobData[jid].get('user_id')].append(jid)
    return uid2jid

def jid2uid (job):
    return job['user_id']
    
if __name__=="__main__":
   cache     = InMemCache()
   while True:
      contents1 = urllib.request.urlopen("http://scclin011:8126/getNodeData").read()
      contents2 = urllib.request.urlopen("http://scclin011:8126/getAllJobData").read()
      nodeData    = ast.literal_eval(contents1.decode("utf-8"))
      ts, jobData = ast.literal_eval(contents2.decode("utf-8"))
      uid2jid   = getAll_uid2jid(jobData)
      cache.append (nodeData, ts, jobData, uid2jid)

      time.sleep(60)


