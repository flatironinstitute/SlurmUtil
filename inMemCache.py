#!/usr/bin/env python

import ast
import json
import logging
import os.path
import pdb
import sys
import threading
import time
import urllib.request

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from bisect import bisect_right

import MyTool
import querySlurm

logger   = MyTool.getFileLogger('inMemCache', logging.DEBUG)  # use module name

#keep a in-mem cache that can be queried about running jobs and other jobs in the past 3(?) days
class InMemCache:
    ONE_HOUR      = 3600
    TIME_WINDOW   = 3 * 24 * 3600             # 3 days' time window
    USER_INFO_IDX = 3
    CPU_IDX       = 7
    RSS_IDX       = 9
    IO_IDX        = 10
    def __init__(self, debugMode = False):
        self.node_job      = defaultdict(lambda:defaultdict(list))   # node:  {jid: JobNodeSeries}, ...
        self.node_user     = defaultdict(lambda:defaultdict(list))   # node: {uid:  JobNodeSeries}, ...
        self.nodes         = defaultdict(lambda:{'last_ts':0, 'first_ts':0, 'seq':[]})   # node: {'first_ts':, 'last_ts':, 'seq':[(ts, status)]}
        self.jobs          = defaultdict(lambda:{'submit_time':0, 'start_time':0, 'preempt_time':0, 'suspend_time':0, 'resize_time':0, 'nodes':[], 'seq':[]})   # jid: {'first_ts':, 'last_ts':, 'seq':[(ts, status)]}

    # return users' usage on the node in a dict
    # nodeInfo format: ['IDLE+POWER', 91.20261883735657, 1574352819.760845]
    # ['MIXED', 93.20821714401245, 1574353973.507513, ['cbertrand', 1544, 1, 5, 0.00847564757921933, 774889472, 3722960896, [[39264, 0.0, 1574090530.73, 0.0, 0.0, 1859584, 116178944, ['/bin/bash', '/cm/local/apps/slurm/var/spool/job421611/slurm_script'], 0]
    def getUsageOnNode (self, nodeName, nodeInfo, jobData):
        result = {}       #{uid: []}
        jobRlt = {}       #{jid: []}
        ts     = int(nodeInfo[2])
        # create a usage unit
        #if 'ALLOCATED' in nodeInfo[0] or 'MIXED' in nodeInfo[0]: # node is allocated
        if len(nodeInfo) > InMemCache.USER_INFO_IDX:                         # node reports values 
           # loop over every user
           for user, uid, coreNum, proNum, cpuUtil, rss, vms, procs, io, cpuTime, *etc in nodeInfo[InMemCache.USER_INFO_IDX:]:
               # jid information is in procs [pid, CPURate, proc['create_time'], proc['cpu']['user_time'], proc['cpu']['system_time'], proc['mem']['rss'], proc['mem']['vms'], proc['cmdline'], IOBps, jid]
               jids = set()
               if len(procs)>0 and len(procs[0])>11:
                  jids   = set([p[9] for p in procs])
                  jids.discard(-1)
                  for jid in jids:
                      job_procs   = [ p for p in procs if p[9]==jid]
                      j_cpuUtil   = sum([ p[1]      for p in job_procs])
                      j_cpuTime   = sum([ p[3]+p[4] for p in job_procs])
                      j_rss       = sum([ p[5]      for p in job_procs])
                      j_vms       = sum([ p[6]      for p in job_procs])
                      j_ioBps     = sum([ p[8]      for p in job_procs])
                      j_read      = sum([ p[10]      for p in job_procs])
                      j_write     = sum([ p[11]      for p in job_procs])
                      jobRlt[jid] = [ts, nodeName, uid, jid, nodeInfo[0], sum(jobData[jid]['cpus_allocated'].values()), len(job_procs), j_cpuUtil, j_cpuTime, j_rss, j_vms, j_ioBps, j_read, j_write]
               lst = [ts, nodeName, uid, jids, nodeInfo[0], coreNum, proNum, cpuUtil, cpuTime, rss, vms, io]
               result[uid] = lst

        #logger.debug("getUsageOnNode {} got userRlt={}\njobRlt={}\n".format(nodeName, result, jobRlt))
        return result, jobRlt

    # add data from updateSlurmData
    def append (self, nodeData, jobTS, jobData, uid2jid=None):
        # loop over every node and every user on node
        for nodename, nodeInfo in sorted(nodeData.items()):
            # compare with last value, make sure ts is sorted
            ts = int(nodeInfo[2])
            if ts > self.nodes[nodename]['last_ts']: # ignore the older information
               nodeUserUsage, nodeJobUsage = self.getUsageOnNode(nodename, nodeInfo, jobData)
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
        activeJob  = [jid for jid, job in jobData.items() if job['job_state'] in ['PENDING', 'RUNNING', 'PREEMPTED', 'SUSPENDED', 'RESIZING']]
        for jid in activeJob:
            self.jobs[jid]['seq'].append((jobTS, jobData[jid]['job_state'], jobData[jid].get('job_inst_util',-1), jobData[jid].get('job_io_bps',-1), jobData[jid].get('job_mem_util',-1)))
            if not self.jobs[jid]['submit_time']:
               self.jobs[jid]['submit_time']  = jobData[jid]['submit_time']
            if (not self.jobs[jid]['start_time']) and jobData[jid]['start_time']:
               self.jobs[jid]['start_time']   = jobData[jid]['start_time']
            if (not self.jobs[jid]['nodes']) and jobData[jid]['cpus_allocated']:
               self.jobs[jid]['nodes']  = list(jobData[jid]['cpus_allocated'].keys())
            if jobData[jid]['suspend_time']:
               self.jobs[jid]['suspend_time'] = jobData[jid]['suspend_time']
            if jobData[jid]['preempt_time']:
               self.jobs[jid]['preempt_time'] = jobData[jid]['preempt_time']
            if jobData[jid]['resize_time']:
               self.jobs[jid]['resize_time']  = jobData[jid]['resize_time']
            
       
        #remove data of not running jobs from self.job_node
        doneJob   = [jid for jid in jobData              if jid not in activeJob and jid in self.jobs]
        if doneJob:
           logger.info('remove done jobs {} from self.jobs'.format(doneJob))
           for jid in doneJob:
               if jid not in self.jobs:
                  logger.warning("Job {}({}-{}) is not in self.jobs. {}".format(jid, jobData[jid]['start_time'], jobData[jid]['end_time'], jobData[jid]))
               else:
                  self.jobs.pop(jid)
                  for node in jobData[jid]['cpus_allocated'].keys():
                      if jid not in self.node_job[node]:
                         logger.warning("Job {}({}-{}) is not in self.node_job[{}]={}".format(jid, jobData[jid]['start_time'], jobData[jid]['end_time'], node, list(self.node_job[node].keys())))
                      else:
                         self.node_job[node].pop(jid)
            
    # query job's history from start_time to now
    def queryJob (self, jid, start_time=None):
        logger.debug("queryJob {}".format(jid))
        cpu_rlt, mem_rlt, read_rlt, write_rlt = [], [], [], []
        if jid not in self.jobs:
           logger.warning("Job {}({}-) is not in self.jobs={}".format(jid, start_time, list(self.jobs.keys())))
           return None, [], [], [], []
        
        #logger.debug("\t{}".format(self.jobs[jid]))
        for node in self.jobs[jid]['nodes']:
            #logger.debug("\t{}:{}".format(node, self.node_job[node][jid]))
            cpu_rlt.append  ({'name':node, 'data':[ [ts*1000, usage[InMemCache.CPU_IDX]] for (ts, usage) in self.node_job[node][jid] ]})       
            mem_rlt.append  ({'name':node, 'data':[ [ts*1000, usage[9]]  for (ts, usage) in self.node_job[node][jid] ]})       
            read_rlt.append ({'name':node, 'data':[ [ts*1000, usage[12]] for (ts, usage) in self.node_job[node][jid] ]})       
            write_rlt.append({'name':node, 'data':[ [ts*1000, usage[13]] for (ts, usage) in self.node_job[node][jid] ]})       

        return self.jobs[jid], cpu_rlt, mem_rlt, read_rlt, write_rlt

    # query worker's history, must be shorter than 3 days:
    def queryNode (self, node, start_ts=None, end_ts=None):
        cpu_rlt, mem_rlt, read_rlt, write_rlt = [], [], [], []
        if node not in self.nodes:
           logger.debug("Node {} is not in cache".format(node, list(self.nodes.keys())))
           return None, [], [], [], []
        if start_ts and start_ts < self.nodes[node]['first_ts']-300:  # five minutes gap is allowed
           logger.debug("Node {}: period {}-{} is not in cache ({}-{})".format(node, start_ts, end_ts, self.nodes[node]['first_ts'], self.nodes[node]['last_ts']))
           return None, [], [], [], []

        for uid, user_usage in self.node_user[node].items():
            logger.debug("\t{}:{}".format(node, self.node_user[node][uid]))
            user = MyTool.getUser (uid)
            seq  = user_usage
            if start_ts and start_ts > self.nodes[node]['first_ts']+300:
               idx = bisect_right(seq, (start_ts,))
               seq = user_usage[idx:]
            if end_ts and end_ts >= self.nodes[node]['last_ts']-300:
               idx = bisect_right(seq, (end_ts,))
               seq = user_usage[:idx-1]
            cpu_rlt.append  ({'name':user, 'data':[ [ts*1000, usage[InMemCache.CPU_IDX]] for (ts, usage) in seq ]})       
            mem_rlt.append  ({'name':user, 'data':[ [ts*1000, usage[9]] for (ts, usage) in seq ]})       
            read_rlt.append ({'name':user, 'data':[ [ts*1000, usage[9]] for (ts, usage) in seq ]})       
            write_rlt.append({'name':user, 'data':[ [ts*1000, usage[9]] for (ts, usage) in seq ]})       

        logger.debug("\tnode={}:cpu_rlt={}".format(self.nodes[node], cpu_rlt))
        return self.nodes[node], cpu_rlt, mem_rlt, read_rlt, write_rlt

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


