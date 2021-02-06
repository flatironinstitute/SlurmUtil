#!/usr/bin/env python

from __future__ import print_function

import logging
import pyslurm
import re, sys, time

import MyTool, config

from collections import defaultdict
from datetime import datetime
from operator import attrgetter, itemgetter
from querySlurm import SlurmCmdQuery

logger   = config.logger
#MyTool.getFileLogger('SlurmEntities', logging.DEBUG)  # use module name

PEND_EXP={
    'QOSMaxCpuPerUserLimit': 'Will exceed QoS User CPU  limit ({max_cpu_user}). User {user} already alloc {curr_cpu_user}  CPUs in {partition}.', # QOS MaxTRESPerUser exceeded (CPU) 
    'QOSMaxNodePerUserLimit':'Will exceed QoS User Node limit ({max_node_user}). User {user} already alloc {curr_node_user} Nodes in {partition}.',	                         # QOS MaxTRESPerUser exceeded (Node)
    'QOSGrpNodeLimit':       'Will exceed QoS Group Node limit ({max_node_grp}). Group already alloc {curr_node_grp} Nodes in {partition}.', # QOS GrpTRES exceeded (Node)
    'QOSGrpCpuLimit':        'Will exceed QoS Group CPU limit  ({max_cpu_grp}). Group already alloc {curr_cpu_grp}  CPUs in {partition}.',
    'QOSMaxWallDurationPerJobLimit': 'Job time {job_time_limit} exceed QoS {qos}\'s MaxWallDurationPerJob limit ({qos_limit}).',
    'QOSMaxGRESPerUser':     'Will exceed QoS User GPU limit ({max_gpu_user}). User {user} already alloc {curr_gpu_user} GPUs in {partition}.',
    'QOSMinGRES':            'QOS MinTRESPerJob not reached (CPU).',
    'QOSMaxNodePerJobLimit': 'Exceed QOS Job TRES limit ({max_tres_pj}).',
    'Dependency':            'Job dependencies ({dependency}) not satisfied.', #/* dependent job has not completed */
    'Priority':              'Resources being reserved for higher priority job. Partition {partition} queue higher priority jobs {higher_job}.', #/* higher priority jobs exist */
    'Resources':             'Required resources not available. Partition {partition} have {avail_node} requested {feature} nodes and {avail_cpu} CPUs.', #required resources not available
    'JobArrayTaskLimit':     'Job array ({array_task_str}) reach max task limit {array_max_tasks}. Tasks {array_tasks} are running.',
    'JobHeldUser':           "Job is held by User.",     #check with sprio -j command, which only work for FIFO
    'Reservation':           'Waiting for advanced reservation.',
    'QOSJobLimit':           'Quality Of Service (QOS) job limit reached',
    'QOSResourceLimit':      'Quality Of Service (QOS) resource limit reached',
    'QOSTimeLimit':          'Quality Of Service (QOS) time limit reached',
}

MAX_LIMIT = 2**32 - 1  #429496729, biggest integer

class SlurmEntities:
  #two's complement -1

  def __init__ (self):
    self.getPyslurmData ()

  def getPyslurmData (self): 
    self.config_dict    = pyslurm.config().get()
    self.qos_dict       = pyslurm.qos().get()
    self.partition_dict = pyslurm.partition().get()
    py_node             = pyslurm.node()
    self.node_dict      = py_node.get()
    self.ts_node_dict   = py_node.lastUpdate()        # must retrieve ts as above seq
    py_job              = pyslurm.job()
    self.job_dict       = py_job.get()
    self.ts_job_dict    = py_job.lastUpdate()
    #self.res_future     = [res for res in pyslurm.reservation().get().values() if res['start_time']>time.time()]  # reservation in the future
    self.res_future     = []
    r                   = pyslurm.reservation()   # slurm20: pyslurm.reservation().get() core dump
    if r.ids():
      for name,res in pyslurm.reservation().get().items():
        if res['start_time']>time.time():  # reservation in the future
           res['job_id']    = name          # just for simplicity
           self.res_future.append (res)
    self.user_assoc_dict= SlurmCmdQuery.getAllUserAssoc()

    self.part_node_cpu  = {}  # {'gen': [40, 28], 'ccq': [40, 28], 'ib': [44, 28], 'gpu': [40, 36, 28], 'mem': [96], 'bnl': [40], 'bnlx': [40], 'genx': [44, 40, 28], 'amd': [128, 64]}
    for pname,part in self.partition_dict.items():
        self.part_node_cpu[pname] = sorted(set([self.node_dict[name]['cpus'] for name in MyTool.nl2flat(part['nodes'])]), reverse=True)

    # extend node_dict by adding running_jobs, gpu_total, gpu_used
    self.extendNodeDict ()

    # extend partition_dict by adding node_flats, flag_shared, running_jobs, pending_jobs...
    for pname, part in self.partition_dict.items():
        self.extendPartitionDict (pname, part)

  #return nodeLimit, cpuLimit (max value TCMO)
  #'max_tres_pu': '1=320,2=9000000,1001=32'
  #1: cpu, 4: node, 1001: gpu
  @staticmethod
  def getQoSTresLimit (tres_str, defaultValue=MAX_LIMIT):
    d = {}
    if tres_str:
      for tres in tres_str.split(','):
        t, value = tres.split('=')
        d[t]     = value
    
    return int(d.get('4', defaultValue)), int(d.get('1', defaultValue)), int(d.get('1001', defaultValue))

  #get allocation of all jobs in partition
  #def getAllocInPartition(partition, job_dict):
  #  jobLst     = [ job for job in job_dict.values() if job['partition'] == partition and job['job_state']=='RUNNING']
  #  ex_nodeCnt = sum([job['num_nodes'] for job in jobLst if job['shared']!='OK'])
  #  sh_nodeCnt = sum([job['num_nodes'] for job in jobLst if job['shared']=='OK'])
  #  cpuCnt     = sum([job['num_cpus']  for job in jobLst])
  #  return ex_nodeCnt, sh_nodeCnt, cpuCnt

  #get allocation of all jobs in partition
  #def getAccountAllocInPartition(self, partition, account):
  #  jobLst     = [ job for job in self.job_dict.values() if job['partition'] == partition 
  #                                                      and job['job_state']=='RUNNING'
  #                                                      and self.user_assoc_dict[MyTool.getUser(job['user_id'])]['Account'] == account]
  #  ex_nodeCnt = sum([job['num_nodes'] for job in jobLst if job['shared']!='OK'])
  #  sh_nodeCnt = sum([job['num_nodes'] for job in jobLst if job['shared']=='OK'])
  #  cpuCnt     = sum([job['num_cpus']  for job in jobLst])
  #  return ex_nodeCnt, sh_nodeCnt, cpuCnt

  def getMaxWallPJ (self, job):
      qos_set = set ()
      #partition QoS
      if job['partition']:
         qos_set.add(self.partition_dict[job['partition']].get('qos_char',''))
      #Job QoS
      qos_set.add(job['qos'])
      #User Association QoS

      qos, lmt= min([ (qos, self.qos_dict[qos].get('max_wall_pj', 4294967295)) for qos in qos_set], key=lambda x: x[1])
      return qos, lmt

  def getPartitionAndNodes (self, p_name):
      partition  = self.partition_dict[p_name]
      nodes      = [self.node_dict[nm] for nm in partition['nodes_flat']]

      #get rid of None and UNLIMITED in partition
      partition  = dict((key, value) for key, value in partition.items() if value and value not in ['UNLIMITED','1', 1, 'NONE']) 
      rlt        = []
      for n in nodes:
          #rlt.append(dict([(f, n[f]) for f in nodeFields if n.get(f, None)]))
          rlt.append(n)
          
      return partition, rlt

  #add gpus, alloc_gpus, running_jobs
  def extendNodeDict (self):
    #extend for gpu
    for node in self.node_dict.values():
        if 'gpu' in node['features']:
            node['gpus'], node['alloc_gpus']= MyTool.getGPUCount(node['gres'], node['gres_used'])
        else:
            node['gpus'], node['alloc_gpus']= 0,0
        node['running_jobs'] = []

    # add job allocate information
    run_jobs   = [(jid,job) for jid, job in self.job_dict.items() if job['job_state']=='RUNNING']
    for jid,job in run_jobs:
        for n in job['cpus_allocated'].keys():
            self.node_dict[n]['running_jobs'].append(jid)
    return self.node_dict
    
  #return TRUE if it is shared
  def getSharedFlag(self, partition):
    return partition['flags']['Shared'] == 'NO'

  # node['gres'] - node['gres_used'] >= gresReq) 
  def nodeWithGres (self, node, gresReq):
      gresTotal = MyTool.gresList2Dict(node['gres'])
      gresUsed  = MyTool.gresList2Dict(node['gres_used'])

      for gres,count in gresReq.items():
          if gres not in gresTotal:
             return False
          if (int(gresTotal[gres]) - int(gresUsed.get(gres,0))) < int(count):
             return False
      return True

  # return node['gres']
  def nodeAvailGres (self, node):
      return node_dict[node]['gres']

  #return totalGPU, usedGPU on node
  def getNodeGPUCount (self, pyslurm_node):
      if 'gpu' not in pyslurm_node['features']:
         return 0,0
      return MyTool.getGPUCount(pyslurm_node['gres'], pyslurm_node['gres_used'])

  # modify self.partition_dict by adding attributes to partition p_name, flag_shared, avail_nodes, avail_cpus, running_jobs, pending_jobs
  # return avail_nodes, avail_cpus with the constrain of features and min_mem_per_node
  def extendPartitionDict (self, p_name, p):
      if not p.get('flag_shared', None):  # not extend yet
         p['flag_shared'] = 'YES' if self.getSharedFlag(p) else 'NO'

         if p['nodes']:
            nodes             = MyTool.nl2flat(p['nodes'])
            avail_nodes       = [n for n in nodes if self.node_dict.get(n, {}).get('state', None) == 'IDLE']
            if p['flag_shared'] == 'YES':     # count MIXED node with idle CPUs as well
               avail_nodes   += [n for n in nodes if (self.node_dict.get(n, {}).get('state', None) == 'MIXED') and (self.node_dict.get(n,{}).get('cpus', 0) > self.node_dict.get(n, {}).get('alloc_cpus', 0))]
         else:
            nodes             = []
            avail_nodes       = []

         avail_cpus_cnt,lst1  = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
         p['nodes_flat']      = nodes
         p['avail_nodes_cnt'] = len(avail_nodes)
         p['avail_nodes']     = avail_nodes
         p['avail_cpus_cnt']  = avail_cpus_cnt
         p['avail_cpus']      = lst1

         # get running jobs on the parition
         part_jobs        = [j for j in self.job_dict.values() if j['partition'] == p_name]
         p['running_jobs']= [j['job_id'] for j in part_jobs if j['job_state']=='RUNNING']
         p['pending_jobs']= [j['job_id'] for j in part_jobs if j['job_state']=='PENDING']

         # get gpu on the parition
         p['total_gpus']  = MyTool.getTresDict(p['tres_fmt_str']).get('gres/gpu',0)
         if p['total_gpus']:
            #logger.debug ("extendPartition {} total_gpus={}".format(p, p["total_gpus"]))
            part_gpus        = [self.getNodeGPUCount(self.node_dict[node]) for node in avail_nodes]  #only gpu in avail_nodes can be used
            avail_total_gpu  = sum([cnt[0] for cnt in part_gpus])
            used_gpu_cnt     = sum([cnt[1] for cnt in part_gpus])
            logger.debug ("extendPartition avail_gpus={} used_gpus={}".format(avail_total_gpu, used_gpu_cnt))
            p['avail_gpus_cnt']  = avail_total_gpu - used_gpu_cnt
         else:
            p['avail_gpus_cnt']  = 0
      return p['avail_nodes']

  #gpu_per_node is job['tres_per_node'] in format: 'gpu:v100-32gb:1' or 'gpu:4'
  def nodeWithGPU (self, pyslurm_node, job_tres_per_node):
      lst = job_tres_per_node.split(',')
      if len(lst) > 1:
         print ('WARNING: tres_per_node of job have more than one value. TODO: change the code')

      lst = lst[0].split(':')
      if lst[0] != 'gpu':   
         print ('WARNING: tres_per_node of job change format and have new key.')
         return True
      if 'gpus' in pyslurm_node:  # no gpu on the node
         gpuAvail = pyslurm_node['gpus'] - pyslurm_node['alloc_gpus']
         if int(lst[-1]) <= gpuAvail:  # count is satisfied
            if len(lst)==2:
               return True
            #check feature lst[1] with pyslurm_node['gres'], pyslurm_node['gres_used']
            #TODO: assuming the same type of GPU on one node
            node_gpu_type = pyslurm_node['gres'][0].split(':')[1]
            if lst[1] == node_gpu_type:
               return True
      return False

  # get a node_list such that if job will run on node_list, it will not conflict with any job in job_list
  # return node and the conflict jobs in res_list
  def getConflictResNodes(self, job, candidate_nodes, res_list, node_field='node_list'):  #supporse job run from now to job_end_time
      cand_nodes    = set(candidate_nodes)
      conflict_res  = []  #reservation that has conflict
      if res_list:
         end_time = int(time.time()) + job['time_limit']*60  #time_limit is in minutes
         for res in res_list:
             if res['start_time'] < end_time and res[node_field]:   #conflict
                res_nodes = set(MyTool.nl2flat(res[node_field]))
                if not cand_nodes.isdisjoint (res_nodes):
                   cand_nodes = cand_nodes.difference(res_nodes)
                   conflict_res.append (res)
                #if not (conflict_nodes.issuperset(new_set)):
                #   conflict_nodes = conflict_nodes.union(new_set)
                #   conflict_res.append  (res)
      return list(cand_nodes), conflict_res

  #return a sublist rlt of input lst, 
  #the sum of the count number of items in rlt will be over min_sum
  def getNodesWithCPUs (self, node_lst, count, min_sum):
      if count > len(node_lst):
         return []
      sort_lst = [(name, self.node_dict[name]['cpus']-self.node_dict[name]['alloc_cpus']) for name in node_lst]
      sort_lst.sort(key=lambda i: i[1], reverse=True) #sorted by available cpus decreasingly
      val_lst  = [i[1] for i in sort_lst]
      top_sum  = sum(val_lst[0:count-1])    # sum of top count-1 items
      if top_sum+val_lst[count-1] < min_sum:  #cannot satisfy min_sum
         return []
      if (count==len(node_lst)) or (top_sum+val_lst[-1] >= min_sum):  #any subset can satisfy min_sum
         return node_lst
      idx      = count
      while (idx<len(sort_lst)) and (top_sum+val_lst[idx]>=min_sum): #top count-1 + curr item can satisfy min_sum
          idx      += 1
      return [i[0] for i in sort_lst[0:idx]]

  #check reservation and other constaints such as features
  #return nodes that can be used 
  #strictFlag=True means request number of nodes with min cpus instead of satisfy sum
  def getPartitionAvailNodeCPU (self, p_name, job, strictFlag=False, higherPending=[]):
      conflict_res = []
      # partition available
      avail_nodes         = self.partition_dict[p_name]['avail_nodes']
      print('---Job {} init avail_nodes={}'.format(job['job_id'], avail_nodes))

      # exclude reserved nodes
      if avail_nodes and self.res_future:
         avail_nodes, conflict_res = self.getConflictResNodes(job, avail_nodes, self.res_future, node_field='node_list')
         #avail_nodes          = [node for node in avail_nodes if node not in res_nodes]
         print('---reservation avail_nodes={}'.format(avail_nodes))

      # avail_nodes - reserved_nodes_for_higherPending
      if avail_nodes and higherPending:
         avail_nodes, conflict_res = self.getConflictResNodes(job, avail_nodes, higherPending, node_field='sched_nodes')
         #avail_nodes          = [node for node in avail_nodes if node not in res_nodes]
         print('---higher_pending, avail_nodes={}'.format(avail_nodes))
         
      # exclude nodes by job request
      if avail_nodes and job.get('exc_nodes',[]):
         exc_nodes        = []
         for item in job['exc_nodes']:
             exc_nodes.extend (MyTool.nl2flat(item))
         avail_nodes      = list(set(avail_nodes).difference(set(exc_nodes)))
         print('---exc_nodes avail_nodes={}'.format(avail_nodes))

      # restrain nodes with required features
      features            = job.get('features',[])
      if avail_nodes and features:        
         avail_nodes      = [n for n in avail_nodes if set(self.node_dict[n]['features_active'].split(',')).intersection(features)]
         print('---feature ({}), avail_nodes={}'.format(features, avail_nodes))

      # restrain nodes if job cannot share 
      if avail_nodes and job['shared']=='0':
         avail_nodes      = [n for n in avail_nodes if self.node_dict[n]['state']=='IDLE']
         features.append ('exclusive')
         print('---shared avail_nodes={}'.format(avail_nodes))

      # restrain nodes with gpu 
      gpu_per_node        = job['tres_per_node']    #'gpu:v100-32gb:01' or 'gpu:4'
      if avail_nodes and gpu_per_node:
         avail_nodes      = [n for n in avail_nodes if self.nodeWithGPU (self.node_dict[n], gpu_per_node)]  #'gpu:v100-32gb:01' or 'gpu:4'
         features.append (gpu_per_node)
         print('---gpu avail_nodes={}'.format(avail_nodes))

      # restrain nodes with cpu      
      if avail_nodes and job['ntasks_per_node']:
         avail_nodes      = [n for n in avail_nodes if (self.node_dict[n]['cpus']-self.node_dict[n]['alloc_cpus'])>=job['ntasks_per_node']]
         features.append ('{} tasks'.format(job['ntasks_per_node']))
         print('---ntask_per_node ({}), avail_nodes={}'.format(job['ntasks_per_node'], avail_nodes))

      # restrain nodes with min memory 
      if avail_nodes and job['mem_per_node'] and job['pn_min_memory']:  
         avail_nodes      = [n for n in avail_nodes if (self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem']) >= job['pn_min_memory']]
         features.append ('{}MB mem_per_node'.format(job['pn_min_memory']))
         print('---mem_per_node ({}), avail_nodes={}'.format(job['pn_min_memory'], avail_nodes))

      # restrain nodes with cpu level
      # sum of cpu
      if job['num_nodes']==1:
         avail_nodes      = [n for n in avail_nodes if (self.node_dict[n]['cpus']-self.node_dict[n]['alloc_cpus'])>=job['num_cpus']]
      else:
         if strictFlag:
            avail_nodes      = self.getNodesWithCPUs(avail_nodes, job['num_nodes'], job['num_cpus'])
         else:
            max_cpu_per_node = self.part_node_cpu[p_name][0]
            min_cpu_per_node = job['num_cpus'] - (job['num_nodes']-1) * max_cpu_per_node
            if min_cpu_per_node > 0:
               avail_nodes   = [n for n in avail_nodes if (self.node_dict[n]['cpus']-self.node_dict[n]['alloc_cpus'])>=min_cpu_per_node]
      print('---sum_of_cpu avail_nodes={}'.format(avail_nodes))

      avail_cpus_cnt,lst1 = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
      if avail_nodes and job['mem_per_cpu'] and job['min_memory_cpu']:
         avail_cpus       = dict(zip(avail_nodes, lst1))   
         if job['ntasks_per_node']:
            avail_nodes   = [n             for n in avail_nodes if (job['ntasks_per_node']*job['min_memory_cpu'])<=(self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem'])]
         else:
            avail_nodes   = [n             for n in avail_nodes if (avail_cpus[n]*job['min_memory_cpu'])<=(self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem'])]
         lst1             = [avail_cpus[n] for n in avail_nodes]
         avail_cpus_cnt   = sum(lst1)
         features.append ('{}MB mem_per_cpu'.format (job['min_memory_cpu']))
         print('---mem_per_cpu avail_nodes={}'.format(avail_nodes))

      return len(avail_nodes), avail_cpus_cnt, avail_nodes, features, conflict_res
      
  # return list of partitions with fields, add attributes to partition_dict
  def getPartitions(self):
    return [self.partition_dict[name] for name in sorted(self.partition_dict.keys())]

  #TODO: consider OverPartQOS
  #return nodeLimit, cpuLimit, gresLimit
  def getJobQoSTresLimit (self, job, p_name, tres_attribute, OverPartQOS=False):
      partition                       = self.partition_dict[p_name]
      tres_str                        = self.qos_dict[partition['qos_char']][tres_attribute]
      node_lmt,   cpu_lmt,   gpu_lmt  = None, None, None
      j_node_lmt, j_cpu_lmt, j_gpu_lmt= MAX_LIMIT,MAX_LIMIT,MAX_LIMIT
      if tres_str:  #partitio QoS
         node_lmt, cpu_lmt, gpu_lmt = SlurmEntities.getQoSTresLimit (tres_str, defaultValue=0)
      if not (node_lmt and cpu_lmt and gpu_lmt): #partition QoS override Job QoS
         tres_str    = self.qos_dict[job['qos']][tres_attribute]
         if tres_str:
            j_node_lmt, j_cpu_lmt, j_gpu_lmt = SlurmEntities.getQoSTresLimit (tres_str, defaultValue=MAX_LIMIT)
         node_lmt = node_lmt if node_lmt else j_node_lmt
         cpu_lmt  = cpu_lmt  if cpu_lmt  else j_cpu_lmt
         gpu_lmt  = gpu_lmt  if gpu_lmt  else j_gpu_lmt
         
      return node_lmt, cpu_lmt, gpu_lmt

  # return nodeLimit, cpuLimit, gresLimit
  def getPartQoSTresLimit (self, partition, tres_attribute):
      tres_str =  self.qos_dict[self.partition_dict[partition]['qos_char']][tres_attribute]
      return SlurmEntities.getQoSTresLimit (tres_str)

  #return jobs in job_list that has a smaller id than job_id
  def getSmallerJobIDs (self, job_id, job_list):
      return [jid for jid in job_list if jid < job_id]

  #return list of current pending jobs sorted by jid, no explaination
  def getCurrentPendingJobs (self, fields=['job_id', 'submit_time', 'user_id', 'account', 'qos', 'partition', 'state_reason', 'tres_per_node']):
      logging.debug ("getCurrentPendingJobs")
      job_dict = pyslurm.job().get()
      pending  = [MyTool.sub_dict(job, fields) for jid,job in job_dict.items() if job['job_state']=='PENDING']

      return datetime.now().timestamp(), pending

  #return list of jobs sorted by jid and with state_exp filled
  def getPendingJobs (self):
      pending   = [job for jid,job in sorted(self.job_dict.items()) if job['job_state']=='PENDING']
      res_nodes = []
      higherJobs= []
      for job in pending:
          p_name_lst = job['partition'].split(',')
          for p_name in p_name_lst:
              exp              = self.explainPendingJob (job, p_name, higherJobs, res_nodes)
              job['state_exp'] = exp if 'state_exp' not in job else '{}\n{}'.format(job['state_exp'],exp)
          if job['sched_nodes']:
             higherJobs.append (job)
             res_nodes.extend  (MyTool.nl2flat(job['sched_nodes']))
      return pending

  def explainPendingJob(self, job, p_name, higherJobs, reserved_nodes):
      job['user']    = MyTool.getUser(job['user_id'])  # will be used in html
      if '_' in job['state_reason']:
         # mod in pyslurm not working anymore, workaround 02/05/2021
         job['state_reason_desc'] = job['state_reason']
         job['state_reason']      = 'Resources'
      state_exp      = PEND_EXP.get(job['state_reason'], '')

      if job['state_reason'] == 'QOSMaxWallDurationPerJobLimit':
         qos, lmt       = self.getMaxWallPJ(job)
         state_exp      = state_exp.format(job_time_limit=job.get('time_limit_str'), qos=qos, qos_limit=lmt)
      elif job['state_reason'] == 'QOSMinGRES':
         state_exp      = 'A job need to request at least 1 GPU in partition {}'.format(p_name)     
      elif 'PerUser' in job['state_reason']:
         u_node_qos, u_cpu_qos, u_gpu_qos = self.getJobQoSTresLimit (job, p_name, 'max_tres_pu')
         u_node,     u_cpu,     u_gpu     = SlurmEntities.getUserAllocInPartition(job['user_id'], p_name, self.job_dict)
         if job['state_reason'] == 'QOSMaxCpuPerUserLimit':
            state_exp      = state_exp.format(user=job['user'], max_cpu_user=u_cpu_qos, curr_cpu_user=u_cpu, partition=p_name)
            u_cpu_avail    = u_cpu_qos - u_cpu
            if job['shared']!= 'OK' and (u_cpu_avail >0) and (MyTool.getTresDict(job['tres_req_str'])['cpu']) <= u_cpu_avail:
                   state_exp   = '{} {}'.format(state_exp, 'Job request exclusive nodes and may allocate more CPUs than requested.')
         elif job['state_reason'] == 'QOSMaxGRESPerUser':
            state_exp      = state_exp.format(user=job['user'], max_gpu_user=u_gpu_qos, curr_gpu_user=u_gpu, partition=p_name)
         elif job['state_reason'] == 'QOSMaxNodePerUserLimit':
            state_exp      = state_exp.format(user=job['user'], max_node_user=u_node_qos, curr_node_user=u_node, partition=p_name)
      elif job['state_reason'] == 'QOSGrpNodeLimit':
         a_node_qos, a_cpu_qos, etc    = self.getJobQoSTresLimit (job, p_name, 'grp_tres')
         j_account                     = self.user_assoc_dict[MyTool.getUser(job['user_id'])]['Account']
         a_node,     a_cpu,     etc    = SlurmEntities.getAllAllocInPartition(p_name, self.job_dict)
         #a_ex_node,a_sh_node,a_cpu     = self.getAccountAllocInPartition(p_name, j_account)  #Grp limit seems to address all accounts
         state_exp                     = state_exp.format(max_node_grp=a_node_qos, curr_node_grp=a_node, partition=p_name)
      elif job['state_reason'] == 'QOSGrpCpuLimit':
         a_node_qos, a_cpu_qos, etc    = self.getJobQoSTresLimit (job, p_name, 'grp_tres')
         j_account                     = self.user_assoc_dict[MyTool.getUser(job['user_id'])]['Account']
         a_node,     a_cpu,     etc    = SlurmEntities.getAllAllocInPartition(p_name, self.job_dict)
         #a_ex_node,a_sh_node,a_cpu     = self.getAccountAllocInPartition(p_name, j_account)
         state_exp                     = state_exp.format(max_cpu_grp=a_cpu_qos, curr_cpu_grp=a_cpu, partition=p_name)
      elif job['state_reason'] == 'Resources':
         pa_node, pa_cpu, pa_node_lst, features, conflict_res = self.getPartitionAvailNodeCPU (p_name, job, higherPending=higherJobs)
         pa_node_str                                          = '{}'.format(pa_node)
         if pa_node:
            pa_node_str = '{} ({})'.format(pa_node,['<a href=./nodeDetails?node={node_name}>{node_name}</a>'.format(node_name=node) for node in sorted(pa_node_lst)])
         state_exp      = state_exp.format(partition=p_name, avail_node=pa_node_str, avail_cpu=pa_cpu, feature='({0})'.format(features))
         if job.get('state_reason_desc',None): #modify pyslurm to add state_reason_desc 02/27/2020
            #pa_node_lst = list( set(pa_node_lst) - set(reserved_nodes) )
            state_exp   = '{} ({}). {} '.format(job['state_reason_desc'].replace('_',' '), [res['job_id'] for res in conflict_res], state_exp)
      elif job['state_reason'] == 'Priority':
         earlierJobs    = self.getSmallerJobIDs(job['job_id'], self.partition_dict[p_name].get('pending_jobs',[]))
         state_exp      = state_exp.format(partition=p_name, higher_job=earlierJobs)
      elif job['state_reason'] == 'JobArrayTaskLimit':
         array_tasks_lst = [j.get('array_task_id','') for j in self.job_dict.values() if j.get('array_job_id',-1) == job['job_id'] and j.get('job_state','') == 'RUNNING'] 
         array_tasks_lst = [t for t in array_tasks_lst if t]
         state_exp       = state_exp.format(array_task_str=job.get('array_task_str', ''), array_max_tasks=job.get('array_max_tasks', ''), array_tasks=array_tasks_lst)
      elif job['state_reason'] == 'Dependency':
         state_exp      = state_exp.format(dependency=job.get('dependency'))
      elif job['state_reason'] == 'QOSMaxNodePerJobLimit':
         pa_qos         = self.partition_dict[job['partition']]['qos_char']
         state_exp      = state_exp.format(max_tres_pj=MyTool.getTresDict(self.qos_dict[pa_qos]['max_tres_pj'],mapKey=True))
          
      # information added to state_exp for all reasons
      if job['sched_nodes']:
         state_exp      += ' Job is scheduled on {}'.format(job['sched_nodes'])
         if job['start_time']:
            state_exp   += ' starting from {}'.format(MyTool.getTsString(job['start_time']))
         running    = [job for jid,job in self.job_dict.items() if job['job_state']=='RUNNING']
         schedNode  = set([nm for nm in MyTool.nl2flat (job['sched_nodes']) if self.node_dict[nm]['state']!='IDLE'])
         waitForJob = ['<a href="./jobDetails?jid={jid}">{jid}</a>'.format(jid=job['job_id']) for job in running if schedNode.intersection(set(job['cpus_allocated']))]
         if waitForJob:
            state_exp      += ', waiting for running jobs {}.'.format(waitForJob)
         else:
            state_exp      += '.'
      elif job['start_time']:
         state_exp      += ' Job will start no later than {}.'.format(MyTool.getTsString(job['start_time']))
 
      return state_exp

  @staticmethod
  def findNodeInState(node_dict, state):
    nodes   = [nid for nid,node in node_dict.items() if node['state']==state]

    return nodes

  @staticmethod
  def getIdleCores (node_dict, node_list):
    nodes   = [node for nid,node in node_dict.items() if nid in node_list]
    ic_list = [node['cpus']-node['alloc_cpus'] for node in nodes]
    total   = sum(ic_list)

    return total, ic_list

  def getNodes(self):
    try:
        nodes     = pyslurm.node()

        if len(self.node_dict) > 0:
            idle    = SlurmEntitites.findNodeInState(self.node_dict, 'IDLE')
            mixed   = SlurmEntitites.findNodeInState(self.node_dict, 'MIXED')
            #print("Nodes in IDLE state - {0}".format(idle))
            #print("Nodes in MIXED state - {0}".format(mixed))
            #print()

            total1,lst1 = SlurmEntities.getIdleCores (self.node_dict, idle)
            total2,lst2 = SlurmEntities.getIdleCores (self.node_dict, mixed)
            print("Total {0} nodes: {1} IDLE nodes ({2} cpus) and {3} MIXED nodes ({4} cpus)".format(len(self.node_dict), len(idle), total1, len(mixed), total2))
        else:
            print("No Nodes found !")

    except ValueError as e:
        print("Error - {0}".format(e.args[0]))

  # return jobs of a user {'RUNNING':[], 'otherstate':[]}
  def getUserJobsByState (self, uid):
      result    = defaultdict(list)  #{state:[job...]}
      jobs      = [job for job in self.job_dict.values() if job['user_id']==uid]
      for job in jobs:
          result[job['job_state']].append (job)
      return result

  def getAllAllocInPartition ( pname, job_dict ):
      jobLst    = [ job for job in job_dict.values() if job['partition'] == pname and job['job_state']=='RUNNING' ]
      return SlurmEntities.getJobAlloc(jobLst, True)

  # get user+partion
  def getUserAllocInPartition( uid, pname, job_dict):
      jobLst     = [ job for job in job_dict.values() if job['job_state']=='RUNNING' and job['user_id'] == uid and job['partition'] == pname ]
      return SlurmEntities.getJobAlloc(jobLst, True)

  def getJobAlloc( jobLst, gpu_flag=False):
    tres     = [MyTool.getTresDict(job['tres_alloc_str']) for job in jobLst]
    cpu_cnt  = sum([t.get('cpu',0) for t in tres])
    node_cnt = sum([t.get('node',0) for t in tres])
    if gpu_flag:
       gpu_cnt = sum([t.get('gres/gpu',0) for t in tres])
       return node_cnt, cpu_cnt, gpu_cnt
    else:
       return node_cnt, cpu_cnt, 0

  #check access control 'allow_groups', 'allow_accounts'
  #check qos 
  def getAccountPartition (self, account, uid):
    result    = []
    for pname, part in self.partition_dict.items():
        if (part.get('allow_groups', []) == 'ALL') or set(part.get('allow_groups',[])).intersection(account):
           if (part.get('allow_accounts', []) == 'ALL') or (account in part.get('allow_accounts',[])): #deny_accounts
              # user is allowed to access the partition
              # partition and user's current usage
              partJobs = [ job for job in self.job_dict.values() if job['job_state']=='RUNNING' and job['partition'] == pname ]
              grpJobs  = [ job for job in partJobs               if job['account'] == account]
              userJobs = [ job for job in partJobs               if job['user_id'] == uid]
              gpuFlag  = True if part['total_gpus'] else False
              gNodeCnt, gCpuCnt, gGpuCnt = SlurmEntities.getJobAlloc  (grpJobs,  gpuFlag)
              uNodeCnt, uCpuCnt, uGpuCnt = SlurmEntities.getJobAlloc  (userJobs, gpuFlag)

              # qos limit & partitin lmt
              qos_char = part['qos_char']
              if qos_char:
                 qos = self.qos_dict[qos_char]
                 gNodeLmt, gCpuLmt, gGpuLmt = SlurmEntities.getQoSTresLimit(qos.get('grp_tres',    '')) 
                 uNodeLmt, uCpuLmt, uGpuLmt = SlurmEntities.getQoSTresLimit(qos.get('max_tres_pu', ''))
              else:
                 #gNodeLmt, gCpuLmt, gGpuLmt = part['total_nodes'], part['total_cpus'], part['total_gpus']
                 #uNodeLmt, uCpuLmt, uGpuLmt = part['total_nodes'], part['total_cpus'], part['total_gpus']
                 gNodeLmt, gCpuLmt, gGpuLmt = MAX_LIMIT,MAX_LIMIT,MAX_LIMIT
                 uNodeLmt, uCpuLmt, uGpuLmt = MAX_LIMIT,MAX_LIMIT,MAX_LIMIT

              # user avail
              availNodeCnt   = min(part['avail_nodes_cnt'],  gNodeLmt-gNodeCnt, uNodeLmt-uNodeCnt)
              availCpuCnt    = min(part['avail_cpus_cnt'],   gCpuLmt-gCpuCnt,   uCpuLmt-uCpuCnt)
              availGpuCnt    = 0
              if part['total_gpus']:
                 availGpuCnt = min(part['avail_gpus_cnt'],   gGpuLmt-gGpuCnt,   uGpuLmt-uGpuCnt)
              if availCpuCnt < part['avail_cpus_cnt']:
                 if part['flag_shared'] == 'NO':
                    #if not shared partition, extra constraint on availNodeCnt imposed by availCpuCnt
                    avail_cpus_inc = part['avail_cpus'].copy()
                    avail_cpus_inc.sort()
                    count=0
                    for i in range(len(avail_cpus_inc)):  # range starts at 0, stops at end-1
                        count += avail_cpus_inc[i]
                        if count > availCpuCnt:           
                           availNodeCnt = i               # biggest node possible
                           #availCpuCnt  = count - avail_cpus_inc[i]
                           break
                    availCpuCnt=min(availCpuCnt, sum(avail_cpus_inc[-availNodeCnt:]))  #biggest cpu possible
                 else:
                    availNodeCnt = min(availNodeCnt, availCpuCnt)
              elif availNodeCnt < part['avail_nodes_cnt']:  #availCpuCnt == part['avail_cpus']
                    #if not shared partition, extra constraint on availCpuCnt imposed by availNodeCnt
                    avail_cpus_dec = part['avail_cpus'].copy()
                    avail_cpus_dec.sort(reverse=True)
                    count          = sum(avail_cpus_dec[0:availNodeCnt])
                    availCpuCnt    = max(count, availCpuCnt)
            
              result.append({'name':pname, 'flag_shared':part['flag_shared'], 
                             'total_nodes':part['total_nodes'], 'total_cpus':part['total_cpus'], 'total_gpus':part['total_gpus'],
                             'avail_nodes_cnt':part['avail_nodes_cnt'], 'avail_cpus_cnt':part['avail_cpus_cnt'], 'avail_gpus_cnt':part['avail_gpus_cnt'],
                             'user_alloc_nodes': uNodeCnt,  'user_alloc_cpus': uCpuCnt, 'user_alloc_gpus': uGpuCnt,
                             'grp_alloc_nodes': gNodeCnt,   'grp_alloc_cpus': gCpuCnt,  'grp_alloc_gpus':  gGpuCnt,
                             'user_lmt_nodes': uNodeLmt,    'user_lmt_cpus': uCpuLmt, 'user_lmt_gpus': uGpuLmt,
                             'grp_lmt_nodes': gNodeLmt,     'grp_lmt_cpus': gCpuLmt, 'grp_lmt_gpus': gGpuLmt,
                             'user_avail_nodes':availNodeCnt, 'user_avail_cpus':availCpuCnt, 'user_avail_gpus':availGpuCnt})

    return result

  
  def getAvailCPURange (self, node_lst, num_node, min_lmt):
    # sort the node_lst on avail cpus
    # node_lst is already sorted decreasing in getNodesWithCPUs
    sort_lst = [(name, self.node_dict[name]['cpus']-self.node_dict[name]['alloc_cpus']) for name in node_lst]
    sort_lst.sort(key=lambda i: i[1], reverse=True) #sorted by available cpus decreasingly
    val_lst  = [i[1] for i in sort_lst]
    max_cpus = sum(val_lst[:num_node])    
    min_cpus = sum(val_lst[-num_node:])  
    #min_cpus = max(min_lmt, sum([ self.node_dict[node]['cpus'] - self.node_dict[node]['alloc_cpus'] for node in node_lst[-num_node:]]))
    #max_cpus = sum([ self.node_dict[node]['cpus'] - self.node_dict[node]['alloc_cpus'] for node in node_lst[:num_node]])
    return min_cpus, max_cpus
    
  # can the job be scheduled if QoS is relaxed
  # assume that job is the first in the queue for its partition
  # return a suggestion
  def relaxQoS(self):
    #loop over all the pending jobs sorted by priority TODO: currently just jid will do
    pendingJids  = sorted([jid for jid,job in self.job_dict.items() if job['job_state']=='PENDING'])
    higherJobs   = []
    rlt          = {}
    for jid in pendingJids:
        #print('---relaxQoS Job {}'.format(jid))
        job = self.job_dict[jid]
        uid = job['user'] if 'user' in job else job['user_id']
        if 'QOS' in job['state_reason'] and uid not in rlt.keys():  # if pending for QOS reason
           p_name = job['partition'].split(',')[0]
           if self.partition_dict[p_name]['avail_cpus_cnt']:
              if self.partHasJobResource(job, p_name):
                 suggestion   = ''
                 #take action, sacctmgr or send email
                 u_node_qos,  u_cpu_qos,  u_gpu_qos   = self.getJobQoSTresLimit (job, p_name, 'max_tres_pu')  #qos limit for user
                 g_node_qos,  g_cpu_qos,  g_gpu_qos   = self.getJobQoSTresLimit (job, p_name, 'grp_tres')     #qos limit for grp
                 u_node_alloc,u_cpu_alloc,u_gpu_alloc = SlurmEntities.getUserAllocInPartition(job['user_id'], p_name, self.job_dict)  #user allocation
                 g_node_alloc,g_cpu_alloc,g_gpu_alloc = SlurmEntities.getAllAllocInPartition(p_name, self.job_dict)  #all allocation in part, seems the where the grp limit applied
                 j_gpu                                = MyTool.getTresDict(job['tres_req_str']).get('gres/gpu',0)
                 if job['shared']=='OK' and self.partition_dict[p_name]['flag_shared'] == 'YES':
                    j_min_cpus, j_max_cpus            = job['num_cpus'], job['num_cpus']
                 else:
                    j_min_cpus, j_max_cpus            = self.getAvailCPURange (job['qos_relax_nodes'], job['num_nodes'],job['num_cpus'])
                 nu_NodeAlloc,nu_CPUAlloc_min,nu_CPUAlloc_max,nu_GPUAlloc = u_node_alloc+job['num_nodes'],u_cpu_alloc+j_min_cpus,u_cpu_alloc+j_max_cpus,u_gpu_alloc+j_gpu
                 ng_NodeAlloc,ng_CPUAlloc_min,ng_CPUAlloc_max,ng_GPUAlloc = g_node_alloc+job['num_nodes'],g_cpu_alloc+j_min_cpus,g_cpu_alloc+j_max_cpus,g_gpu_alloc+j_gpu
                 if nu_NodeAlloc > u_node_qos:
                    print('---Increase User Node Limit to {}'.format (nu_NodeAlloc))
                    suggestion += 'Increase User Node Limit to {}. '.format (nu_NodeAlloc)
                 if nu_CPUAlloc_min  > u_cpu_qos:
                    print('---Increase User CPU Limit to {}'.format  (nu_CPUAlloc_min))
                    if nu_CPUAlloc_min == nu_CPUAlloc_max:
                       suggestion += 'Increase User CPU Limit to {}. '.format (nu_CPUAlloc_min)
                    else:
                       suggestion += 'Increase User CPU Limit to [{},{}]. '.format (nu_CPUAlloc_min,nu_CPUAlloc_max)
                 if ng_NodeAlloc > g_node_qos:
                    print('---Increase Group Node Limit to {}'.format(ng_NodeAlloc))
                    suggestion += 'Increase Group Node Limit to {}. '.format (ng_NodeAlloc)
                 if ng_CPUAlloc_min  > g_cpu_qos:
                    print('---Increase Group CPU Limit to {}'.format (ng_CPUAlloc_min))
                    if ng_CPUAlloc_min == ng_CPUAlloc_max:
                       suggestion += 'Increase Group CPU Limit to {}. '.format (ng_CPUAlloc_min)
                    else:
                       suggestion += 'Increase Group CPU Limit to [{},{}]. '.format (ng_CPUAlloc_min, ng_CPUAlloc_max)
                 delayJobs = self.mayDelayPending(job, higherJobs)
                 if delayJobs:
                    suggestion += 'QoS relax of the job may delay highe priority pending jobs {}'.format([job['job_id'] for job in delayJobs])
                 print('---May delay jobs {} in {}'.format([job['job_id'] for job in delayJobs], [job['job_id'] for job in higherJobs]))
                 if suggestion:
                    suggestion = 'To run job {} in partition {} with candidate nodes {}, the following relaxation of QoS {} will be recommended. '.format(jid,p_name, job['qos_relax_nodes'], self.partition_dict[p_name]['qos_char']) + suggestion
                    rlt[uid]= suggestion
 
        if job['start_time']:   # see if will influce other jobs
           higherJobs.append(job)
    #print ('---Jobs with start_time={}'.format([(job['job_id'],job['state_reason']) for job in higherJobs]))
    return rlt

  # if job's running will influce other pending jobs's start time
  def mayDelayPending (self, job, pending):
      j_end_time   = int(time.time()) + job['time_limit']*60  #time_limit is in minutes
      j_cand_nodes = job['qos_relax_nodes']                   #candidate nodes, will use job['num_nodes'] only 
      return [job for job in pending if job['start_time'] < j_end_time 
                                    and set(MyTool.nl2flat(job.get('sched_nodes',''))).intersection(j_cand_nodes)]
     
  # if the requested part has enough resouces to run the job
  def partHasJobResource (self, job, p_name):
      p_node_avail,p_cpu_avail,p_node_avail_lst,features,conflict_res= self.getPartitionAvailNodeCPU (p_name, job, strictFlag=True) #some subset of p_node_avail can satisfy job requirement
      #enough nodes
      if p_node_avail==0:
         return False
      job['qos_relax_nodes']=p_node_avail_lst
      print("partHasJobResource {} {} {} {}".format(p_node_avail, p_cpu_avail, p_node_avail_lst, features))
      return True

  def test1():
      ins = SlurmEntities()
      rlt = ins.getNodesWithCPUs(['worker0044', 'worker3001', 'worker4007','workergpu01'], 3, 30)
      print (rlt)
  def test2():
      ins = SlurmEntities()
      rlt = ins.relaxQoS()
      print (rlt)

if __name__ == "__main__":
    SlurmEntities.test2()
    #ins = SlurmEntities()
    #ins.getPendingJobs()
    #ins.getUserPartition('agabrielpillai')
    #ins.getPartitions()
