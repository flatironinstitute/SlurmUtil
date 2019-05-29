#!/usr/bin/env python

from __future__ import print_function

import pyslurm
import sys

import MyTool
import logging

from time import gmtime, strftime, sleep
from datetime import datetime

PEND_EXP={
    'QOSMaxCpuPerUserLimit': 'QoS User CPU  limit ({max_cpu_user})  exceeded. User {user} already alloc {curr_cpu_user}  CPUs in {partition}.', # QOS MaxTRESPerUser exceeded (CPU) 
    'QOSMaxNodePerUserLimit':'QoS User Node limit ({max_node_user}) exceeded. User {user} already alloc {curr_node_user} Nodes in {partition}',	                         # QOS MaxTRESPerUser exceeded (Node)
    'QOSGrpNodeLimit':       'QoS Group Node limit ({max_node_grp}) exceeded. Group already alloc {curr_node_grp} Nodes in {partition}.', # QOS GrpTRES exceeded (Node)
    'QOSGrpCpuLimit':        'QoS Group CPU limit  ({max_cpu_grp})  exceeded. Group already alloc {curr_cpu_grp}  Nodes in {partition}.',
    'QOSMaxWallDurationPerJobLimit': 'Job time {job_time_limit} exceed QoS {qos}\'s MaxWallDurationPerJob limit ({qos_limit}).',
    'Dependency':            'Dependent jobs ({dependency}) have not completed', #/* dependent job has not completed */
    'Priority':              'Higher priority jobs exist. Partition {partition} queue higher priority jobs {higher_job}.', #/* higher priority jobs exist */
    'Resources':             'Required resources not available. Partition {partition} have {avail_node} requested {feature} nodes and {avail_cpu} CPUs. Resources will be availble no later than {start_time}.', #required resources not available
    'JobArrayTaskLimit':     'Job array ({array_task_str}) reach max task limit {array_max_tasks}. Tasks {array_tasks} are running.',
}
class SlurmEntities:
  #two's complement -1
  TCMO = 2**32 - 1

  def __init__ (self):
    self.config_dict    = pyslurm.config().get()
    self.qos_dict       = pyslurm.qos().get()
    self.partition_dict = pyslurm.partition().get()
    self.node_dict      = pyslurm.node().get()
    self.job_dict       = pyslurm.job().get()
    self.update_time    = datetime.now()

  #return nodeLimit, cpuLimit (max value TCMO)
  @staticmethod
  def getTresLimit (tres_str):
    d = {}
    if tres_str:
      for tres in tres_str.split(','):
        t, value = tres.split('=')
        d[t]     = value
      
    return d.get('4', SlurmEntities.TCMO), d.get('1', SlurmEntities.TCMO)

  #get allocation of all jobs in partition
  def getAllocInPartition(partition, job_dict):
    jobLst  = [ job for job in job_dict.values() if job['partition'] == partition and job['job_state']=='RUNNING']
    nodeCnt = sum([job['num_nodes'] for job in jobLst])
    cpuCnt  = sum([job['num_cpus']  for job in jobLst])
    return nodeCnt, cpuCnt

  # get user+partion
  def getUserAllocInPartition( uid, partition, job_dict):
    jobLst  = [ jid for jid, job in job_dict.items() if job['job_state']=='RUNNING' and job['user_id'] == uid and job['partition'] == partition ]
    nodeCnt = sum([job_dict[jid]['num_nodes'] for jid in jobLst])
    cpuCnt  = sum([job_dict[jid]['num_cpus']  for jid in jobLst])
    return nodeCnt, cpuCnt

  #check access control 'allow_groups', 'allow_accounts'
  #check qos 
  @staticmethod
  def getUserPartitions(uid, user, account, partition_dict, qos_dict, job_dict):
    linux_grp = set(MyTool.getUserGroups(user))
    lst1      = []
    lst2      = []
    for name,p in partition_dict.items():
        if (p['allow_groups'] == 'ALL') or (not not set(p['allow_groups']).intersection(linux_grp)):
           if (p['allow_accounts'] == 'ALL') or (not p['deny_accounts']) or (not account in p['deny_accounts']):
              gnCnt, gcCnt = SlurmEntities.getAllocInPartition(name, job_dict)
              unCnt, ucCnt = SlurmEntities.getUserAllocInPartition(uid,     name, job_dict)
              lst1.append(name + ' ' + str(gnCnt) + ',' + str(gcCnt) + ',' + str(unCnt) + ',' + str(ucCnt))

              qos = qos_dict[p['qos_char']]
              gnLmt, gcLmt = SlurmEntities.getTresLimit(qos.get('grp_tres',    '')) 
              unLmt, ucLmt = SlurmEntities.getTresLimit(qos.get('max_tres_pu', ''))
              lst2.append(p['qos_char']+' ' + str(gnLmt) + ','+ str(gcLmt)+',' + str(unLmt) + ',' + str(ucLmt))

    return lst1,lst2

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

  def getPartitionInfo (self, p_name, fields=['name', 'state', 'tres_fmt_str', 'features', 'gres', 'alloc_cpus','alloc_mem', 'cpu_load', 'gres_used', 'run_jobs']):
      partition  = self.partition_dict[p_name]
      node_names = MyTool.nl2flat(partition['nodes'])
      nodes      = self.addUpdateNodeInfo (node_names)

      #get rid of None and UNLIMITED in partition
      partition  = dict((key, value) for key, value in partition.items() if value and value not in ['UNLIMITED','1', 1, 'NONE'] 
                                                                            and key not in ['default_time', 'max_time', 'total_cpus', 'total_nodes', 'name', 'state'])
      partition['flags'] = repr(partition['flags'])
      rlt        = []
      for n in nodes:
          rlt.append(dict([(f, n[f]) for f in fields if n.get(f, None)]))
          
      return partition, rlt

  # return node allocate information of the partition
  def addUpdateNodeInfo (self, node_names):
    # should get newest data?
    job_dict   = pyslurm.job().get()
    node_dict  = pyslurm.node().get()

    run_jobs   = [(jid,job) for jid, job in job_dict.items() if job['job_state']=='RUNNING']
    
    # add job allocate information
    for jid,job in run_jobs:
        for n in MyTool.nl2flat(job['nodes']):
            if 'run_jobs' not in node_dict[n]: node_dict[n]['run_jobs'] = []
            node_dict[n]['run_jobs'].append(jid)
        
    nodes = [node_dict[nm] for nm in node_names]
    return nodes

  #return TRUE if it is shared
  def getSharedFlag(self, partition):
    return partition['flags']['Shared'] == 'NO'

  # node['gres'] - node['gres_used'] >= gresReq) 
  def nodeHaveGres (self, node, gresReq):
      gresTotal = MyTool.gresList2Dict(node['gres'])
      gresUsed  = MyTool.gresList2Dict(node['gres_used'])

      for gres,count in gresReq.items():
          if gres not in gresTotal:
             return False
          if (int(gresTotal[gres]) - int(gresUsed.get(gres,0))) < int(count):
             return False
      return True

  # modify self.partition_dict by adding attributes to partition p_name, flag_shared, avail_nodes, avail_cpus, running_jobs, pending_jobs
  # return avail_nodes, avail_cpus with the constrain of features and min_mem_per_node
  def addUpdatePartitionInfo (self, p_name, features=[], min_mem_per_node=0, gres=[]):
      p = self.partition_dict[p_name]

      if not p.get('flag_shared', None):
         p['flag_shared'] = 'YES' if self.getSharedFlag(p) else 'NO'

         if p['nodes']:
            nodes             = MyTool.nl2flat(p['nodes'])
            avail_nodes       = [n for n in nodes if self.node_dict.get(n, {}).get('state', None) == 'IDLE']
            if p['flag_shared'] == 'YES':     # count MIXED node with idle CPUs as well
               avail_nodes  += [n for n in nodes if (self.node_dict.get(n, {}).get('state', None) == 'MIXED') and (self.node_dict.get(n,{}).get('cpus', 0) > self.node_dict.get(n, {}).get('alloc_cpus', 0))]
         else:
            avail_nodes       = []

         avail_cpus_cnt,lst1  = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
         p['avail_nodes_cnt'] = len(avail_nodes)
         p['avail_nodes']     = avail_nodes
         p['avail_cpus_cnt']  = avail_cpus_cnt
         p['avail_cpus']      = lst1

         # get running jobs on the parition
         part_jobs        = [j for j in self.job_dict.values() if j['partition'] == p_name]
         p['running_jobs']= [j['job_id'] for j in part_jobs if j['job_state']=='RUNNING']
         p['pending_jobs']= [j['job_id'] for j in part_jobs if j['job_state']=='PENDING']
      else:
         avail_nodes      = p['avail_nodes']
         avail_cpus_cnt,lst1  = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)

      if features:             # restrain nodes with required features
         avail_nodes     = [n for n in avail_nodes if set(self.node_dict[n]['features_active'].split(',')).intersection(features)]
         avail_cpus_cnt,lst1 = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)

      if min_mem_per_node:      # restrain nodes with min memory 
         avail_nodes     = [n for n in avail_nodes if (self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem']) > min_mem_per_node]
         avail_cpus_cnt,lst1 = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
         
      if gres:
         avail_nodes     = [n for n in avail_nodes if self.nodeHaveGres (self.node_dict[n], gres)]
         avail_cpus_cnt,lst1 = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
      #if min_mem_per_cpu:      # restrain nodes with min memory 
      #   avail_cpus,lst1 = SlurmEntities.getIdleCores (self.node_dict, avail_nodes)
      #   avail_nodes     = [n for idx,n in enumerate(avail_nodes) if (self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem'])/lst1[idx] > min_mem_per_cpu]

      #print("addUpdatePartitionInfo return {}, {}".format(len(avail_nodes), avail_cpus_cnt))
      return len(avail_nodes), avail_cpus_cnt

  # return list of partitions with fields, add attributes to partition_dict
  def getPartitions(self, fields=['name', 'flag_shared', 'total_nodes', 'total_cpus', 'avail_nodes_cnt', 'avail_cpus_cnt', 'running_jobs', 'pending_jobs']):
    retList      = []

    for name in sorted(self.partition_dict.keys()):
        p = self.partition_dict[name]
        if not p.get('flag_shared', None):
           self.addUpdatePartitionInfo (name)

        retList.append(MyTool.sub_dict(p, fields))

    return retList

  # return nodeLimit, cpuLimit
  def getPartitionTres (self, partition, tres_attribute):
      tres_str =  self.qos_dict[self.partition_dict[partition]['qos_char']][tres_attribute]
      return SlurmEntities.getTresLimit (tres_str)

  #return jobs in job_list that has a smaller id than job_id
  def getSmallerJobIDs (self, job_id, job_list):
      return [jid for jid in job_list if jid < job_id]

  #return list of current pending jobs sorted by jid, no explaination
  def getCurrentPendingJobs (self, fields=['job_id', 'submit_time', 'user_id', 'account', 'qos', 'partition', 'state_reason']):
      logging.debug ("getCurrentPendingJobs")
      job_dict = pyslurm.job().get()
      pending  = [MyTool.sub_dict(job, fields) for jid,job in job_dict.items() if job['job_state']=='PENDING']

      return datetime.now().timestamp(), pending

  #return list of jobs sorted by jid
  def getPendingJobs (self, fields=['job_id', 'submit_time', 'num_nodes', 'num_cpus', 'user_id', 'account', 'qos', 'partition', 'state_reason', 'user', 'submit_time_str', 'state_exp']):
      #pending = [(MyTool.sub_dict(job, fields)).update({'user', MyTool.getUser(job['user_id'])}) for jid,job in self.job_dict.items() if job['job_state']=='PENDING']
      pending = [job for jid,job in self.job_dict.items() if job['job_state']=='PENDING']
      for job in pending:
          p_name                = job['partition']
          if ',' in p_name:
             #TODO: only get the explaination for the first partition listed
             p_name             = p_name.split(',')[0]
          job['user']           = MyTool.getUser(job['user_id'])
          job['submit_time_str']= str(datetime.fromtimestamp(job['submit_time']))
          job['state_exp']      = PEND_EXP.get(job['state_reason'], '')

          if job['state_reason'] == 'QOSMaxCpuPerUserLimit':
             u_node_qos, u_cpu_qos = self.getPartitionTres (p_name, 'max_tres_pu')
             u_node,     u_cpu     = SlurmEntities.getUserAllocInPartition(job['user_id'], p_name, self.job_dict)
             job['state_exp']      = job['state_exp'].format(user=job['user'], max_cpu_user=u_cpu_qos, curr_cpu_user=u_cpu, partition=p_name)
          elif job['state_reason'] == 'QOSMaxNodePerUserLimit':
             u_node_qos, u_cpu_qos = self.getPartitionTres (p_name, 'max_tres_pu')
             u_node,     u_cpu     = SlurmEntities.getUserAllocInPartition(job['user_id'], p_name, self.job_dict)
             job['state_exp']      = job['state_exp'].format(user=job['user'], max_node_user=u_node_qos, curr_node_user=u_node, partition=p_name)
          elif job['state_reason'] == 'QOSGrpNodeLimit':
             a_node_qos, a_cpu_qos = self.getPartitionTres (p_name, 'grp_tres')
             a_node,     a_cpu     = SlurmEntities.getAllocInPartition(p_name, self.job_dict)
             job['state_exp']      = job['state_exp'].format(max_node_grp=a_node_qos, curr_node_grp=a_node, partition=p_name)
          elif job['state_reason'] == 'QOSGrpCpuLimit':
             a_node_qos, a_cpu_qos = self.getPartitionTres (p_name, 'grp_tres')
             a_node,     a_cpu     = SlurmEntities.getAllocInPartition(p_name, self.job_dict)
             job['state_exp']      = job['state_exp'].format(max_cpu_grp=a_cpu_qos, curr_cpu_grp=a_cpu, partition=p_name)
          elif job['state_reason'] == 'Resources':
             # check features and memory requirement
             j_features            = job.get('features',[])
             j_min_mem_pn          = job.get('pn_min_memory', 0)  if job.get('mem_per_node') else 0
             j_min_mem_pc          = job.get('min_memory_cpu', 0) if job.get('mem_per_cpu') else 0
             # check gres requirement
             j_gres                = MyTool.gresList2Dict(job.get('gres', []))
             j_min_mem_pn          = max(j_min_mem_pn, j_min_mem_pc*job['num_cpus'])
             pa_node,    pa_cpu    = self.addUpdatePartitionInfo (p_name, j_features, j_min_mem_pn, j_gres)
             if j_gres:              j_features.append (MyTool.gresList2Str(job['gres']))
             if j_min_mem_pn:        j_features.append ('mem_per_node={}MB'.format(j_min_mem_pn))
             if j_min_mem_pc:        j_features.append ('mem_per_cpu={}MB'.format(j_min_mem_pc))
             #print("getPendingJobs {} {}".format(pa_node, pa_cpu))
             job['state_exp']      = job['state_exp'].format(partition=p_name, avail_node=pa_node, avail_cpu=pa_cpu, feature='({0})'.format(j_features), start_time=MyTool.getTimeString(job.get('start_time','')))
          elif job['state_reason'] == 'Priority':
             earlierJobs           = self.getSmallerJobIDs(job['job_id'], self.partition_dict[p_name].get('pending_jobs',[]))
             job['state_exp']      = job['state_exp'].format(partition=p_name, higher_job=earlierJobs)
          elif job['state_reason'] == 'JobArrayTaskLimit':
             array_tasks_lst       = [j.get('array_task_id','') for j in self.job_dict.values() if j.get('array_job_id',-1) == job['job_id'] and j.get('job_state','') == 'RUNNING'] 
             array_tasks_lst       = [t for t in array_tasks_lst if t]
             job['state_exp']      = job['state_exp'].format(array_task_str=job.get('array_task_str', ''), array_max_tasks=job.get('array_max_tasks', ''), array_tasks=array_tasks_lst)
          elif job['state_reason'] == 'Dependency':
             job['state_exp']      = job['state_exp'].format(dependency=job.get('dependency'))
          elif job['state_reason'] == 'QOSMaxWallDurationPerJobLimit':
             qos, lmt              = self.getMaxWallPJ(job)
             job['state_exp']      = job['state_exp'].format(job_time_limit=job.get('time_limit_str'), qos=qos, qos_limit=lmt)
          

          if job['sched_nodes']:
             job['state_exp']      += ' Job is scheduled on {}'.format(job['sched_nodes'])
             running    = [job for jid,job in self.job_dict.items() if job['job_state']=='RUNNING']
             schedNode  = set([nm for nm in MyTool.nl2flat (job['sched_nodes']) if self.node_dict[nm]['state']!='IDLE'])
             waitForJob = [job['job_id'] for job in running if schedNode.intersection(set(MyTool.nl2flat(job['nodes'])))]
             if waitForJob:
                job['state_exp']      += ', waiting for running jobs {}.'.format(waitForJob)
             else:
                job['state_exp']      += '.'
             
             #waitForJobs           = [jid for node in job['sched_nodes'] j_dict[jid]['nodes']
             

      pending = [MyTool.sub_dict(job, fields) for job in pending]
      return pending

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

if __name__ == "__main__":

    ins = SlurmEntities()
    ins.getNodes()
    print()

    print(ins.getPendingJobs ())
