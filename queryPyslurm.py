#!/usr/bin/env python

import time
t1=time.time()
import pyslurm
import config, MyTool

from collections import defaultdict
from datetime    import date, timedelta
from functools   import reduce
from querySlurm  import SlurmCmdQuery

logger    = config.logger
MAX_LIMIT = 2**32 - 1  #429496729, biggest integer

class PyslurmQuery():    
    @staticmethod
    def getAllNodes ():
        return pyslurm.node().get()
    
    @staticmethod
    def getQoSDict (cluster="Flatiron", pyslurmData=None):
      qos    = pyslurmData.get("qos", {}) if pyslurmData else {}
      if cluster=="Flatiron":          # local cluster
         qos = pyslurm.qos().get()
      return qos
 
    @staticmethod
    def getGPUNodes (pyslurmNodes):
        #TODO: need to change max_gpu_cnt if no-GPU node add other gres
        gpu_nodes   = [n_name for n_name, node in pyslurmNodes.items() if 'gpu' in node['features']]
        lst         = [MyTool.getNodeGresGPUCount(pyslurmNodes[n]['gres']) for n in gpu_nodes]
        max_gpu_cnt = max(lst) if lst else 0
        return gpu_nodes, max_gpu_cnt

    @staticmethod
    def getJobGPUNodes (jobs, pyslurmNodes):
        gpu_nodes   = reduce(lambda rlt, curr: rlt.union(curr), [set(job['gpus_allocated'].keys()) for job in jobs.values() if 'gpus_allocated' in job and job['gpus_allocated']], set())
        if gpu_nodes:
           max_gpu_cnt = max([MyTool.getNodeGresGPUCount(pyslurmNodes[n]['gres']) for n in gpu_nodes])
        else:
           max_gpu_cnt = 0
        return gpu_nodes, max_gpu_cnt

    @staticmethod
    def getUserCurrJobs (user_id, jobs=None):
        if not jobs:
           jobs = pyslurm.job().get()
        return [job for job in jobs.values() if job['user_id']==user_id]

    @staticmethod
    def getUserRunningJobs (user_id, jobs=None):
        if not jobs:
           jobs = pyslurm.job().get()
        return [job for job in jobs.values() if (job['user_id']==user_id) and (job['job_state']=='RUNNING')]

    @staticmethod
    def getCurrJob (jid, job_dict=None):
        if not job_dict:
           job_dict = pyslurm.job().get()
        if jid not in job_dict:
           return None
        return job_dict[jid]

    @staticmethod
    def getNode (node_name, node_dict=None):
        if not node_dict:
           node_dict = pyslurm.node().get()
        if node_name not in node_dict:
           return None
        return node_dict[node_name]
        
    @staticmethod
    def getNodeAllocJobs (node_name, node=None, node_dict=None, job_dict=None):
        if not node:
           node = getNode(node_name, node_dict)
        if ('ALLOC' not in node['state']) and ('MIXED' not in node['state']):
           # no allocated jobs
           return []
        if not job_dict:
           job_dict = pyslurm.job().get()
           return [job for job in job_dict.values() if node_name in job['cpus_allocated']]
           
    @staticmethod
    def getNodeAllocGPU (node_name, node_dict=None):
        if not node_dict:
           node_dict = pyslurm.node().get()
        if node_name not in node_dict or 'gres' not in node_dict[node_name]:
           return None
        return MyTool.getNodeGresGPUCount(node_dict[node_name]['gres'])

    @staticmethod
    def getJobAllocGPU (job, node_dict=None):
        if not node_dict:
           node_dict = pyslurm.node().get()
        if not job['cpus_allocated']:
           return None
        node_list      = [node_dict[node] for node in job['cpus_allocated']]
        gpus_allocated = MyTool.getGPUAlloc_layout(node_list, job['gres_detail'])
        return gpus_allocated

    @staticmethod
    def getJobAllocGPUonNode (job, node):
        gpus_allocated = MyTool.getGPUAlloc_layout([node], job['gres_detail'])
        if gpus_allocated:
           return gpus_allocated[node['name']]
        else:
           return []

    @staticmethod
    def getUserAllocGPU (uid, node_dict=None):
        if not node_dict:
           node_dict = pyslurm.node().get()
        rlt      = {}
        rlt_jobs = []
        jobs     = PyslurmQuery.getUserCurrJobs(uid)
        if jobs:
           for job in jobs:
               job_gpus = PyslurmQuery.getJobAllocGPU(job, node_dict)
               if job_gpus:   # job has gpu
                  rlt_jobs.append(job)
                  for node, gpu_ids in job_gpus.items():
                      if node in rlt:
                         rlt[node].extend (gpu_ids)
                      else:
                         rlt[node] = gpu_ids
        return rlt, rlt_jobs

    @staticmethod
    def getSlurmDBClusters ():
        c_dict = pyslurm.slurmdb_clusters().get()
        return c_dict

    #common: ['account', 'array_job_id', 'array_task_id', 'array_task_str', 'array_max_tasks', 'derived_ec', 'nodes', 'partition', 'priority', 'resv_name', 'tres_alloc_str', 'tres_req_str', 'wckey', 'work_dir']
    #only in job: ['accrue_time', 'admin_comment', 'alloc_node', 'alloc_sid', 'assoc_id', 'batch_flag', 'batch_features', 'batch_host', 'billable_tres', 'bitflags', 'boards_per_node', 'burst_buffer', 'burst_buffer_state', 'command', 'comment', 'contiguous', 'core_spec', 'cores_per_socket', 'cpus_per_task', 'cpus_per_tres', 'cpu_freq_gov', 'cpu_freq_max', 'cpu_freq_min', 'dependency', 'eligible_time', 'end_time', 'exc_nodes', 'exit_code', 'features', 'group_id', 'job_id', 'job_state', 'last_sched_eval', 'licenses', 'max_cpus', 'max_nodes', 'mem_per_tres', 'name', 'network', 'nice', 'ntasks_per_core', 'ntasks_per_core_str', 'ntasks_per_node', 'ntasks_per_socket', 'ntasks_per_socket_str', 'ntasks_per_board', 'num_cpus', 'num_nodes', 'num_tasks', 'mem_per_cpu', 'min_memory_cpu', 'mem_per_node', 'min_memory_node', 'pn_min_memory', 'pn_min_cpus', 'pn_min_tmp_disk', 'power_flags', 'profile', 'qos', 'reboot', 'req_nodes', 'req_switch', 'requeue', 'resize_time', 'restart_cnt', 'run_time', 'run_time_str', 'sched_nodes', 'shared', 'show_flags', 'sockets_per_board', 'sockets_per_node', 'start_time', 'state_reason', 'std_err', 'std_in', 'std_out', 'submit_time', 'suspend_time', 'system_comment', 'time_limit', 'time_limit_str', 'time_min', 'threads_per_core', 'tres_bind', 'tres_freq', 'tres_per_job', 'tres_per_node', 'tres_per_socket', 'tres_per_task', 'user_id', 'wait4switch', 'gres_detail', 'cpus_allocated', 'cpus_alloc_layout']
    #only in db_job: ['alloc_gres', 'alloc_nodes', 'associd', 'blockid', 'cluster', 'derived_es', 'elapsed', 'eligible', 'end', 'exitcode', 'gid', 'jobid', 'jobname', 'lft', 'qosid', 'req_cpus', 'req_gres', 'req_mem', 'requid', 'resvid', 'show_full', 'start', 'state', 'state_str', 'stats', 'steps', 'submit', 'suspended', 'sys_cpu_sec', 'sys_cpu_usec', 'timelimit', 'tot_cpu_sec', 'tot_cpu_usec', 'track_steps', 'uid', 'used_gres', 'user', 'user_cpu_sec', 'wckeyid']
    COMMON_FLD  = ['account', 'array_job_id', 'array_task_id', 'array_task_str', 'array_max_tasks', 'derived_ec', 'nodes', 'partition', 'priority', 'resv_name', 'tres_alloc_str', 'tres_req_str', 'wckey', 'work_dir']
    MAP_JOB2DBJ = {'submit_time':'submit', 'start_time':'start','end_time':'end', 'exitcode':'exit_code', 'jobid':'job_id', 'job_state':'state_str'}
    DEF_REQ_FLD = COMMON_FLD + list(MAP_JOB2DBJ.keys())
    @staticmethod
    def getSlurmDBJob (jid, req_fields=DEF_REQ_FLD):
        job = pyslurm.slurmdb_jobs().get(jobids=[jid]).get(jid, None)
        if not job:   # cannot find
           return None

        job['user_id']=MyTool.getUid(job['user'])
        for f in req_fields:
            if f in job:
               continue
            if f in PyslurmQuery.MAP_JOB2DBJ:  # can be converted
               if type(PyslurmQuery.MAP_JOB2DBJ[f])!=list:
                  db_fld = PyslurmQuery.MAP_JOB2DBJ[f]
                  job[f] = job[db_fld]
               else:
                  db_fld, cvtFunc = PyslurmQuery.MAP_JOB2DBJ[f]
                  job[f] = cvtFunc(job[db_fld])
            else:                 # cannot be converted
               logger.error("Cannot find/map reqested job field {} in job {}".format(f, job))
        return job


#TODO: job['num_tasks'] what is it
PEND_EXP={
    'QOSMaxCpuPerUserLimit': 'Will exceed QoS user CPU  limit ({max_cpu_user}). User {user} already alloc {curr_cpu_user}  CPUs in {partition}.', # QOS MaxTRESPerUser exceeded (CPU) 
    'QOSMaxNodePerUserLimit':'Will exceed QoS user Node limit ({max_node_user}). User {user} already alloc {curr_node_user} Nodes in {partition}.',	                         # QOS MaxTRESPerUser exceeded (Node)
    'QOSMaxJobsPerUserLimit':'Will exceed QoS user Job limit ({max_job_user}). User {user} already execute {curr_job_user} jobs in {partition}.',
    'QOSMaxGRESPerUser':     'Will exceed QoS user GPU limit ({max_gpu_user}). User {user} already alloc {curr_gpu_user} GPUs in {partition}.',
    'QOSMaxMemoryPerUser':   'Will exceed QoS user Mem limit ({max_mem_user}). User {user} already alloc {curr_mem_user} GPUs in {partition}.',
    'QOSGrpNodeLimit':       'Will exceed QoS Group Node limit ({max_node_grp}). Group already alloc {curr_node_grp} Nodes in {partition}.', # QOS GrpTRES exceeded (Node)
    'QOSGrpCpuLimit':        'Will exceed QoS Group CPU limit  ({max_cpu_grp}). Group already alloc {curr_cpu_grp}  CPUs in {partition}.',
    'QOSMaxWallDurationPerJobLimit': 'Job time {job_time_limit} exceed QoS {qos}\'s MaxWallDurationPerJob limit ({qos_limit}).',
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

class SlurmEntities:
  def __init__ (self, cluster="Flatiron", pyslurmData={}):
    self.cluster = cluster
    if cluster=="Flatiron":           #local realtime pyslurm
        self.getPyslurmData ()
        self.updateTS = int(time.time())
    else:
        self.setPyslurmData (pyslurmData)
        self.updateTS = pyslurmData.get('updateTS',0)
    self.user_assoc_dict= SlurmCmdQuery.getAllUserAssoc(cluster)

    self.part_node_cpu  = {}  # {'gen': [40, 28], 'ccq': [40, 28], 'ib': [44, 28], 'gpu': [40, 36, 28], 'mem': [96], 'bnl': [40], 'bnlx': [40], 'genx': [44, 40, 28], 'amd': [128, 64]}
    for pname,part in self.partition_dict.items():
        self.part_node_cpu[pname] = sorted(set([self.node_dict[name]['cpus'] for name in MyTool.nl2flat(part['nodes'])]), reverse=True)

    # extend node_dict by adding running_jobs, gpu_total, gpu_used
    self.extendNodeDict ()

    # extend partition_dict by adding node_flats, flag_shared, running_jobs, pending_jobs...
    for pname, part in self.partition_dict.items():
        self.extendPartitionDict (pname, part)

  def getPyslurmData (self): 
    self.qos_dict       = pyslurm.qos().get()
    self.partition_dict = pyslurm.partition().get()
    py_node             = pyslurm.node()
    self.node_dict      = py_node.get()
    #self.ts_node_dict   = py_node.lastUpdate()        # must retrieve ts as above seq
    py_job              = pyslurm.job()
    self.job_dict       = py_job.get()
    #self.ts_job_dict    = py_job.lastUpdate()
    #self.res_future     = [res for res in pyslurm.reservation().get().values() if res['start_time']>time.time()]  # reservation in the future
    self.res_future     = []
    r                   = pyslurm.reservation()   # slurm20: pyslurm.reservation().get() core dump
    if r.ids():
      for name,res in pyslurm.reservation().get().items():
        if res['start_time']>time.time():  # reservation in the future
           res['job_id']    = name          # just for simplicity
           self.res_future.append (res)

  def setPyslurmData (self, pyslurmData): 
                                                      #{'jobs':pyslurm.job().get(), 'nodes':pyslurm.node().get(), 'partition':pyslurm.partition().get(), 'qos':pyslurm.qos().get(), 'reservation':pyslurm.reservation().get(), 'extra_pyslurm':True}
    self.qos_dict       = pyslurmData.get('qos',       {})
    self.partition_dict = pyslurmData.get('partition', {})
    self.node_dict      = pyslurmData.get('nodes',     {})
    self.job_dict       = pyslurmData.get('jobs',      {})
    self.res_future     = []
    for name,res in pyslurmData.get('reservation',{}).items():
        if res['start_time']>time.time():  # reservation in the future
           res['job_id']    = name          # just for simplicity
           self.res_future.append (res)

  @staticmethod
  def getQoSTresDict (tres_str):
    d = {}
    if tres_str:
      for tres in tres_str.split(','):
        t, value = tres.split('=')
        d[t]     = value
    return d       
 
  #return nodeLimit, cpuLimit (max value TCMO)
  #'max_tres_pu': '1=320,2=9000000,1001=32'
  #1: cpu, 4: node, 1001: gpu
  @staticmethod
  def getQoSTresLimit (tres_str, defaultValue=MAX_LIMIT):
    d = SlurmEntities.getQoSTresDict(tres_str)
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

  def idleNode (self, node_name):
      if (node_name not in self.node_dict) or ('state' not in self.node_dict[node_name]):
         return False 
      node_state = self.node_dict[node_name]['state']
      if node_state == 'IDLE' or node_state=='IDLE+POWER':
         return True
      else:
         return False
      
  # modify self.partition_dict by adding attributes to partition p_name, flag_shared, avail_nodes, avail_cpus, running_jobs, pending_jobs
  # return avail_nodes, avail_cpus with the constrain of features and min_mem_per_node
  def extendPartitionDict (self, p_name, p):
      if not p.get('flag_shared', None):  # not extend yet
         p['flag_shared'] = 'YES' if self.getSharedFlag(p) else 'NO'

         if p['nodes']:
            nodes             = MyTool.nl2flat(p['nodes'])
            avail_nodes       = [n for n in nodes if self.idleNode(n)]
            #avail_nodes       = [n for n in nodes if self.node_dict.get(n, {}).get('state', None) == 'IDLE']
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
      lst = job_tres_per_node.split(',')  #gpu:v100-32gb:2
      if len(lst) > 1:
         logger.warning ('tres_per_node of job have more than one value. TODO: change the code')

      lst = lst[0].split(':')
      if lst[0] != 'gpu':   
         logger.warning ('tres_per_node of job change format and have new key.')
         return True
      if 'gpus' in pyslurm_node:  # no gpu on the node
         gpuAvail = pyslurm_node['gpus'] - pyslurm_node['alloc_gpus']
         if int(lst[-1]) <= gpuAvail:  # count is satisfied
            if len(lst)==2:
               return True
            #check feature lst[1] with pyslurm_node['gres'], pyslurm_node['gres_used']
            #TODO: assuming the same type of GPU on one node
            node_gpu_type = pyslurm_node['gres'][0].split(':')[1]  #gpu:v100s-32gb:4(S:0-1)
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
      logger.debug('---Job {} init avail_nodes={}'.format(job['job_id'], avail_nodes))

      # exclude reserved nodes
      if avail_nodes and self.res_future:
         avail_nodes, conflict_res = self.getConflictResNodes(job, avail_nodes, self.res_future, node_field='node_list')
         #avail_nodes          = [node for node in avail_nodes if node not in res_nodes]
         logger.debug('---reservation avail_nodes={}'.format(avail_nodes))

      # avail_nodes - reserved_nodes_for_higherPending
      if avail_nodes and higherPending:
         avail_nodes, conflict_res = self.getConflictResNodes(job, avail_nodes, higherPending, node_field='sched_nodes')
         #avail_nodes          = [node for node in avail_nodes if node not in res_nodes]
         logger.debug('---higher_pending, avail_nodes={}'.format(avail_nodes))
         
      # exclude nodes by job request
      if avail_nodes and job.get('exc_nodes',[]):
         exc_nodes        = []
         for item in job['exc_nodes']:
             exc_nodes.extend (MyTool.nl2flat(item))
         avail_nodes      = list(set(avail_nodes).difference(set(exc_nodes)))
         logger.debug('---exc_nodes avail_nodes={}'.format(avail_nodes))

      # restrain nodes with required features
      features            = job.get('features',[])
      if avail_nodes and features:        
         avail_nodes      = [n for n in avail_nodes if set(self.node_dict[n]['features_active'].split(',')).intersection(features)]
         logger.debug('---feature ({}), avail_nodes={}'.format(features, avail_nodes))

      # restrain nodes if job cannot share 
      if avail_nodes and job['shared']=='0':
         avail_nodes      = [n for n in avail_nodes if self.idleNode(n)]
         #avail_nodes      = [n for n in avail_nodes if self.node_dict[n]['state']=='IDLE']
         features.append ('exclusive')
         logger.debug('---shared avail_nodes={}'.format(avail_nodes))

      # restrain nodes with gpu 
      gpu_per_node        = job['tres_per_node']    #'gpu:v100-32gb:01' or 'gpu:4'
      if (not gpu_per_node) and (job['num_nodes']==1):
         gpu_per_node     = job['tres_per_job']
      if avail_nodes and gpu_per_node:
         avail_nodes      = [n for n in avail_nodes if self.nodeWithGPU (self.node_dict[n], gpu_per_node)]  #'gpu:v100-32gb:01' or 'gpu:4'
         features.append (gpu_per_node)
         logger.debug('---gpu avail_nodes={}'.format(avail_nodes))

      # restrain nodes with cpu      
      if avail_nodes and job['ntasks_per_node']:
         avail_nodes      = [n for n in avail_nodes if (self.node_dict[n]['cpus']-self.node_dict[n]['alloc_cpus'])>=job['ntasks_per_node']]
         features.append ('{} tasks'.format(job['ntasks_per_node']))
         logger.debug('---ntask_per_node ({}), avail_nodes={}'.format(job['ntasks_per_node'], avail_nodes))

      # restrain nodes with min memory 
      if avail_nodes and job['mem_per_node'] and job['pn_min_memory']:  
         avail_nodes      = [n for n in avail_nodes if (self.node_dict[n]['real_memory'] - self.node_dict[n]['alloc_mem']) >= job['pn_min_memory']]
         features.append ('{}MB mem_per_node'.format(job['pn_min_memory']))
         logger.debug('---mem_per_node ({}), avail_nodes={}'.format(job['pn_min_memory'], avail_nodes))

      # restrain nodes at cpu level
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
      logger.debug('---sum_of_cpu avail_nodes={}'.format(avail_nodes))

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
         logger.debug('---mem_per_cpu avail_nodes={}'.format(avail_nodes))

      return len(avail_nodes), avail_cpus_cnt, avail_nodes, features, conflict_res
      
  # return list of partitions with fields, add attributes to partition_dict
  def getPartitions(self):
    return [self.partition_dict[name] for name in sorted(self.partition_dict.keys())]

  def getPartitionQoS (self, p_name):
      partition   = self.partition_dict[p_name]
      #preempty partition qos_char is None
      if partition['qos_char']:
         return self.qos_dict[partition['qos_char']]
      else:
         return None

  def getJobQoSValue (self, job, p_name, attr):
      part_qos = self.getPartitionQoS (p_name)
      value    = part_qos.get(attr, None) if part_qos else None
      if value:
         return value
      #job qos
      job_qos  = self.getPartitionQoS(job['qos']) if 'qos' in job else None
      value    = job_qos.get(attr, None)  if job_qos  else None
      return value

  def getJobQoSTresDict (self, job, part_qos, tres_attribute):
      part_tres_str  = part_qos.get(tres_attribute, None) if part_qos else None
      job_tres_str   = self.qos_dict[job['qos']].get(tres_attribute, None)

      part_tres_dict = MyTool.getTresDict (part_tres_str, mapKey=True)
      job_tres_dict  = MyTool.getTresDict (job_tres_str,  mapKey=True)

      logger.debug("part_tres={},job_tres={}".format(part_tres_dict,job_tres_dict))
      #partition QoS override Job QoS
      for key, value in job_tres_dict.items():
          if key in part_tres_dict:
             continue
          part_tres_dict[key] = value
      return part_tres_dict

  #TODO: consider OverPartQOS
  #return nodeLimit, cpuLimit, gresLimit
  def getJobQoSTresLimit (self, job, part_qos, tres_attribute, OverPartQOS=False):
      tres_dict  = self.getJobQoSTresDict (job, part_qos, tres_attribute) 
      return tres_dict.get('node',MAX_LIMIT), tres_dict.get('cpu',MAX_LIMIT), tres_dict.get('gpu',MAX_LIMIT)

  # return nodeLimit, cpuLimit, gresLimit
  def getPartQoSTresLimit (self, partition, tres_attribute):
      tres_str =  self.qos_dict[self.partition_dict[partition]['qos_char']][tres_attribute]
      return SlurmEntities.getQoSTresLimit (tres_str)

  #return jobs in job_list that has a smaller id than job_id
  def getSmallerJobIDs (self, job_id, job_list):
      return [jid for jid in job_list if jid < job_id]

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
          logger.debug("{}:{} pending expl: {}".format(job['job_id'], job['state_reason'], exp)) 
      return pending

  def sumJobMemAlloc(jobLst):
    cpu_cnt  = sum([t.get('cpu',0) for t in tres])
    node_cnt = sum([t.get('node',0) for t in tres])
    if gpu_flag:
       gpu_cnt = sum([t.get('gres/gpu',0) for t in tres])
       return node_cnt, cpu_cnt, gpu_cnt
    else:
       return node_cnt, cpu_cnt, 0

  def getUserMemAlloc_Partition(uid, pname, job_dict):
      jobLst         = [job for job in job_dict.values() if job['job_state']=='RUNNING' and job['user_id']==uid and job['partition']==pname ]
      tres_alloc     = [MyTool.getTresDict(job['tres_alloc_str']) for job in jobLst]
      mem_alloc      = MyTool.sumOfListWithUnit([t['mem']         for t   in tres_alloc if 'mem' in t])
      return mem_alloc

  def exp_QOSMaxMemoryPerUser (self, job, p_name, higherJobs=None):
      part_qos       = self.getPartitionQoS (p_name) 
      tres_d         = self.getJobQoSTresDict (job, part_qos, 'max_tres_pu')
      mem_limit      = tres_d['mem'] if 'mem' in tres_d else MAX_LIMIT        # 2 is memory
      mem_alloc      = SlurmEntities.getUserMemAlloc_Partition(job['user_id'], p_name, self.job_dict)
      state_exp      = PEND_EXP['QOSMaxMemoryPerUser'].format(user=job['user'], max_mem_user='{}M'.format(mem_limit), curr_mem_user=mem_alloc, partition=p_name)
      return state_exp

  def exp_QOSMaxJobsPerUserLimit (self, job, p_name, higherJobs=None):
      state_exp  = PEND_EXP['QOSMaxJobsPerUserLimit']
      uid        = job['user_id']
      u_job      = sum([1 for j in self.job_dict.values() if j['job_state']=='RUNNING' and j['user_id']==uid and j['partition']==p_name ])
      state_exp  = state_exp.format(user=MyTool.getUser(uid, self.cluster), max_job_user=self.getJobQoSValue(job, p_name, 'max_jobs_pu'), curr_job_user=u_job, partition=p_name)

      return state_exp

  def exp_Resources (self, job, p_name, higherJobs):
      state_exp  = PEND_EXP['Resources']
      pa_node, pa_cpu, pa_node_lst, features, conflict_res = self.getPartitionAvailNodeCPU (p_name, job, higherPending=higherJobs)
      pa_node_str                                          = '{}'.format(pa_node)
      if pa_node:
         #pa_node_str = '{} ({})'.format(pa_node,['<a href=./nodeDetails?node={node_name}>{node_name}</a>'.format(node_name=node) for node in sorted(pa_node_lst)])
         pa_node_str = '{} ({})'.format(pa_node,['{node_name}'.format(node_name=node) for node in sorted(pa_node_lst)])
      state_exp      = state_exp.format(partition=p_name, avail_node=pa_node_str, avail_cpu=pa_cpu, feature='({0})'.format(features))
      if job.get('state_reason_desc',None): #modify pyslurm to add state_reason_desc 02/27/2020
         state_exp   = '{} ({}). {} '.format(job['state_reason_desc'].replace('_',' '), [res['job_id'] for res in conflict_res], state_exp)
      return state_exp

  EXPLAIN_FUNC = {'QOSMaxJobsPerUserLimit':exp_QOSMaxJobsPerUserLimit, 
                  'QOSMaxMemoryPerUser':   exp_QOSMaxMemoryPerUser,
                  'Resources':             exp_Resources}
  def explainPendingJob(self, job, p_name, higherJobs, reserved_nodes):
      job['user']    = MyTool.getUser(job['user_id'], self.cluster)  # will be used in html
      if '_' in job['state_reason']:
         # mod in pyslurm not working anymore, workaround 02/05/2021
         job['state_reason_desc'] = job['state_reason']
         job['state_reason']      = 'Resources'

      state_exp      = PEND_EXP.get(job['state_reason'], '')
      p_qos          = self.getPartitionQoS (p_name)
      if job['state_reason'] in SlurmEntities.EXPLAIN_FUNC:
         state_exp   = SlurmEntities.EXPLAIN_FUNC[job['state_reason']](self, job, p_name, higherJobs)
      elif job['state_reason'] == 'QOSMaxWallDurationPerJobLimit':
         qos, lmt       = self.getMaxWallPJ(job)
         state_exp      = state_exp.format(job_time_limit=job.get('time_limit_str'), qos=qos, qos_limit=lmt)
      elif job['state_reason'] == 'QOSMinGRES':
         state_exp      = 'A job need to request at least 1 GPU in partition {}'.format(p_name)     
      elif 'PerUser' in job['state_reason']:
         u_node_qos, u_cpu_qos, u_gpu_qos = self.getJobQoSTresLimit (job, p_qos, 'max_tres_pu')
         u_node,     u_cpu,     u_gpu     = SlurmEntities.getUserAlloc_Partition(job['user_id'], p_name, self.job_dict)
         if job['state_reason'] == 'QOSMaxCpuPerUserLimit':
            state_exp      = state_exp.format(user=job['user'], max_cpu_user=u_cpu_qos, curr_cpu_user=u_cpu, partition=p_name)
            u_cpu_avail    = u_cpu_qos - u_cpu
            if job['shared']!= 'OK' and (u_cpu_avail >0) and (MyTool.getTresDict(job['tres_req_str'])['cpu']) <= u_cpu_avail:
                   state_exp   = '{} {}'.format(state_exp, 'Job request exclusive nodes and may allocate more CPUs than requested.')
         elif job['state_reason'] == 'QOSMaxNodePerUserLimit':
            state_exp      = state_exp.format(user=job['user'], max_node_user=u_node_qos, curr_node_user=u_node, partition=p_name)
         elif job['state_reason'] == 'QOSMaxGRESPerUser':
            state_exp      = state_exp.format(user=job['user'], max_gpu_user =u_gpu_qos,  curr_gpu_user=u_gpu, partition=p_name)
      elif job['state_reason'] == 'QOSGrpNodeLimit':
         a_node_qos, a_cpu_qos, etc    = self.getJobQoSTresLimit (job, p_qos, 'grp_tres')
         j_account                     = self.user_assoc_dict[MyTool.getUser(job['user_id'], self.cluster)]['Account']
         a_node,     a_cpu,     etc    = SlurmEntities.getAllAlloc_Partition(p_name, self.job_dict)
         state_exp                     = state_exp.format(max_node_grp=a_node_qos, curr_node_grp=a_node, partition=p_name)
      elif job['state_reason'] == 'QOSGrpCpuLimit':
         a_node_qos, a_cpu_qos, etc    = self.getJobQoSTresLimit (job, p_qos, 'grp_tres')
         j_account                     = self.user_assoc_dict[MyTool.getUser(job['user_id'], self.cluster)]['Account']
         a_node,     a_cpu,     etc    = SlurmEntities.getAllAlloc_Partition(p_name, self.job_dict)
         state_exp                     = state_exp.format(max_cpu_grp=a_cpu_qos, curr_cpu_grp=a_cpu, partition=p_name)
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
         schedNode  = set([nm for nm in MyTool.nl2flat (job['sched_nodes']) if self.idleNode(nm)])
         #schedNode  = set([nm for nm in MyTool.nl2flat (job['sched_nodes']) if self.node_dict[nm]['state']!='IDLE'])
         waitForJob = ['<a href="./jobDetails?jid={jid}">{jid}</a>'.format(jid=job['job_id']) for job in running if schedNode.intersection(set(job['cpus_allocated']))]
         if waitForJob:
            state_exp      += ', waiting for running jobs {}.'.format(waitForJob)
         else:
            state_exp      += '.'
      elif job['start_time']:
         state_exp      += ' Job will start no later than {}.'.format(MyTool.getTsString(job['start_time']))
 
      return state_exp

  #@staticmethod
  #def findNodeInState(node_dict, state):
  #  nodes   = [nid for nid,node in node_dict.items() if node['state']==state]
  #  return nodes

  @staticmethod
  def getIdleCores (node_dict, node_list):
    nodes   = [node for nid,node in node_dict.items() if nid in node_list]
    ic_list = [node['cpus']-node['alloc_cpus'] for node in nodes]
    total   = sum(ic_list)

    return total, ic_list

  # return jobs of a user {'RUNNING':[], 'otherstate':[]}
  def getUserJobsByState (self, uid):
      job_dict  = self.job_dict
      result    = defaultdict(list)  #{state:[job...]}
      jobs      = [job for job in job_dict.values() if job['user_id']==uid]
      for job in jobs:
          result[job['job_state']].append (job)
      return dict(result)

  def getAllAlloc_Partition ( pname, job_dict ):
      jobLst    = [ job for job in job_dict.values() if job['partition'] == pname and job['job_state']=='RUNNING' ]
      return SlurmEntities.getJobAlloc(jobLst, True)


  # get user+partion
  def getUserAlloc_Partition( uid, pname, job_dict):
      jobLst    = [ job for job in job_dict.values() if job['job_state']=='RUNNING' and job['user_id'] == uid and job['partition'] == pname ]
      return SlurmEntities.getJobAlloc(jobLst, gpu_flag=True)

  def sumAllAlloc_Partition ( pname, job_dict ):
      jobLst    = [ job for job in job_dict.values() if job['partition'] == pname and job['job_state']=='RUNNING' ]
      return SlurmEntities.sumJobAlloc(jobLst, True, True)
  def sumUserAlloc_Partition( uid, pname, job_dict):
      jobLst    = [ job for job in job_dict.values() if job['job_state']=='RUNNING' and job['user_id'] == uid and job['partition'] == pname ]
      return SlurmEntities.sumJobAlloc(jobLst, True, True)

  def sumJobAlloc(jobLst, gpu_flag=True, mem_flag=True):
    tres = [MyTool.getTresDict(job['tres_alloc_str']) for job in jobLst]
    rlt  = {'job'  : len(jobLst),
            'cpu'  : sum([t['cpu']     for t in tres if 'cpu'      in t]), 
            'node' : sum([t['node']    for t in tres if 'node'     in t])}
    if gpu_flag:
       rlt['gpu'] = sum([t['gres/gpu'] for t in tres if 'gres/gpu' in t])
    if mem_flag:
       rlt['mem']    = MyTool.sumOfListWithUnit([t['mem'] for t in tres if 'mem' in t])
       rlt['mem_MB'] = MyTool.convert2M(rlt['mem'])
    return rlt

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

  # based on how node_lst is retrieved, max_cpu>min_lmt, len(node_lst)>=num_node
  def getAvailCPURange (self, node_lst, num_node, min_lmt):
      # sort the node_lst on avail cpus
      # node_lst is already sorted decreasing in getNodesWithCPUs
      cpu_lst = [self.node_dict[name]['cpus']-self.node_dict[name]['alloc_cpus'] for name in node_lst]
      cpu_lst.sort()
      max_cpu = sum(cpu_lst[-num_node:])
      min_cpu = sum(cpu_lst[0:num_node])  # no need to be more accurate based on how node_lst is retrieved
      return max(min_cpu,min_lmt), max_cpu

      #sort_lst = [(name, self.node_dict[name]['cpus']-self.node_dict[name]['alloc_cpus']) for name in node_lst]
      #sort_lst.sort(key=lambda i: i[1], reverse=True) #sorted by available cpus decreasingly
      #val_lst  = [i[1] for i in sort_lst]
      #max_cpus = sum(val_lst[:num_node])    
      #min_cpus = sum(val_lst[-num_node:])  
      #return min_cpus, max_cpus
    
  def relaxJob_Part (self, job, p_name):
      if not self.partition_dict[p_name]['avail_cpus_cnt']:   # no resource
         return ''
      p_node_avail,p_cpu_avail,p_node_avail_lst,features,conflict_res= self.getPartitionAvailNodeCPU (p_name, job, strictFlag=True) 
      if p_node_avail==0:
         return ''
      # job has resource to run
      logger.debug("Job {} can run on {}:{} nodes len={} cpu={} features={}".format(job['job_id'], p_name, p_node_avail_lst, p_node_avail, p_cpu_avail, features))
      job['qos_relax_nodes'] = p_node_avail_lst

      user_alloc             = SlurmEntities.sumUserAlloc_Partition (job['user_id'], p_name, self.job_dict)  #user allocation
      p_qos                  = self.getPartitionQoS (p_name)
      suggestion             = ''
      if job['state_reason'] == 'QOSMaxJobsPerUserLimit':
         if p_qos:
            job_limit        = p_qos['max_jobs_pu']
            suggestion       = 'Increase User Job Limit from {} to {}. '.format (job_limit, user_alloc['job']+1)

      user_limit             = self.getJobQoSTresDict (job, p_qos, 'max_tres_pu')           #qos limit for user
      grp_limit              = self.getJobQoSTresDict (job, p_qos, 'grp_tres')              #qos limit for grp
      grp_alloc              = SlurmEntities.sumAllAlloc_Partition  (p_name, self.job_dict) #all allocation in part, seems where the grp limit applied

      job_tres_req                    = MyTool.getTresDict(job['tres_req_str'])
      if 'mem' in job_tres_req:
         job_tres_req['mem_MB']       = MyTool.convert2M (job_tres_req['mem'])

      # check node using job['num_nodes']
      logger.debug("user_limit={},user_alloc={},job_tres_req={},job_num_nodes={}".format(user_limit, user_alloc, job_tres_req, job['num_nodes']))
      logger.debug("grp_limit={},grp_alloc={}".format(grp_limit, grp_alloc))
      if 'node' in user_limit:
         new_NodeAlloc                = user_alloc.get('node',0) + job['num_nodes'] 
         if new_NodeAlloc > user_limit['node']:
            suggestion += 'Increase User Node Limit from {} to {}. '.format (user_limit['node'], new_NodeAlloc)
      if 'node' in grp_limit:
         new_NodeAlloc                = grp_alloc.get ('node',0) + job['num_nodes']
         if new_NodeAlloc > grp_limit['node']:
            suggestion += 'Increase Group Node Limit from {} to {}. '.format (grp_limit['node'], new_NodeAlloc)

      # check cpu
      if job['shared']=='OK' and self.partition_dict[p_name]['flag_shared'] == 'YES':
         j_min_cpus, j_max_cpus       = job['num_cpus'], job['num_cpus']
      else:
         j_min_cpus, j_max_cpus       = self.getAvailCPURange (job['qos_relax_nodes'], job['num_nodes'],job['num_cpus'])
      if 'cpu' in user_limit:
         new_CPUAlloc_min             = user_alloc.get('cpu',0) + j_min_cpus
         new_CPUAlloc_max             = user_alloc.get('cpu',0) + j_max_cpus
         if new_CPUAlloc_min > user_limit['cpu']:
            if new_CPUAlloc_min == new_CPUAlloc_max:
               suggestion += 'Increase User CPU Limit from {} to {}. '.format     (user_limit['cpu'], new_CPUAlloc_max)
            else:
               suggestion += 'Increase User CPU Limit from {} to [{},{}]. '.format(user_limit['cpu'], new_CPUAlloc_min,new_CPUAlloc_max)
      if 'cpu' in grp_limit:
         new_CPUAlloc_min             = grp_alloc.get('cpu',0) + j_min_cpus
         new_CPUAlloc_max             = grp_alloc.get('cpu',0) + j_max_cpus
         if new_CPUAlloc_min > grp_limit['cpu']:
            if new_CPUAlloc_min == new_CPUAlloc_max:
               suggestion += 'Increase Group CPU Limit from {} to {}. '.format     (grp_limit['cpu'], new_CPUAlloc_max)
            else:
               suggestion += 'Increase Group CPU Limit from {} to [{},{}]. '.format(grp_limit['cpu'], new_CPUAlloc_min, new_CPUAlloc_max)

      # check gpu
      if 'gpu' in user_limit:
         new_GPUAlloc                 = user_alloc.get('gpu',0)+job_tres_req.get('gres/gpu',0)
         if new_GPUAlloc > user_limit['gpu']:   
            suggestion += 'Increase User GPU Limit from {} to {}. '.format (user_limit['gpu'], new_GPUAlloc)
      if 'gpu' in grp_limit:
         new_GPUAlloc                 = grp_alloc.get('gpu',0)+job_tres_req.get('gres/gpu',0)
         if new_GPUAlloc > grp_limit['gpu']:
            suggestion += 'Increase Group GPU Limit from {} to {}. '.format (grp_limit['gpu'], new_GPUAlloc)

      # check mem
      if 'mem' in user_limit:
         new_memAlloc                 = user_alloc.get('mem_MB',0)  +job_tres_req.get('mem_MB',0) 
         if new_memAlloc > user_limit['mem']:
            suggestion += 'Increase User Mem Limit from {} to {}. '.format (user_limit['mem'], new_memAlloc)
      if 'mem' in grp_limit:
         new_memAlloc                 = grp_alloc.get('mem_MB',0)  +job_tres_req.get('mem_MB',0) 
         if new_memAlloc > grp_limit['mem']:
            suggestion += 'Increase User GPU Limit from {} to {}. '.format (grp_limit['mem'], new_memAlloc)
      return suggestion

  # can the job be scheduled if QoS is relaxed
  # assume that job is the first in the queue for its partition
  # return a suggestion
  def relaxQoS(self):
    #loop over all the pending jobs sorted by priority TODO: currently just jid will do
    pendingJids  = sorted([jid for jid,job in self.job_dict.items() if job['job_state']=='PENDING'])
    higherJobs   = []
    rlt          = {}
    for jid in pendingJids:
        #logger.debug('---relaxQoS Job {}'.format(jid))
        job  = self.job_dict[jid]
        user = job['user'] if 'user' in job else job['user_id']
      
        if ('QOS' in job['state_reason']) and (user not in rlt):    # QOS pending job and no previous suggestion for the user
           p_name = job['partition'].split(',')[0] # only consider first partition
           suggestion = self.relaxJob_Part(job, p_name)
           if suggestion:
              delayJobs = self.mayDelayPending(job, higherJobs)
              if delayJobs:
                 suggestion += 'QoS relax of the job may delay highe priority pending jobs {}'.format([job['job_id'] for job in delayJobs])
              suggestion = 'To run job {} in partition {} with candidate nodes {}, the following relaxation of QoS {} will be recommended. '.format(jid,p_name, job['qos_relax_nodes'], self.partition_dict[p_name]['qos_char']) + suggestion
              rlt[user]= suggestion
 
        if job['start_time']:   # see if will influce other jobs
           higherJobs.append(job)
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
      logger.debug("partHasJobResource {} {} {} {}".format(p_node_avail, p_cpu_avail, p_node_avail_lst, features))
      return True

  def test1():
      ins = SlurmEntities()
      rlt = ins.getNodesWithCPUs(['worker0044', 'worker3001', 'worker4007','workergpu01'], 3, 30)
      logger.debug (rlt)
  def test2():
      ins = SlurmEntities()
      rlt = ins.relaxQoS()
      logger.debug (rlt)

#+---------------+---------+------+----------------+------+
#| creation_time | deleted | id   | type           | name |
#+---------------+---------+------+----------------+------+
#|    1505726834 |       0 |    1 | cpu            |      |
#|    1505726834 |       0 |    2 | mem            |      |
#|    1505726834 |       0 |    3 | energy         |      |
#|    1505726834 |       0 |    4 | node           |      |
#|    1544835407 |       0 |    5 | billing        |      |
#|    1553370225 |       0 |    6 | fs             | disk |
#|    1553370225 |       0 |    7 | vmem           |      |
#|    1553370225 |       0 |    8 | pages          |      |
#|    1536253094 |       1 | 1000 | dynamic_offset |      |
#|    1559052921 |       0 | 1001 | gres           | gpu  |
#+---------------+---------+------+----------------+------+

def test1():
    d = PyslurmQuery.getQoSDict()
    print(d)

if __name__ == "__main__":
    test1()
    #SlurmEntities.test2()
    #ins = SlurmEntities()
    #ins.getPendingJobs()
    #ins.getUserPartition('agabrielpillai')
    #ins.getPartitions()
