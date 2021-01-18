#!/usr/bin/env python00

import time
t1=time.time()
import os,re,subprocess
import pyslurm
import config, MyTool
from datetime import date, timedelta
from functools import reduce

logger    = config.logger
CSV_DIR   = "/mnt/home/yliu/projects/slurm/utils/data/"

class SlurmStatus:
    STATUS_LIST=['undefined', 'running', 'sleeping', 'disk-sleep', 'zombie', 'stopped', 'tracing-stop']

    @classmethod
    def getStatusID (cls, statusStr):
        if statusStr not in cls.STATUS_LIST:
           print("SlurmStatus ERROR: status not existed! " + statusStr)
           cls.STATUS_LIST.append(statusStr)
           return len(cls.STATUS_LIST)-1
        else:
           return cls.STATUS_LIST.index(statusStr)
        
    @classmethod
    def getStatus (cls, idInt):
        return cls.STATUS_LIST[idInt]

class SlurmCmdQuery:
    TS_ASSOC   = 0
    DICT_ASSOC = {}

    def __init__(self):
        pass

    @staticmethod
    def sacct_getUserJobReport (user, days=3, output='JobID,JobIDRaw,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End', skipJobStep=True):
        return SlurmCmdQuery.sacct_getReport(['-u', user], days, output, skipJobStep)

    @staticmethod
    def sacct_getNodeReport (nodeName, days=3, output = 'JobID,JobIDRaw,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End,AllocTRES', skipJobStep=True):
        jobs = SlurmCmdQuery.sacct_getReport(['-N', nodeName], days, output, skipJobStep)
        if 'AllocTRES' in output:
           for job in jobs:
               job['AllocGPUS']=MyTool.getTresGPUCount(job['AllocTRES'])
        return jobs
 
    @staticmethod
    def sacct_getJobReport (jobid, skipJobStep=False):
        #output = 'JobID,JobIDRaw,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End,AllocNodes,NodeList'
        # may include sub jobs
        jobs   = SlurmCmdQuery.sacct_getReport(['-j', str(jobid)], days=None, output='ALL', skipJobStep=skipJobStep)
        if not jobs:
           return None
        return jobs

    # return {jid:jinfo, ...}
    @staticmethod
    def sacct_getReport (criteria, days=3, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End', skipJobStep=True):
        #print('sacct_getReport {} {} {}'.format(criteria, days, skipJobStep))
        if days:
           t = date.today() + timedelta(days=-days)
           startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
           criteria  = ['-S', startDate] + criteria

        field_str, sacct_rlt = SlurmCmdQuery.sacctCmd (criteria, output)
        keys                 = field_str.split(sep='|')
        jobs                 = []
        jid_idx              = keys.index('JobID')
        for line in sacct_rlt:
            ff = line.split(sep='|')
            if (skipJobStep and '.' in ff[jid_idx]): continue # indicates a job step --- under what circumstances should these be broken out?
            #508550_0.extern, 508550_[111-626%20], (array job) 511269+0, 511269+0.extern, 511269+0.0 (?)
            if ( '.' in ff[jid_idx] ):
               ff0 = ff[jid_idx].split(sep='.')[0]
            else:
               ff0 = ff[jid_idx]

            m  = re.fullmatch(r'(\d+)([_\+])(.*)', ff0)
            if not m:
               jid = int(ff0)
            else:
               jid = int(m.group(1))
            if ff[3].startswith('CANCELLED by '):
                uid   = ff[3].rsplit(' ', 1)[1]
                uname = MyTool.getUser(uid)
                ff[3] = '%s (%s)'%(ff[3], uname)
            job = dict(zip(keys, ff))
            jobs.append(job)

        return jobs

    @staticmethod
    def sacctCmd (criteria, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'):
        cmd = ['sacct', '-P', '-o', output] + criteria
        #print('{}'.format(cmd)) 
        try:
            #TODO: capture standard error separately?
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))
        fields, *rlt = d.decode('utf-8').splitlines()
        return fields, rlt

    # given a jid, return [jid, uid, start_time, end_time]
    # if end_time is unknown, return current time
    # if start_time is unknown, error
    def getSlurmJobInfo (self, jid): 
        cmd = ['sacct', '-n', '-P', '-X', '-j', str(jid), '-o', 'JobID,User,NodeList,Start,End']
        try:
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" ERROR: returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        d    = d.decode('utf-8')
        lines= d.split('\n')
        info = lines[0].rstrip().split('|')
        if not info or len(info)<5:
           return [None, None, None, None, None]

        idx  = info[0].find('_')
        s    = []
        if idx != -1:  # job array
           jobid = info[0].split('_')[0]  #jobid
           for l in lines:
               if not l.rstrip():      #empty
                  continue
               li = l.rstrip().split('|')
               #print('--{} - {}'.format(l, li[2]))
               if len(li)>2 and li[2] != "None assigned":
                  s.extend(MyTool.nl2flat(li[2]))
           nodelist = list(set(s))                #nodelist
        else:
           jobid = info[0]
           nodelist = MyTool.convert2list(info[2])
           
        if info[4]!='Unknown':
           return [jobid, MyTool.getUid(info[1]), nodelist, MyTool.str2ts(info[3]), MyTool.str2ts(info[4])]
        else:
           return [jobid, MyTool.getUid(info[1]), nodelist, MyTool.str2ts(info[3]), time.time()]
        
    @staticmethod
    def updateAssoc ():
        file_nm = CSV_DIR + "sacctmgr_assoc.csv"
        file_ts = os.path.getmtime(file_nm)
        if file_ts > SlurmCmdQuery.TS_ASSOC:      # if file is newer
           with open(file_nm) as fp: 
                lines = fp.read().splitlines()
           fields = lines[0].split('|')
           d      = {}
           for i in range(1, len(lines)):
               values = lines[i].split('|')
               d[values[0]] = dict(zip(fields,values))
           SlurmCmdQuery.DICT_ASSOC = d
           SlurmCmdQuery.TS_ASSOC   = file_ts

    #sacctmgr list user -P -s 
    @staticmethod
    def getUserQOS (user):
        userAssoc = getUserAssoc (user)
        if userAssoc:
           return userAssoc['QOS'].split(',')
        return []

    @staticmethod
    def getUserAssoc (user):
        SlurmCmdQuery.updateAssoc ()
        if user in SlurmCmdQuery.DICT_ASSOC:
           return SlurmCmdQuery.DICT_ASSOC[user]
        return {}

    @staticmethod
    def getAllUserAssoc ():
        SlurmCmdQuery.updateAssoc ()
        return SlurmCmdQuery.DICT_ASSOC

class PyslurmQuery():
    @staticmethod
    def getAllNodes ():
        return pyslurm.node().get()

    @staticmethod
    def getGPUNodes (pyslurmNodes):
        #TODO: need to change max_gpu_cnt if no-GPU node add other gres
        gpu_nodes   = [n_name for n_name, node in pyslurmNodes.items() if 'gpu' in node['features']]
        max_gpu_cnt = max([MyTool.getNodeGresGPUCount(pyslurmNodes[n]['gres']) for n in gpu_nodes])
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

#common: ['account', 'array_job_id', 'array_task_id', 'array_task_str', 'array_max_tasks', 'derived_ec', 'nodes', 'partition', 'priority', 'resv_name', 'tres_alloc_str', 'tres_req_str', 'wckey', 'work_dir']
#only in job: ['accrue_time', 'admin_comment', 'alloc_node', 'alloc_sid', 'assoc_id', 'batch_flag', 'batch_features', 'batch_host', 'billable_tres', 'bitflags', 'boards_per_node', 'burst_buffer', 'burst_buffer_state', 'command', 'comment', 'contiguous', 'core_spec', 'cores_per_socket', 'cpus_per_task', 'cpus_per_tres', 'cpu_freq_gov', 'cpu_freq_max', 'cpu_freq_min', 'dependency', 'eligible_time', 'end_time', 'exc_nodes', 'exit_code', 'features', 'group_id', 'job_id', 'job_state', 'last_sched_eval', 'licenses', 'max_cpus', 'max_nodes', 'mem_per_tres', 'name', 'network', 'nice', 'ntasks_per_core', 'ntasks_per_core_str', 'ntasks_per_node', 'ntasks_per_socket', 'ntasks_per_socket_str', 'ntasks_per_board', 'num_cpus', 'num_nodes', 'num_tasks', 'mem_per_cpu', 'min_memory_cpu', 'mem_per_node', 'min_memory_node', 'pn_min_memory', 'pn_min_cpus', 'pn_min_tmp_disk', 'power_flags', 'profile', 'qos', 'reboot', 'req_nodes', 'req_switch', 'requeue', 'resize_time', 'restart_cnt', 'run_time', 'run_time_str', 'sched_nodes', 'shared', 'show_flags', 'sockets_per_board', 'sockets_per_node', 'start_time', 'state_reason', 'std_err', 'std_in', 'std_out', 'submit_time', 'suspend_time', 'system_comment', 'time_limit', 'time_limit_str', 'time_min', 'threads_per_core', 'tres_bind', 'tres_freq', 'tres_per_job', 'tres_per_node', 'tres_per_socket', 'tres_per_task', 'user_id', 'wait4switch', 'gres_detail', 'cpus_allocated', 'cpus_alloc_layout']
#only in db_job: ['alloc_gres', 'alloc_nodes', 'associd', 'blockid', 'cluster', 'derived_es', 'elapsed', 'eligible', 'end', 'exitcode', 'gid', 'jobid', 'jobname', 'lft', 'qosid', 'req_cpus', 'req_gres', 'req_mem', 'requid', 'resvid', 'show_full', 'start', 'state', 'state_str', 'stats', 'steps', 'submit', 'suspended', 'sys_cpu_sec', 'sys_cpu_usec', 'timelimit', 'tot_cpu_sec', 'tot_cpu_usec', 'track_steps', 'uid', 'used_gres', 'user', 'user_cpu_sec', 'wckeyid']
    COMMON_FLD  = ['account', 'array_job_id', 'array_task_id', 'array_task_str', 'array_max_tasks', 'derived_ec', 'nodes', 'partition', 'priority', 'resv_name', 'tres_alloc_str', 'tres_req_str', 'wckey', 'work_dir']
    MAP_JOB2DBJ = {'start_time':'start','end_time':'end', 'exitcode':'exit_code', 'jobid':'job_id', 'user_id':'uid'}
    @staticmethod
    def getSlurmDBJob (jid, req_fields=[]):
        job = pyslurm.slurmdb_jobs().get(jobids=[jid]).get(jid, None)
        if not job:   # cannot find
           return None

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

def test1():
    client = SlurmCmdQuery()
    info = client.getSlurmJobInfo('110972')

def test2():
    client = SlurmDBQuery()
    info = client.getClusterJobQueue()

def test3():
    client = SlurmDBQuery()
    client.getJobByName ('script.sh')

def test4():
    jobs=SlurmCmdQuery.sacct_getJobReport(514269)
    print(repr(jobs))

def test5():
    jobs=SlurmCmdQuery.sacct_getNodeReport('workergpu00')
    print(repr(jobs))

def test6():
    user=SlurmCmdQuery.getUserAssoc('adaly')
    print('{}'.format(user))

def main():
    #job= pyslurm.job().find_id ('88318')
    #job= pyslurm.slurmdb_jobs().get ()
    #print(repr(job))
    t1=time.time()
    #print(SlurmStatus.getStatusID('running'))
    #print(SlurmStatus.getStatusID('running1'))
    #print(SlurmStatus.getStatus(1))
    #info = client.getSlurmJobInfo('105179')
    #info = client.test()
    test4()
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
