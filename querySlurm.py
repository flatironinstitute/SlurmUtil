#!/usr/bin/env python00

import time
t1=time.time()
import subprocess
import os
import pyslurm
import MyTool
import pandas as pd

from collections import defaultdict
from datetime import datetime, date, timezone, timedelta

SLURM_STATE_DICT   = {0:'PENDING', 1:'RUNNING', 3:'COMPLETED', 4:'CANCELED', 5:'FAILED', 6:'TIMEOUT', 7:'NODE_FAIL', 8:'PREEMPTED', 11:'OUT_OF_MEM', 8192:'RESIZING'}

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

class SlurmDBQuery:
    def __init__(self):
        self.job_df    = None
        self.job_df_ts = 0
  
    def updateDB (self):
        subprocess.call('../mysqldump.sh')

    def getJobTable (self):
        #read file time, if updated from last time, reset the value
        f_name = "slurm_cluster_job_table.csv"
        m_time = os.stat(f_name).st_mtime
        if m_time > self.job_df_ts:
           self.job_df    = pd.read_csv(f_name,usecols=['cpus_req','id_job','state', 'time_submit', 'time_eligible', 'time_start', 'time_end'])
           self.job_df_ts = m_time

        return self.job_df
        
    def getAccountUsage_hourly (self, start='', stop=''):
        # check if the data too old
        # if yes, generate data
        # generate 

        #cluster usage
        df         = pd.read_csv("slurm_cluster_assoc_usage_hour_table.csv", names=['creation_time','mod_time','deleted','id','id_tres','time_start','alloc_secs'], usecols=['id','id_tres','time_start','alloc_secs'])
        st, stp, df= MyTool.getDFBetween (df, 'time_start', start, stop)

        # get account's data, id_assoc (user) - account
        userDf     = pd.read_csv("slurm_cluster_assoc_table.csv", usecols=['id_assoc','acct'], index_col=0)
        # add acct to df
        df['acct'] = df['id'].map(userDf['acct'])
        df.drop('id', axis=1, inplace=True)

        # sum over the same id_tres, acct, time_start
        sumDf       = df.groupby(['id_tres','acct', 'time_start']).sum()
        sumDf['ts'] = sumDf.index.get_level_values('time_start') * 1000

        return st, stp, sumDf

    def getUserReport_hourly(self, start='', stop='', top=5):
        # get top 5 user for each resource
        df     = pd.read_csv("slurm_cluster_assoc_usage_day_table.csv", names=['creation_time','mod_time','deleted','id','id_tres','time_start','alloc_secs'], usecols=['id','id_tres', 'alloc_secs', 'time_start'])
        st, stp, df     = MyTool.getDFBetween (df, 'time_start', start, stop)
        sumDf  = df.groupby(['id_tres','id']).sum()

        top    = int(top)
        cpuIdx = sumDf.loc[(1,)].nlargest(top, 'alloc_secs').index
        memIdx = sumDf.loc[(2,)].nlargest(top, 'alloc_secs').index
        nodeIdx= sumDf.loc[(4,)].nlargest(top, 'alloc_secs').index
        #topIdx = cpuIdx.union(memIdx).union(nodeIdx)

        userDf = pd.read_csv("slurm_cluster_assoc_table.csv",            usecols=['id_assoc','user','acct'], index_col=0)
        df     = pd.read_csv("slurm_cluster_assoc_usage_hour_table.csv", names=['creation_time','mod_time','deleted','id','id_tres','time_start','alloc_secs'], usecols=['id','id_tres','time_start','alloc_secs'])
        st, stp, df     = MyTool.getDFBetween (df, 'time_start', start, stop)
        # get top users data only
        dfg    = df.groupby(['id_tres','id'])
        tresSer= {1:[],     2:[],     4:[]} # {1: [{'data': [[ms,value],...], 'name': uid},...], 2:...} 
        idxSer = {1:cpuIdx, 2:memIdx, 4:nodeIdx}
        for tres in [1,2,4]:
            for uid in idxSer[tres]:
                topDf          = dfg.get_group((tres,uid))
                topDf['ts_ms'] = topDf['time_start'] * 1000
                topLst         = topDf[['ts_ms','alloc_secs']].values.tolist()
                tresSer[tres].append({'data': topLst, 'name': userDf.loc[uid,'user']})

        return tresSer
    
    # return time_col, value_cols
    def getTimeIndexValue (self, df, time_col, value_cols):
        df_time = df[[time_col] +  value_cols]
        values  = ['value' + str(i) for i in range(len(value_cols))]
        df_time.columns = ['time'] + values
        df_time = df_time.groupby('time').agg(dict.fromkeys(values, sum))   #sum the value with the same time

        return df_time

    # get job queue length for the cluster in the format of time, jobQueueLength, requestedCPUs
    def getClusterJobQueue (self, start='', stop='', qTime=0):
        #job_db_inx,mod_time,deleted,account,admin_comment,array_task_str,array_max_tasks,array_task_pending,cpus_req,derived_ec,derived_es,exit_code,job_name,id_assoc,id_array_job,id_array_task,id_block,id_job,id_qos,id_resv,id_wckey,id_user,id_group,pack_job_id,pack_job_offset,kill_requid,mcs_label,mem_req,nodelist,nodes_alloc,node_inx,partition,priority,state,timelimit,time_submit,time_eligible,time_start,time_end,time_suspended,gres_req,gres_alloc,gres_used,wckey,work_dir,track_steps,tres_alloc,tres_req
        # index is id_job

        #df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc'])
        df            = self.getJobTable()
        start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        # Normally, a job enter the queue at time_eligible and leave the queue at time_start
        df               = df[(df['time_eligible'] > 0) & (df['time_eligible'] < stop)]
        # reppared for counting
        df['inc_count']  = 1
        df['dec_count']  = -1
        df['inc_count2'] = df['cpus_req']
        df['dec_count2'] = -df['cpus_req']

        # Count queued job that 1) leave queue to start with time_start>0, 
        #                       2) leave queue without start with time_start=0 and time_end>0 (mostly cancelled)
        #                       3) not leave queue with time_start=0 and time_end=0
        df            = df[(df['time_eligible'] < df['time_start']-1-qTime) | ((df['time_start']==0) & (df['time_end']==0)) | ((df['time_start']==0) & (df['time_eligible'] < df['time_end']-1-qTime))]   # as the unit is in sec, +1 to remove accuracy issue,
        #print ('getClusterJobQueue {} {} df=\n{}'.format(start, stop, df))

        # counting
        df_t1           = self.getTimeIndexValue(df,                     'time_eligible', ['inc_count', 'inc_count2'])
        #print ('getClusterJobQueue df_t1={}'.format(df_t1))
        df_t2           = self.getTimeIndexValue(df[df['time_start']>0], 'time_start',    ['dec_count', 'dec_count2'])
        #print ('getClusterJobQueue df_t2={}'.format(df_t2))
        df_t3           = self.getTimeIndexValue(df[(df['time_start']==0) & (df['time_end']>0)], 'time_end',    ['dec_count', 'dec_count2'])
        #print ('getClusterJobQueue df_t3={}'.format(df_t3))
        df_time         = df_t1.add(df_t2, fill_value=0).add(df_t3, fill_value=0)
        df_time         = df_time.sort_index().cumsum()
        #print ('getClusterJobQueue df={}'.format(df_time))

        return start, stop, df_time.reset_index()

    # not used by anybody now
    # get jobs information as eligible_time, start_time, id_job, id_user, account, cpus_req, nodes_alloc
    def getJobsName (self, start='', stop=''):
        # index is id_job
        df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','job_name', 'id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc', 'tres_req'],index_col=3)
        start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        df['count']   = 1
        dfg           = df.groupby('job_name')

        # sort by the count of job_name
        # group more by 

        return df

    def getQoS (self):
        # index is id_job
        df = pd.read_csv("qos_table.csv",usecols=['deleted','id','name','max_tres_pu','max_wall_duration_per_job','grp_tres','preempt','preempt_mode','priority','usage_factor'], index_col=1)
        df = df[df['deleted']==0]

        return df
        
    def getPartitions (self):
        partitions = pyslurm.partition().get()
        #{'scc': {u'billing_weights_str': None, u'def_mem_per_cpu': None, u'max_mem_per_cpu': None, u'allow_alloc_nodes': u'ALL', u'default_time_str': u'NONE', u'total_nodes': 360, u'min_nodes': 1, u'deny_qos': None, u'preempt_mode': u'OFF', u'allow_qos': [u'gen', u'scc'], u'max_nodes': u'UNLIMITED', u'deny_accounts': None, u'alternate': None, u'state': 'UP', u'priority_job_factor': 1, u'nodes': u'worker[1000-1239,3000-3119]', u'total_cpus': 11520, u'max_mem_per_node': u'UNLIMITED', u'tres_fmt_str': u'cpu=11520,mem=210000G,node=360', u'default_time': u'NONE', u'qos_char': u'scc', u'allow_accounts': u'ALL', u'max_cpus_per_node': u'UNLIMITED', u'max_share': 0, u'max_time_str': u'UNLIMITED', u'def_mem_per_node': u'UNLIMITED', u'name': u'scc', u'grace_time': 0, u'flags': {u'RootOnly': 0, u'Default': 0, u'DisableRootJobs': 0, u'ExclusiveUser': 0, u'LLN': 0, u'Shared': u'EXCLUSIVE', u'Hidden': 0}, u'max_time': u'UNLIMITED', u'cr_type': 0, u'allow_groups': [u'scc'], u'priority_tier': 1, u'over_time_limit': 0}, ...}
        #allow_groups, allow_accounts

        return paritions

    def getNodes (self):
        nodes = pyslurm.node().get()
        #{'worker0001': {u'features': u'ib', u'weight': 1, u'energy': {u'current_watts': 0, u'consumed_energy': 0, u'base_watts': 0, u'previous_consumed_energy': 0, u'base_consumed_energy': 0}, u'cpus': 44, u'cpu_spec_list': [], u'owner': None, u'gres_drain': u'N/A', u'real_memory': 384000L, u'tmp_disk': 0, u'slurmd_start_time': 1544199176, u'features_active': u'ib', u'reason_time': None, u'mcs_label': None, u'sockets': 2, u'alloc_mem': 0L, u'state': u'REBOOT*', u'version': None, u'node_hostname': u'worker0001', u'partitions': [], u'power_mgmt': {u'cap_watts': None}, u'core_spec_cnt': 0, u'err_cpus': 0, u'reason': None, u'alloc_cpus': 0, u'threads': 1, u'boot_time': 1544198661, u'cores': 22, u'reason_uid': 0, u'node_addr': u'worker0001', u'arch': u'x86_64', u'name': u'worker0001', u'boards': 1, u'gres': [], u'free_mem': None, u'tres_fmt_str': u'cpu=44,mem=375G', u'gres_used': [u'gpu:0', u'mic:0'], u'mem_spec_limit': 0L, u'os': u'Linux', u'cpu_load': 2}, ...}

        return nodes

    #return jobs that run on node during [start, stop]
    def getNodeRunJobs (self, node, start, stop):
        df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['id_job','id_user','nodelist','nodes_alloc','state','time_start','time_end','time_suspended'])
        #df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['id_job','id_user','nodelist','nodes_alloc','state','time_start','time_end','time_suspended'],index_col=0)
        start,stop,df = MyTool.getDFBetween (df, 'time_start', start, stop)
        df            = df[df['nodes_alloc'] > 0]

        #jobs running on node
        if node:
           criterion      = df['nodelist'].map(lambda x: node in MyTool.nl2flat(x))
           df             = df[criterion]
           df['user']     = df['id_user'].map(lambda x: MyTool.getUser(x))

        return df[['id_job', 'user', 'time_start', 'time_end', 'time_suspended']]

    # return jobs with the given job_name
    # add fields user, duration
    def getJobByName (self, job_name, fields=['id_job','job_name', 'id_user','state', 'nodes_alloc','nodelist', 'time_start','time_end', 'tres_req', 'gres_req']):
        df             = pd.read_csv("slurm_cluster_job_table.csv",usecols=fields)
        df             = df[df['job_name']==job_name]
        df['state']    = df['state'].map(lambda x: SLURM_STATE_DICT.get(x, x))
        df['user']     = df['id_user'].map(lambda x: MyTool.getUser(x))
        df['duration'] = df['time_end'] - df['time_start']
        df['duration'] = df['duration'].map(lambda x: x if x >0 else 0)
        df             = df.fillna('Not Defined')
        lst            = df.to_dict(orient='records')
        return lst

    # return jobs after the given start time
    def getJobByStartTime (self, start_time, fields=['id_job','job_name', 'id_user','state', 'nodes_alloc','nodelist', 'time_start','time_end', 'tres_req', 'gres_req']):
        df             = pd.read_csv("slurm_cluster_job_table.csv",usecols=fields)
        df             = df[df['time_start']>start_time]
        lst            = df.to_dict(orient='records')
        return lst
    
class SlurmCmdQuery:
    LOCAL_TZ = timezone(timedelta(hours=-4))

    def __init__(self):
        pass

    @staticmethod
    def sacct_getUserJobReport (user, days=3, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End', skipJobStep=True):
        return SlurmCmdQuery.sacct_getReport(['-u', user], days, output, skipJobStep)

    @staticmethod
    def sacct_getJobReport (jobid, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End,AllocNodes,NodeList'):
        jobs = SlurmCmdQuery.sacct_getReport(['-j', str(jobid)], days=None, output=output, skipJobStep=False)
        if not jobs:
           return None
        job  = jobs[0]
        job['Start']      = int(MyTool.getSlurmTimeStamp(job['Start']))
        job['NodeList']   = MyTool.nl2flat(job['NodeList'])
        job['AllocNodes'] = int(job['AllocNodes'])
        job['AllocCPUS']  = int(job['AllocCPUS'])
        return job

    # return {jid:jinfo, ...}
    @staticmethod
    def sacct_getReport (criteria, days=3, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End', skipJobStep=True):
        #print('sacct_getReport {} {} {}'.format(criteria, days, skipJobStep))
        if days:
           t = date.today() + timedelta(days=-days)
           startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
           criteria  = ['-S', startDate] + criteria

        sacct_str = SlurmCmdQuery.sacctCmd (criteria, output)
        jobs      = []
        for line in sacct_str.splitlines():
            if not line: continue
            ff = line.split(sep='|')
            if (skipJobStep and '.' in ff[0]): continue # indicates a job step --- under what circumstances should these be broken out?
            if ( '.' in ff[0] ):
               ff0 = ff[0].split(sep='.')[0]
            else:
               ff0 = ff[0]

            f0p = ff0.split(sep='_')
            try:
                jid, aId = int(f0p[0]), int(f0p[1])
            except:
                jid, aId = int(f0p[0]), -1
            if ff[3].startswith('CANCELLED by '):
                uid   = ff[3].rsplit(' ', 1)[1]
                uname = MyTool.getUser(uid)
                ff[3] = '%s (%s)'%(ff[3], uname)
            keys = output.split(sep=',')
            job = dict(zip(keys, ff))
            job['job_id'] = job['JobID']
            jobs.append(job)

        return jobs

    @staticmethod
    def sacctCmd (criteria, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'):
        cmd = ['sacct', '-n', '-P', '-o', output] + criteria
        print('{}'.format(cmd)) 
        try:
            #TODO: capture standard error separately?
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        return d.decode('utf-8')

    # given a jid, return [jid, uid, start_time, end_time]
    # if end_time is unknown, return current time
    # if start_time is unknown, error
    def getSlurmJobInfo (self, jid): 
        cmd = ['sacct', '-n', '-P', '-X', '-j', str(jid), '-o', 'JobID,User,NodeList,Start,End']
        try:
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" ERROR: returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        d=d.decode('utf-8')
        print (d)
        info = d.rstrip().split('|')
        print (repr(info))
        if info[4]!='Unknown':
           return [info[0], MyTool.getUid(info[1]), MyTool.convert2list(info[2]), MyTool.getSlurmTimeStamp(info[3]), MyTool.getSlurmTimeStamp(info[4])]
        else:
           return [info[0], MyTool.getUid(info[1]), MyTool.convert2list(info[2]), MyTool.getSlurmTimeStamp(info[3]), time.time()]
        
    def getSlurmJobNodes (self, jid):
        pass
        
    def getDictIntValue (self, point, name):
        pass
           
    def getSlurmNodeMon (self, hostname, uid, start_time, end_time):
        pass

    #return [[...],[...],[...]], each of which
    #[{'node':nodename, 'data':[[timestamp, value]...]} ...]
    def getSlurmJobMonData(self, jid, uid, nodelist, start_time, stop_time):
        pass

    #return [[...],[...],[...]], each of which
    #[{'node':nodename, 'data':[[timestamp, value]...]} ...]
    def getSlurmUidMonData(self, uid, nodelist, start_time, stop_time):
        pass

def test1():
    client = SlurmCmdQuery()
    info = client.getSlurmJobInfo('110972')

def test2():
    client = SlurmDBQuery()
    info = client.getClusterJobQueue()

def test3():
    client = SlurmDBQuery()
    client.getJobByName ('script.sh')

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
    test3()
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
