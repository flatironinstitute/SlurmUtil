#!/usr/bin/env python00

import time
t1=time.time()
from datetime import datetime, timezone, timedelta
import subprocess
import pyslurm
import MyTool
import pandas as pd

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
        pass
  
    def updatDB (self):
        subprocess.call('../mysqldump.sh')

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
        #df_time = df[[time_col, value_col]]
        #df_time.columns = ['time', 'value']
        #df_time = df_time.groupby('time').agg({'value': sum})   #sum the value with the same time

        df_time = df[[time_col] +  value_cols]
        values  = ['value' + str(i) for i in range(len(value_cols))]
        df_time.columns = ['time'] + values
        df_time = df_time.groupby('time').agg(dict.fromkeys(values, sum))   #sum the value with the same time

        return df_time

    # get job queue length for the cluster in the format of time, jobQueueLength, requestedCPUs
    def getClusterJobQueue (self, start='', stop='', qTime=0):
        # index is id_job
        df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc'],index_col=2)
        start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        df               = df[(df['time_eligible'] > 0) & (df['time_eligible'] < stop)]
        # reppared for counting
        df['inc_count']  = 1
        df['dec_count']  = -1
        df['inc_count2'] = df['cpus_req']
        df['dec_count2'] = -df['cpus_req']

        # there is 3 way to stay in a queue after enter at time_eligible 1) leave at time_start, 2) stay with time_start=0, 3) leave at time_end (mostly cancelled)
        df            = df[(df['time_eligible'] < df['time_start']-1-qTime) | ((df['time_start']==0) & (df['time_end']==0)) | ((df['time_start']==0) & (df['time_eligible'] < df['time_end']-1-qTime))]   # as the unit is in sec, +1 to remove accuracy issue,
        #df            = df[(df['time_eligible'] < df['time_start']-1-qTime)]

        # counting
        df_t1           = self.getTimeIndexValue(df,                     'time_eligible', ['inc_count', 'inc_count2'])
        df_t2           = self.getTimeIndexValue(df[df['time_start']>0], 'time_start',    ['dec_count', 'dec_count2'])
        df_t3           = self.getTimeIndexValue(df[(df['time_start']==0) & (df['time_end']>0)], 'time_end',    ['dec_count', 'dec_count2'])
        df_time         = df_t1.add(df_t2, fill_value=0).add(df_t3, fill_value=0)
        df_time         = df_time.sort_index().cumsum()

        return start, stop, df_time.reset_index()

    # get jobs information as eligible_time, start_time, id_job, id_user, account, cpus_req, nodes_alloc
    def getClusterRunJobs (self, start='', stop=''):
        # index is id_job
        df            = pd.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc', 'tres_req'],index_col=2)
        start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        df               = df[(df['time_start'] > 0) & (df['time_start'] < stop)]
        # reppared for counting
        df['inc_count']  = 1
        df['dec_count']  = -1
        df['inc_count2'] = df['cpus_req']
        df['dec_count2'] = -df['cpus_req']

        # there is 2 way to count executtion 1) stop at time_end, 2) still on with time_end=0,
        df            = df[(df['time_start'] < df['time_end']) | (df['time_end']==0)]

        # counting
        df_t1           = self.getTimeIndexValue(df,                   'time_start', ['inc_count', 'inc_count2'])
        df_t2           = self.getTimeIndexValue(df[df['time_end']>0], 'time_end',   ['dec_count', 'dec_count2'])
        df_time         = df_t1.add(df_t2, fill_value=0)
        df_time         = df_time.sort_index().cumsum()

        return start, stop, df_time.reset_index()

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

class SlurmCmdQuery:
    LOCAL_TZ = timezone(timedelta(hours=-4))

    def __init__(self):
        pass

    def sacctCmd (self, criteria, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'):
        #cmd = ['sacct', '-a', '-n', '-P', '-o', 'JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'] + criteria
        cmd = ['sacct', '-n', '-P', '-o', output] + criteria
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

def main():
    #job= pyslurm.job().find_id ('88318')
    #job= pyslurm.slurmdb_jobs().get ()
    #print(repr(job))
    t1=time.time()
    #print(SlurmStatus.getStatusID('running'))
    #print(SlurmStatus.getStatusID('running1'))
    #print(SlurmStatus.getStatus(1))
    client = SlurmDBQuery()
    #info = client.getSlurmJobInfo('105179')
    #info = client.test()
    info = client.getClusterJobQueue()
    print(repr(info))
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
