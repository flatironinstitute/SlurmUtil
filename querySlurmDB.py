#!/usr/bin/env python00

import time
t1=time.time()
import os,re,subprocess
import pandas
import MyTool
from datetime import datetime, date, timedelta

SLURM_STATE_DICT = {0:'PENDING', 1:'RUNNING', 3:'COMPLETED', 4:'CANCELED', 5:'FAILED', 6:'TIMEOUT', 7:'NODE_FAIL', 8:'PREEMPTED', 11:'OUT_OF_MEM', 8192:'RESIZING'}
CSV_DIR          = "/mnt/home/yliu/projects/slurm/utils/data/"

class SlurmDBQuery:
    def __init__(self):
        self.job_df    = None
        self.job_df_ts = 0
  
    def updateDB (self):
        subprocess.call('../mysqldump.sh')

    def getJobTable (self):
        #read file time, if updated from last time, reset the value
        f_name = CSV_DIR + "slurm_cluster_job_table.csv"
        m_time = os.stat(f_name).st_mtime
        if m_time > self.job_df_ts:
           self.job_df    = pandas.read_csv(f_name,usecols=['cpus_req','id_job','state', 'time_submit', 'time_eligible', 'time_start', 'time_end'])
           self.job_df_ts = m_time

        return self.job_df
        
    def getClusterUsage_hourly(start, stop):
        #read from csv, TODO: deleted=0 for all data now
        df               = pandas.read_csv(CSV_DIR + "slurm_cluster_usage_hour_table.csv", usecols=['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'])
        start, stop, df  = self.getDFBetween(df, 'time_start', start, stop)
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        df               = df[df['count'] * 3600 == df['total_secs']]
        df['tdown_secs'] = df['down_secs']+df['pdown_secs']
        #df               = df[df['alloc_secs']>0]

        return start, stop, df

    # daily.sh update the data daily 
    def getAccountUsage_hourly (start='', stop=''):
        #cluster usage
        df         = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_usage_hour_table.csv", usecols=['id','id_tres','time_start','alloc_secs'])
        st, stp, df= MyTool.getDFBetween (df, 'time_start', start, stop)

        # get account's data, id_assoc (user) - account
        userDf     = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_table.csv", usecols=['id_assoc','acct'], index_col=0)
        # add acct to df
        df['acct'] = df['id'].map(userDf['acct'])
        df.drop('id', axis=1, inplace=True)

        # sum over the same id_tres, acct, time_start
        sumDf       = df.groupby(['id_tres','acct', 'time_start']).sum()
        sumDf['ts'] = sumDf.index.get_level_values('time_start') * 1000

        return st, stp, sumDf

    def getUserReport_hourly(self, start='', stop='', top=5, account=None):
        # get top 5 user for each resource
        df     = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_usage_day_table.csv", usecols=['id','id_tres', 'alloc_secs', 'time_start'], dtype={'time_start':int})
        st, stp, df     = MyTool.getDFBetween (df, 'time_start', start, stop)     #constrain by time
        sumDf  = df.groupby(['id_tres','id']).sum()                               #sum over user
        userDf = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_table.csv",            usecols=['id_assoc','user','acct'], index_col=0)
        sumDf  = sumDf.join(userDf, on='id')
        if account:
           sumDf = sumDf[sumDf['acct']==account]
        cpuIdx = sumDf.loc[(1,)].nlargest(top, 'alloc_secs').index
        memIdx = sumDf.loc[(2,)].nlargest(top, 'alloc_secs').index
        nodeIdx= sumDf.loc[(4,)].nlargest(top, 'alloc_secs').index
        #topIdx = cpuIdx.union(memIdx).union(nodeIdx)

        df     = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_usage_hour_table.csv", usecols=['id','id_tres','time_start','alloc_secs'])
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
                tresSer[tres].append({'data': topLst, 'name': userDf.loc[uid,'user']+"("+userDf.loc[uid,'acct']+")"})

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

        #df            = pandas.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc'])
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
        df            = pandas.read_csv(CSV_DIR + "slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','job_name', 'id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc', 'tres_req'],index_col=3)
        start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        df['count']   = 1
        dfg           = df.groupby('job_name')

        # sort by the count of job_name
        # group more by 

        return df

    def getQoS (self):
        # index is id_job
        df = pandas.read_csv(CSV_DIR + "qos_table.csv",usecols=['deleted','id','name','max_tres_pu','max_wall_duration_per_job','grp_tres','preempt','preempt_mode','priority','usage_factor'], index_col=1)
        df = df[df['deleted']==0]

        return df
        
    #return jobs that run on node during [start, stop]
    def getNodeRunJobs (self, node, start, stop):
        df            = pandas.read_csv(CSV_DIR + "slurm_cluster_job_table.csv",usecols=['id_job','id_user','nodelist','nodes_alloc','state','time_start','time_end','time_suspended'])
        #df            = pandas.read_csv("slurm_cluster_job_table.csv",usecols=['id_job','id_user','nodelist','nodes_alloc','state','time_start','time_end','time_suspended'],index_col=0)
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
        df             = pandas.read_csv(CSV_DIR + "slurm_cluster_job_table.csv",usecols=fields)
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
        df             = pandas.read_csv(CSV_DIR + "slurm_cluster_job_table.csv",usecols=fields)
        df             = df[df['time_start']>start_time]
        lst            = df.to_dict(orient='records')
        return lst

    #sav last yeasrs' cpu usage data to cpuAllocDF.csv
    def savCPUAlloc (output_file, years=2):
        #TODO: add dtype{'id_tres':int, ...} to save memory
        df               = pandas.read_csv(CSV_DIR +"slurm_cluster_usage_hour_table.csv", usecols=['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'])
        # get 2 years's history only
        now              = datetime.now()
        start            = int(now.replace(year=now.year-years).timestamp())      # use 2 years' history
        start,stop,df    = MyTool.getDFBetween(df, 'time_start', start)
        # calculate new columns 
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        df               = df[df['count'] * 3600 == df['total_secs']]         # filter
        df['ds']         = df['time_start'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        #df.reset_index (drop=True, inplace=True)
        # remove 0 alloc_secs at the beginning 
        #idx              = 0
        #while df.loc[idx,'alloc_secs'] == 0: idx += 1
        #if idx:
        #   df.drop(range(0, idx), inplace=True)
        #   df.reset_index (drop=True, inplace=True)

        #save data to cpuAllocDF.sav
        #sav cpu data only
        dfg              = df.groupby  ('id_tres')
        df               = dfg.get_group(1)      #cpu's id_tres is 1
        cpuAllocDf = df[['ds', 'alloc_secs', 'total_secs']]
        cpuAllocDf = cpuAllocDf.rename(columns={'alloc_secs': 'y', 'total_secs': 'cap'})
        cpuAllocDf.to_csv(output_file, index=False)

    def savAccountCPUAlloc (output_file, years=2):
        # read data
        df         = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_usage_hour_table.csv", usecols=['id','id_tres','time_start','alloc_secs'])
        # get 2 years's history only
        now        = datetime.now()
        start      = int(now.replace(year=now.year-years).timestamp())      # use 2 years' history
        st, stp, df= MyTool.getDFBetween (df, 'time_start', start)
        # add acct to df
        userDf     = pandas.read_csv(CSV_DIR + "slurm_cluster_assoc_table.csv", usecols=['id_assoc','acct'], index_col=0)
        df['acct'] = df['id'].map(userDf['acct'])
        df.drop('id', axis=1, inplace=True)
        # sum over the same id_tres, acct, time_start
        df         = df.groupby(['id_tres','acct', 'time_start']).sum()
        df['ts']   = df.index.get_level_values('time_start')

        #sav data
        df['ds']   = df['ts'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        #TODO: hard code account name
        for acct in ['cca', 'ccb', 'ccm', 'ccn', 'ccq']:
            # get cpu,account data
            cpuDf             = df.loc[(1,acct,),]      # TODO: 1 is cpu
            cpuAllocDf        = cpuDf[['ds', 'alloc_secs']]
            cpuAllocDf        = cpuAllocDf.rename(columns={'alloc_secs': 'y'})
            cpuAllocDf.to_csv(output_file.format(acct), index=False)
    
class SlurmCmdQuery:
    #DF_ASSOC   = pandas.read_csv ("sacctmgr_assoc.csv", sep='|')
    #DICT_QOS   = DF_ASSOC.set_index("User").to_dict()['QOS']   # {User:QOS}
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
        if file_ts > SlurmCmdQuery.TS_ASSOC:
           #SlurmCmdQuery.DF_ASSOC   = pandas.read_csv ("sacctmgr_assoc.csv", sep='|')
           #SlurmCmdQuery.DICT_QOS   = SlurmCmdQuery.DF_ASSOC.set_index("User").to_dict()['QOS']   # {User:QOS}
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

def test2():
    client = SlurmDBQuery()
    info = client.getClusterJobQueue()

def test3():
    client = SlurmDBQuery()
    client.getJobByName ('script.sh')

def main():
    t1=time.time()

if __name__=="__main__":
   main()
