#!/usr/bin/env python00

import time
t1=time.time()
import math,os,re,shutil,subprocess
import pandas
import config,MyTool
from datetime import datetime, date, timedelta

logger           = config.logger
SLURM_STATE_DICT = {0:'PENDING', 1:'RUNNING', 3:'COMPLETED', 4:'CANCELED', 5:'FAILED', 6:'TIMEOUT', 7:'NODE_FAIL', 8:'PREEMPTED', 11:'OUT_OF_MEM', 8192:'RESIZING'}
CSV_DIR          = "/mnt/home/yliu/projects/slurm/utils/data/"
CEPH_DIR         = "/mnt/home/yliu/ceph/projects/slurm/utils/"
LOG_BUCKET       = {10.0:10000000000} 
for i in range(1,10):
    for j in range(0,10):
        f        = (i*10+j)/10       
        LOG_BUCKET[f]=round(math.pow(10,f))

#truncate csv file, removing lines with create_time bigger than ts
def truncUsageFile (inFile, outFile, ts):
    print("Truncate {} file and save to {}".format(inFile, outFile))
    #creation_time    mod_time  deleted  id_tres  time_start  ...  down_secs  pdown_secs  idle_secs  resv_secs  over_secs
    df     = pandas.read_csv(inFile)
    df     = df[df['creation_time']<ts]
    df.to_csv(outFile, index=False)

class SlurmDBQuery:
    def __init__(self):
        self.jobTable      = {}       #{cluster:{ts:, df:}}
  
    def updateDB (self):
        subprocess.call('./mysqldump.sh')

    #index is id_job, seems that saving jobTable does not save much time
    def getJobTable (self, cluster, fld_lst=['cpus_req','state', 'time_submit', 'time_eligible', 'time_start', 'time_end']):
        #read file time, if updated from last time, reset the value
        f_name = "{}/{}_{}".format(CSV_DIR, cluster, "job_table.csv")
        m_time = os.stat(f_name).st_mtime
        if (cluster not in self.jobTable) or (m_time > self.jobTable[cluster]['ts']):
           self.jobTable[cluster]       = {'ts': m_time}
           self.jobTable[cluster]['df'] = pandas.read_csv(f_name,usecols=['account', 'cpus_req', 'id_job', 'id_qos','id_user', 'nodes_alloc', 'state', 'time_submit', 'time_eligible', 'time_start', 'time_end', 'tres_alloc'], index_col=2)

        return self.jobTable[cluster]['df'][fld_lst]
        
    def readJobTable (cluster, start=None, stop=None, fld_lst=None, index_col=None):
        #read file time, if updated from last time, reset the value
        f_name        = "{}/{}_{}".format(CSV_DIR, cluster, "job_table.csv")
        df            = pandas.read_csv(f_name, usecols=fld_lst, index_col=index_col)
        if start or stop:
           start,stop,df = MyTool.getDFBetween (df, 'time_submit', start, stop)

        return start, stop, df

    def getClusterUsage_hourly(cluster, start, stop):
        #read from csv, TODO: deleted=0 for all data now
        fname            = "{}/{}_{}".format(CSV_DIR, cluster, "usage_hour_table.csv")
        df               = pandas.read_csv(fname, usecols=['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'])
        start, stop, df  = MyTool.getDFBetween(df, 'time_start', start, stop)
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        df['tdown_secs'] = df['down_secs'] +df['pdown_secs']
        df               = df[df['count'] * 3600 == df['total_secs']]      # count =? count of cores
        df['ts_ms']      = df['time_start'] * 1000
        dfg              = df.groupby  ('id_tres')
 
        cpuDf            = dfg.get_group(1)
        memDf            = dfg.get_group(2)
        #eneDf            = dfg.get_group(3)
        #nodeDf     = dfg.get_group(4)  not available

        return start, stop, cpuDf, memDf

    # daily.sh update the data daily 
    def getAccountUsage_hourly (cluster, start='', stop=''):
        #cluster usage
        fname      = "{}/{}_{}".format(CSV_DIR, cluster, "assoc_usage_hour_table.csv")
        df         = pandas.read_csv(fname, usecols=['id','id_tres','time_start','alloc_secs'])
        st, stp, df= MyTool.getDFBetween (df, 'time_start', start, stop)

        # get account's data, id_assoc (user) - account
        fname1     = "{}/{}_{}".format(CSV_DIR, cluster, "assoc_table.csv")
        userDf     = pandas.read_csv(fname1, usecols=['id_assoc','acct'], index_col=0)
        # add acct to df
        df['acct'] = df['id'].map(userDf['acct'])
        df.drop('id', axis=1, inplace=True)

        # sum over the same id_tres, acct, time_start
        sumDf          = df.groupby(['id_tres','acct', 'time_start']).sum()
        sumDf['ts_ms'] = sumDf.index.get_level_values('time_start') * 1000
        sumDf['alloc_ratio'] = sumDf['alloc_secs']/3600     #1 sec on node1 and 1 sec on node2 =? 2/3600 node  

        return st, stp, sumDf

    def getUserReport_hourly(cluster, start='', stop='', top=5, account=None):
        # get top 5 user for each resource
        fname       = "{}/{}_{}".format(CSV_DIR, cluster, "assoc_usage_day_table.csv")
        df          = pandas.read_csv(fname,  usecols=['id','id_tres', 'alloc_secs', 'time_start'], dtype={'time_start':int})
        st, stp, df = MyTool.getDFBetween (df, 'time_start', start, stop)     #constrain by time
        sumDf       = df.groupby(['id_tres','id']).sum()                               #sum over user
        fname1      = "{}/{}_{}".format(CSV_DIR, cluster, "assoc_table.csv")
        userDf      = pandas.read_csv(fname1, usecols=['id_assoc','user','acct'], index_col=0)
        sumDf       = sumDf.join(userDf, on='id')
        if account:
           sumDf    = sumDf[sumDf['acct']==account]
        cpuIdx      = sumDf.loc[(1,)].nlargest(top, 'alloc_secs').index
        memIdx      = sumDf.loc[(2,)].nlargest(top, 'alloc_secs').index
        nodeIdx     = sumDf.loc[(4,)].nlargest(top, 'alloc_secs').index
        #topIdx = cpuIdx.union(memIdx).union(nodeIdx)

        fname2      = "{}/{}_{}".format(CSV_DIR, cluster, "assoc_usage_hour_table.csv")
        df          = pandas.read_csv(fname2, usecols=['id','id_tres','time_start','alloc_secs'])
        st, stp, df = MyTool.getDFBetween (df, 'time_start', start, stop)
        # get top users data only
        dfg         = df.groupby(['id_tres','id'])
        tresSer     = {1:[],     2:[],     4:[]} # {1: [{'data': [[ms,value],...], 'name': uid},...], 2:...} 
        idxSer      = {1:cpuIdx, 2:memIdx, 4:nodeIdx}
        for tres in [1,2,4]:
            for uid in idxSer[tres]:
                topDf                = dfg.get_group((tres,uid))
                topDf['ts_ms']       = topDf['time_start'] * 1000
                topDf['alloc_ratio'] = topDf['alloc_secs'] / 3600
                topLst               = topDf[['ts_ms','alloc_ratio']].values.tolist()
                tresSer[tres].append({'data': topLst, 'name': userDf.loc[uid,'user']+"("+userDf.loc[uid,'acct']+")"})

        return st,stp,tresSer
    
    # return time_col, value_cols
    def getTimeIndexValue (self, df, time_col, value_cols):
        df_time = df[[time_col] +  value_cols]
        values  = ['value' + str(i) for i in range(len(value_cols))]
        df_time.columns = ['time'] + values
        df_time = df_time.groupby('time').agg(dict.fromkeys(values, sum))   #sum the value with the same time

        return df_time

    # get job queue length for the cluster in the format of time, jobQueueLength, requestedCPUs
    def getClusterJobQueue (self, cluster, start='', stop='', qTime=0):
        #job_db_inx,mod_time,deleted,account,admin_comment,array_task_str,array_max_tasks,array_task_pending,cpus_req,derived_ec,derived_es,exit_code,job_name,id_assoc,id_array_job,id_array_task,id_block,id_job,id_qos,id_resv,id_wckey,id_user,id_group,pack_job_id,pack_job_offset,kill_requid,mcs_label,mem_req,nodelist,nodes_alloc,node_inx,partition,priority,state,timelimit,time_submit,time_eligible,time_start,time_end,time_suspended,gres_req,gres_alloc,gres_used,wckey,work_dir,track_steps,tres_alloc,tres_req
        # index is id_job

        df            = self.getJobTable(cluster)
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
        df_t2           = self.getTimeIndexValue(df[df['time_start']>0], 'time_start',    ['dec_count', 'dec_count2'])
        df_t3           = self.getTimeIndexValue(df[(df['time_start']==0) & (df['time_end']>0)], 'time_end',    ['dec_count', 'dec_count2'])
        df_time         = df_t1.add(df_t2, fill_value=0).add(df_t3, fill_value=0)
        df_time         = df_time.sort_index().cumsum()
        #print ('getClusterJobQueue df={}'.format(df_time))

        return start, stop, df_time.reset_index()

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

    def getJobByName (job_name, fields=['id_job','job_name', 'id_user','state', 'nodes_alloc','nodelist', 'time_start','time_end', 'tres_req', 'gres_req']):
        lst = []    # TODO: in time order
        for cluster in ['slurm_cluster', 'slurm']: 
            lst.extend(SlurmDBQuery.getJobByName_cluster (job_name, cluster, fields))

        return lst

    # return jobs with the given job_name
    # add fields user, duration
    def getJobByName_cluster (job_name, cluster, fields):
        start,stop,df  = SlurmDBQuery.readJobTable(cluster, fld_lst=fields)
        df             = df[df['job_name']==job_name]
        df['state']    = df['state'].map  (lambda x: SLURM_STATE_DICT.get(x, x))
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

    def readClusterTable (cluster, part_table_name, fld_lst, index_col=None):
        f_name        = "{}/{}_{}.csv".format(CSV_DIR, cluster, part_table_name)
        df            = pandas.read_csv(f_name, usecols=fld_lst, index_col=index_col)
        return df

    def readClusterTableBetween (cluster, part_table_name, fld_lst, start=None, stop=None, index_col=None, ts_col=None):
        df  = SlurmDBQuery.readClusterTable (cluster, part_table_name, fld_lst, index_col)
        if ts_col:
           start,stop,df = MyTool.getDFBetween (df, ts_col, start, stop)
           return start, stop, df
        else:
           return 0,0,df

    #sav last yeasrs' cpu usage data to cpuAllocDF.csv
    def savCPUAlloc (cluster, day_or_hour, output_file):
        #maybe add dtype{'id_tres':int, ...} to save memory
        now              = datetime.now()
        #start            = int(now.replace(year=now.year-years).timestamp())      # use 2 years' history
        fld              = ['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs']
        part_table_nm    = "usage_{}_table".format(day_or_hour)
        df               = SlurmDBQuery.readClusterTable(cluster, part_table_nm, fld)

        #sav cpu data only
        dfg        = df.groupby  ('id_tres')
        df         = dfg.get_group(1)      #cpu's id_tres is 1

        # calculate new columns 
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        if day_or_hour == 'day':
           df_warning    = df[df['count'] * 3600*24 != df['total_secs']]
           print("filter out count * 3600 * 24 != total_secs\n{}".format(df_warning))
           df            = df[df['count'] * 3600*24 == df['total_secs']]         # filter, cause problems later for NaN in savAccountCPUAlloc
                                                                                 # the situation may due to add/remove new machines in cluster
        elif day_or_hour == 'hour':
           df_warning    = df[df['count'] * 3600 != df['total_secs']]
           print("filter out count * 3600 != total_secs\n{}".format(df_warning))
           df            = df[df['count'] * 3600 == df['total_secs']]         # filter
        df['ds']         = df['time_start'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

        cpuAllocDf = df[['ds', 'alloc_secs', 'total_secs']]
        cpuAllocDf = cpuAllocDf.rename(columns={'alloc_secs': 'y', 'total_secs': 'cap'})
        cpuAllocDf.to_csv(output_file, index=False)
        return cpuAllocDf.iat[0,0], cpuAllocDf.iat[-1,0], cpuAllocDf

    # use clusterDf to set cap
    def savAccountCPUAlloc (cluster, day_or_hour, output_file, clusterDf=None):
        # read data
        now              = datetime.now()
        #start            = int(now.replace(year=now.year-years).timestamp())      # use 2 years' history
        part_table_nm    = "assoc_usage_{}_table".format(day_or_hour)
        df               = SlurmDBQuery.readClusterTable (cluster, part_table_nm, ['id', 'id_tres','time_start','alloc_secs'])
        start, stop      = df.iat[0,2], df.iat[-1,2]

        # add acct to df
        userDf     = SlurmDBQuery.readClusterTable (cluster, "assoc_table", ['id_assoc','acct'], index_col=0)
        df['acct'] = df['id'].map(userDf['acct'])
        df.drop('id', axis=1, inplace=True)
        # sum over the same id_tres, acct, time_start
        df         = df.groupby(['id_tres','acct', 'time_start']).sum()
        df['ts']   = df.index.get_level_values('time_start')
        df['ds']   = df['ts'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

        #sav data
        idx_acct   = df.index.get_level_values('acct').unique()
        for acct in idx_acct.values:   #['cca', 'ccb', 'ccm', 'ccn', 'ccq']:
            # get cpu,account data
            cpuDf             = df.loc[(1,acct,),]      # TODO: 1 is cpu
            cpuDf             = cpuDf.join(clusterDf.set_index('ds'),on='ds',how='inner')
            cpuAllocDf        = cpuDf[['ds', 'alloc_secs', 'cap']]
            cpuAllocDf        = cpuAllocDf.fillna (cpuAllocDf.iat[len(cpuAllocDf)-1,2])  #fill nan cell, should be non with inner
            cpuAllocDf['cap'] = cpuAllocDf['cap'].astype(int)
            cpuAllocDf        = cpuAllocDf.rename(columns={'alloc_secs': 'y'})
            cpuAllocDf.to_csv(output_file.format(acct), na_rep=cpuAllocDf.iat[len(cpuAllocDf)-1,2], index=False)
            print ("\t{}: {}-{}".format(acct, cpuAllocDf.iat[0,0], cpuAllocDf.iat[-1,0])) 
        return 
    
    def getCDF_X (df, percentile):
        cdf          = df.cumsum()
        maxV         = cdf['count'].iloc[-1]
        cdf['count'] = (cdf['count'] / maxV) * 100
        tmpDf        = cdf[cdf['count'] < (percentile+0.5)]
        if tmpDf.empty:
           x  = 1;
        else:
           x  = tmpDf.iloc[-1].name

        return x, cdf

    #return jobs' count by cpus_req, cpus_alloc and nodes_alloc
    def getJobCount (cluster, start, stop, upper=90):
        start,stop,df    = SlurmDBQuery.readJobTable  (cluster, start, stop, ['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'tres_alloc', 'tres_req'], index_col=2)
        #remove nodes_alloc=0
        df               = df[df['nodes_alloc']>0]
        df               = df[df['account'].notnull()]
        df               = df[df['tres_alloc'].notnull()]
        df               = df[df['tres_req'].notnull()]
        df['count']      = 1
        df['cpus_alloc'] = df['tres_alloc'].map(MyTool.extract1)
        df['nodes_req']  = df['tres_req'].map  (MyTool.extract4)

        result           = {}     #{'cpus_req':{'max_x':, 'total':, 'cca':}
        # get the CDF data of cpus_req
        for col in ['cpus_req', 'cpus_alloc', 'nodes_req', 'nodes_alloc']:
            sumDf        = df[[col, 'count']].groupby(col).sum()
            xMax,cdf     = SlurmDBQuery.getCDF_X (sumDf, upper)
            result[col]  = {'upper_x':xMax, 'count':cdf.reset_index(), 'account':{}}
            # differiate among accounts
            acctDf       = df[['account', col, 'count']].groupby(['account', col]).sum()
            idx0         = acctDf.index.get_level_values(0).unique()
            for v in idx0.values:
                result[col]['account'][v] = acctDf.loc[v].reset_index()

        return start, stop, result

    def logBucket(x):
        if x < 5:
           return 5
        if x <= 10:
           return x
        if x > 10900000000:       # >10^10.04, out of bucket range
           return x
        b = round(math.log10(x)*10)/10
        return LOG_BUCKET[b]

    #return jobs' count by time_run, time_cpu=time_run * cpus_alloc
    def getJobTime (cluster, start, stop, upper=90):
        start,stop,df    = SlurmDBQuery.readJobTable  (cluster, start, stop, ['account', 'cpus_req','id_job','state', 'time_submit', 'time_start', 'time_end', 'tres_alloc'], index_col=2)
        df               = df[df['account'].notnull()]
        #df    = df[df['state'] == state]     # only count completed jobs (state=3)
        df               = df[df['time_start']>0]      
        df               = df[df['time_end']  >df['time_start']]
        df['count']      = 1
        df['time_run']   = df['time_end']-df['time_start']
        #df                   = df[df['time_exe']>5]
        df['cpus_alloc'] = df['tres_alloc'].map(MyTool.extract1)
        df['time_cpu']   = df['time_run'] * df['cpus_alloc']
        df['time_run_log'] = df['time_run'].map(SlurmDBQuery.logBucket)
        df['time_cpu_log'] = df['time_cpu'].map(SlurmDBQuery.logBucket)

        result           = {}     #{'cpus_req':{'max_x':, 'total':, 'cca':}
        # get the CDF data of cpus_req
        for col in ['time_run_log', 'time_cpu_log']:
            sumDf        = df[[col, 'count']].groupby(col).sum()
            xMax,cdf     = SlurmDBQuery.getCDF_X (sumDf, upper)
            result[col]  = {'upper_x':xMax, 'count':cdf.reset_index(), 'account':{}}
            # differiate among accounts
            acctDf       = df[['account', col, 'count']].groupby(['account', col]).sum()
            idx0         = acctDf.index.get_level_values(0).unique()
            for v in idx0.values:
                result[col]['account'][v] = acctDf.loc[v].reset_index()

        return start, stop, result
    #call daily
    def plusFiles ():
        SlurmDBQuery.plusUsageFiles()
        for tbl in ['assoc_table']:
           src  = "{}/slurm_{}.csv".format(CSV_DIR, tbl)
           dst  = "{}/slurm_plus_{}.csv".format(CSV_DIR, tbl)
           shutil.copy(src,dst)

    def plusUsageFiles ():
      # combine usage files
      for tbl in ['usage_day_table', 'usage_hour_table', 'assoc_usage_day_table', 'assoc_usage_hour_table']:
        f1  = "{}/slurm_cluster_mod_{}.csv".format(CSV_DIR, tbl)
        f2  = "{}/slurm_{}.csv".format(CSV_DIR, tbl)
        f   = "{}/slurm_plus_{}.csv".format(CSV_DIR, tbl)

        print("{} + {} = {}".format(f1,f2,f))
        df1 = pandas.read_csv(f1)
        df2 = pandas.read_csv(f2)
        df  = df1.append(df2, ignore_index=True)
        df.to_csv(f, index=False)

#one time calling 
def truncUsageFiles():
    ts     = 1604786400        #head slurm_usage_hour_table 
    for tbl in ['usage_day_table', 'usage_hour_table', 'assoc_usage_day_table', 'assoc_usage_hour_table']:
        in_f  = "{}/slurm_cluster_{}.csv".format(CSV_DIR, tbl)
        out_f = "{}/slurm_cluster_mod_{}.csv".format(CSV_DIR, tbl)
        truncUsageFile (in_f, out_f, ts)

def test1():
    SlurmDBQuery.getJobByName("jupyter-notebook")

def test2():
    client = SlurmDBQuery()
    info = client.getClusterJobQueue()

def test4():
    SlurmDBQuery.getJobCount('slurm_cluster','','')

def main():
    t1=time.time()

if __name__=="__main__":
    #SlurmDBQuery.plusUsageFiles ()
    #def savAccountCPUAlloc (cluster, day_or_hour, output_file, clusterDf=None):
    #df = pandas.read_csv("./data/slurm_plus_day_cpuAllocDF.csv")
    #fname2 = "{}/{}_{}_{}".format(CSV_DIR, cluster, day_or_hour, "cpuAllocDF_{}.csv")
    #SlurmDBQuery.savAccountCPUAlloc('slurm_plus', 'day', './data/slurm_plus_day_cpuAllocDF_{}.csv', df)
    test1()
