import cherrypy, _pickle as cPickle, csv, datetime, json, os
import pandas, pwd, re, subprocess as SUB, sys, time, zlib
from collections import defaultdict
import fbprophet as fbp
import matplotlib.pyplot as plt
import operator
from functools import reduce

import fs2hc
import pyslurm
from queryInflux     import InfluxQueryClient
from querySlurm      import SlurmCmdQuery, SlurmDBQuery
from queryTextFile   import TextfileQueryClient
from queryBright     import BrightRestClient
from IndexedDataFile import IndexedHostData
from EmailSender     import JobNoticeSender

import MyTool
import SlurmEntities
import inMemCache

wai = os.path.dirname(os.path.realpath(sys.argv[0]))

WebPort = int(sys.argv[1])

# Directory where processed monitoring data lives.
htmlPreamble = '''\
<!DOCTYPE html>
<html>
<head>
    <link href="/static/css/style.css" rel="stylesheet">
</head>
<body>
'''

SACCT_WINDOW_DAYS  = 3 # number of days of records to return from sacct.
USER_INFO_IDX      = 3
USER_PROC_IDX      = 7
WAIT_MSG           = 'No data received yet. Wait a minute and come back. '
EMPTYDATA_MSG      = 'There is no data retrieved according to the constraints.'
EMPTYPROCDATA_MSG  = 'There is no process data present in the retrieved data.'
ONE_HOUR_SECS      = 3600
ONE_DAY_SECS       = 86400
TIME_DISPLAY_FORMAT        = '%m/%d/%y %H:%M'
DATE_DISPLAY_FORMAT        = '%m/%d/%y'

@cherrypy.expose
class SLURMMonitor(object):

    def __init__(self):
        with open('./config.json') as config_file:
           self.config = json.load(config_file)
        self.queryTxtClient  = TextfileQueryClient(os.path.join(wai, 'host_up_ts.txt'))
        self.querySlurmClient= SlurmDBQuery()
        self.jobNoticeSender = JobNoticeSender()

        self.startTime       = time.time()
        self.updateTS        = None
        self.rawData         = {}                   #not used
        self.data            = 'No data received yet. Wait a minute and come back.'
        self.allJobs         = {}
        self.currJobs        = {}
        self.node2jobs       = {}                   #{node:[jid...]} {node:{jid: {'proc_cnt','cpu_util','rss','iobps'}}}
        self.uid2jid         = {}                   #
        self.pyslurmNodeData = None
        self.jobNode2ProcRecord = defaultdict(lambda: defaultdict(lambda: (0, defaultdict(lambda: defaultdict(int))))) # jid: node: (ts, pid: cpu_time)
        self.jobNode2ProcRecord_0  = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))  #jid: node: pid: 'ts' 'cpu_time'
                                                                               # one 'ts' kept for each pid
                                                                               # modified through updateJobNode2ProcRecord only
        self.inMemCache      = inMemCache.InMemCache()
 

    # add processes info of jid, modify self.jobNode2ProcRecord
    # TODO: it is in fact userNodeHistory
    def updateJobNode2ProcRecord (self, ts, jobid, node, processes):
        ts            = int(ts)
        savTs, savRec = self.jobNode2ProcRecord[jobid][node]
        if ( ts > savTs ):
           for p in processes:   # pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, intervalIOByteAvg, jid
               # 09/09/2019 add jid
               assert (savRec[p[0]]['cpu_time'] <= p[3] + p[4])        #increasing
               savRec[p[0]]['cpu_time']     = p[3] + p[4]
               savRec[p[0]]['mem_rss_K']    = int(p[5]/1024)
               savRec[p[0]]['io_bps_curr']  = p[8]                    #bytes per sec
               savRec[p[0]]['cpu_util_curr']= p[1]                    #bytes per sec
               savRec[p[0]]['jid']          = p[9]                    
           #else discard older information
        
           self.jobNode2ProcRecord[jobid][node] = (ts, savRec)  # self.jobNode2ProcRecord[jobid][node][1] is modified through savRec
           #if len(self.jobNode2ProcRecord) > 100:
           #remove done job from history
           done_job = [jid for jid, jinfo in self.currJobs.items() if jinfo['job_state'] not in ['RUNNING', 'PENDING', 'PREEMPTED']]
           for jid in done_job:
               self.jobNode2ProcRecord.pop(jid, {})
        
    def getDFBetween(self, df, field, start, stop):
        if start:
            start = time.mktime(time.strptime(start, '%Y-%m-%d'))
            df    = df[df[field] >= start]
        if stop:
            stop  = time.mktime(time.strptime(stop,  '%Y-%m-%d'))
            df    = df[df[field] <= stop]
        if not df.empty:
            start = df.iloc[0][field]
            stop  = df.iloc[-1][field]

        return start, stop, df

    def getCDF_X (self, df, percentile):
        cdf   = df.cumsum()
        maxV  = cdf['count'].iloc[-1]
        cdf['count'] = (cdf['count'] / maxV) * 100
        tmpDf = cdf[cdf['count'] < (percentile+0.5)]
        if tmpDf.empty:
           x  = 1;
        else:
           x  = tmpDf.iloc[-1].name

        return x, cdf

    @cherrypy.expose
    def partitionDetail(self, partition='gpu'):
        ins           = SlurmEntities.SlurmEntities()
        p_info, nodes = ins.getPartitionAndNodes (partition)
        htmlTemp      = os.path.join(wai, 'partitionDetail.html')
        htmlStr       = open(htmlTemp).read().format(p_detail=p_info, p_nodes=json.dumps(nodes))
        return htmlStr
       
    @cherrypy.expose
    def pending(self, start='', stop='', state=3):
        ins        = SlurmEntities.SlurmEntities()
        pendingLst = ins.getPendingJobs()
        partLst    = ins.getPartitions ()
        for p in partLst:
            p['running_jobs'] = ' '.join(str(e) for e in p['running_jobs'])
            p['pending_jobs'] = ' '.join(str(e) for e in p['pending_jobs'])
        
        htmlTemp   = os.path.join(wai, 'pending.html')
        timestr    = ins.update_time.ctime()
        htmlStr    = open(htmlTemp).read().format(update_time=timestr, job_cnt=len(pendingLst), pending_jobs_input=pendingLst, partitions_input=partLst)
        return htmlStr
       
    @cherrypy.expose
    def topJobReport(self, start='', stop='', state=3):
#job_db_inx,mod_time,deleted,account,admin_comment,array_task_str,array_max_tasks,array_task_pending,cpus_req,derived_ec,derived_es,exit_code,job_name,id_assoc,id_array_job,id_array_task,id_block,id_job,id_qos,id_resv,id_wckey,id_user,id_group,kill_requid,mem_req,nodelist,nodes_alloc,node_inx,partition,priority,state,timelimit,time_submit,time_eligible,time_start,time_end,time_suspended,gres_req,gres_alloc,gres_used,wckey,track_steps,tres_alloc,tres_req
        df    = pandas.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','job_name', 'id_assoc', 'id_job','id_qos', 'id_user', 'nodelist', 'nodes_alloc', 'state', 'time_submit', 'time_start', 'time_end', 'gres_req', 'tres_req'],index_col=4)
        start, stop, df    = self.getDFBetween (df, 'time_submit', start, stop)
        df    = df[df['time_start']>0]
        df    = df[df['time_end']  >df['time_start']]

        df['time_exe']       = df['time_end']-df['time_start']
        df['time_exe_total'] = df['time_exe'] * df['cpus_req']

        sortDf=df.sort_values('time_exe_total', ascending=False)

        # CDF data for time_exe_total
        return repr(sortDf.head(10))

    @cherrypy.expose
    def cdfTimeReport(self, upper, start='', stop='', state=3):
        def bucket1(x):
            return bucket(x,1)
        def bucket10(x):
            return bucket(x,10)
        def bucket(x, d):
            if x < 5*d:
               return 5*d
            elif x < 20*d:
               #return (int((x+1*d)/(2*d))) * 2*d
               return (int(x/d)) *d
            elif x < 50*d:
               return (int((x+d)/(2*d))) * 2*d
            elif x < 100*d:
               return (int((x+2.5*d)/(5*d))) * 5*d
            elif x < 200*d:
               return (int((x+5*d)/(10*d)))  * 10*d
            elif x < 500*d:
               return (int((x+10*d)/(20*d))) * 20*d
            elif x < 1000*d:
               return (int((x+25*d)/(50*d))) * 50*d
            elif x < 2000*d:
               return (int((x+50*d)/(100*d)))* 100*d
            elif x < 4000*d:
               return (int((x+100*d)/(200*d))) * 200*d
            elif x < 20000*d:
               return (int((x+250*d)/(500*d))) * 250*d
            else:
               return (int((x+500*d)/(1000*d))) * 1000*d
               
        upper = float(upper)
        df    = pandas.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','state', 'time_submit', 'time_start', 'time_end'],index_col=2)
        df    = df[df['account'].notnull()]
        start, stop, df    = self.getDFBetween (df, 'time_submit', start, stop)
        #df    = df[df['state'] == state]     # only count completed jobs (state=3)
        df    = df[df['time_start']>0]
        df    = df[df['time_end']  >df['time_start']]

        df['count']          = 1
        df['time_exe']       = df['time_end']-df['time_start']
        df                   = df[df['time_exe']>5]
        df['time_exe_total'] = df['time_exe'] * df['cpus_req']
        #df['time_exe']      = (df['time_exe']+5)//10 * 10
        df['time_exe']       = df['time_exe'].map(bucket1)
        df['time_exe_total'] = df['time_exe_total'].map(bucket10)

        # CDF data for time_exe
        sumDf      = df[['time_exe', 'count']].groupby('time_exe').sum()
        xMax,cdf   = self.getCDF_X (sumDf, upper)
        series     = [{'type': 'spline', 'name': 'CDF', 'yAxis':1, 'zIndex': 10, 'data': cdf.reset_index().values.tolist()}] 

        sumDf      = df[['account', 'time_exe', 'count']].groupby(['account', 'time_exe']).sum()
        idx0       = sumDf.index.get_level_values(0).unique()
        for v in idx0.values:
            series.append({'type': 'column', 'name': v, 'data': sumDf.loc[v].reset_index().values.tolist()})

        # CDF data for time_exe_total
        sumDf      = df[['time_exe_total', 'count']].groupby('time_exe_total').sum()
        xMax2,cdf  = self.getCDF_X (sumDf, upper)
        dArr2      = sumDf['count'].reset_index().values

        htmlTemp   = os.path.join(wai, 'CDFHC.html')
        parameters = {'series': series, 'title': 'Execution Time of Jobs', 
                      'start': time.strftime('%Y-%m-%d', time.localtime(start)), 'stop': time.strftime('%Y-%m-%d', time.localtime(stop)),
                      'xMax':int(xMax), 'xLabel': 'Execution Time', 'yLabel': 'Count',
                      'series2': dArr2.tolist(), 'title2': 'Execution Time * Required Cores of Jobs', 
                      'xMax2':int(xMax2), 'xLabel2': 'Execution Time', 'yLabel2': 'Count'}
        htmlStr    = open(htmlTemp).read().format(**parameters)
        return htmlStr

    @cherrypy.expose
    def cdfReport(self, upper, start='', stop=''):
        upper         = float(upper)
        df            = pandas.read_csv("slurm_cluster_job_table.csv",usecols=['account', 'cpus_req','id_job','id_qos', 'id_user', 'nodes_alloc', 'state', 'time_submit', 'tres_alloc'],index_col=2)
        start,stop,df = self.getDFBetween (df, 'time_submit', start, stop)
        #remove nodes_alloc=0
        df            = df[df['nodes_alloc']>0]

        df['count']      = 1
        df['cpus_alloc'] = df['tres_alloc'].map(MyTool.extract1)

        # get the CDF data of cpus_req
        sumDf       = df[['cpus_req', 'count']].groupby('cpus_req').sum()
        xMax,cdf    = self.getCDF_X (sumDf, upper)
        series     = [{'type': 'spline', 'name': 'CDF', 'yAxis':1, 'zIndex': 10, 'data': cdf.reset_index().values.tolist()}] 

        sumDf      = df[['account', 'cpus_req', 'count']].groupby(['account', 'cpus_req']).sum()
        idx0       = sumDf.index.get_level_values(0).unique()
        for v in idx0.values:
            series.append({'type': 'column', 'name': v, 'data': sumDf.loc[v].reset_index().values.tolist()})

        # get the CDF data of cpus_alloc 
        sumDf       = df[['cpus_alloc', 'count']].groupby('cpus_alloc').sum()
        xMax2,cdf       = self.getCDF_X (sumDf, upper)
        dArr2       = sumDf['count'].reset_index().values
        
        # get the CDF data of nodes_alloc 
        sumDf       = df[['nodes_alloc', 'count']].groupby('nodes_alloc').sum()
        xMax3,cdf       = self.getCDF_X (sumDf, upper)
        dArr3       = sumDf['count'].reset_index().values

        htmlTemp   = os.path.join(wai, 'CDFHC_3.html')
        parameters = {'start': time.strftime('%Y-%m-%d', time.localtime(start)), 
                      'stop':  time.strftime('%Y-%m-%d', time.localtime(stop)),
                      'series': series, 'title': 'Number of Requested CPUs', 'xMax': int(xMax), 'xLabel': 'Number of CPUs', 'yLabel': 'Count',
                      'series2':dArr2.tolist(),'title2':'Number of Allocated CPUs', 'xMax2':int(xMax2),'xLabel2':'Number of CPUs', 'yLabel2':'Count',
                      'series3':dArr3.tolist(),'title3':'Number of Allocated Nodes','xMax3':int(xMax3),'xLabel3':'Number of Nodes','yLabel3':'Count'}
        htmlStr    = open(htmlTemp).read().format(**parameters)
        return htmlStr

    @cherrypy.expose
    def accountReport_hourly(self, start='', stop=''):
        #sumDf index ['id_tres','acct', 'time_start'], 
        
        start, stop, sumDf = self.querySlurmClient.getAccountUsage_hourly(start, stop)

        sumDfg      = sumDf.groupby('id_tres')
        tresSer     = {} # {1: [{'data': [[ms,value],...], 'name': uid},...], 2:...} 
        for tres in sumDf.index.get_level_values('id_tres').unique():
            tresSer[tres] = []
            acctIdx       = sumDfg.get_group(tres).index.get_level_values('acct').unique()
            for acct in acctIdx:
                sumDf.loc[(tres,acct,), ]
                tresSer[tres].append({'name': acct, 'data':sumDf.loc[(tres,acct,), ['ts', 'alloc_secs']].values.tolist()}) 
                
        #generate data
        cpuLst   = tresSer[1]
        #start    = min(cpuLst, key=(lambda item: (item['data'][0][0])))['data'][0][0]  /1000
        #stop     = max(cpuLst, key=(lambda item: (item['data'][-1][0])))['data'][-1][0]/1000
        htmlTemp = os.path.join(wai, 'scatterHC.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': tresSer[1], 'title1': 'Account CPU usage hourly report',  'xlabel1': 'CPU core secs', 'aseries1':[], 
                                   'series2': tresSer[4], 'title2': 'Account Node usage hourly report', 'xlabel2': 'Node secs',     'aseries2':[],
                                   'series3': tresSer[2], 'title3': 'Account Mem usage hourly report',  'xlabel3': 'MEM MB secs',   'aseries3':[]}

        return h

    @cherrypy.expose
    def userReport_hourly(self, start='', stop='', top=5):
        # get top 5 user for each resource
        tresSer  = self.querySlurmClient.getUserReport_hourly(start, stop, top)
        
        cpuLst   = tresSer[1]
        start    = min(cpuLst, key=(lambda item: (item['data'][0][0])))['data'][0][0]  /1000
        stop     = max(cpuLst, key=(lambda item: (item['data'][-1][0])))['data'][-1][0]/1000
        htmlTemp = os.path.join(wai, 'scatterHC.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': tresSer[1], 'title1': 'User CPU usage hourly report', 'xlabel1': 'CPU core secs', 'aseries1':[], 
                                   'series2': tresSer[2], 'title2': 'User Mem usage hourly report', 'xlabel2': 'MEM MB secs',   'aseries2':[],
                                   'series3': tresSer[4], 'title3': 'User Node usage hourly report','xlabel3': 'Node secs', 'aseries3':[]}

        return h

    def getClusterUsageHourTable(self, start, stop):
        #read from csv, TODO: deleted=0 for all data now
        df               = pandas.read_csv("slurm_cluster_usage_hour_table.csv", names=['creation_time','mod_time','deleted','id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'], usecols=['id_tres','time_start','count','alloc_secs','down_secs','pdown_secs','idle_secs','resv_secs','over_secs'])
        start, stop, df  = self.getDFBetween(df, 'time_start', start, stop)
        df['total_secs'] = df['alloc_secs']+df['down_secs']+df['pdown_secs']+df['idle_secs']+df['resv_secs']
        df               = df[df['count'] * 3600 == df['total_secs']]

        #df               = df[df['alloc_secs']>0]

        return start, stop, df

    @cherrypy.expose
    def clusterForecast_hourly(self, start='', stop='', chg_prior=0.5, period=720):
        start, stop, df = self.getClusterUsageHourTable (start, stop)

        htmlTemp = os.path.join(wai, 'image.html')
        h = open(htmlTemp).read()

        return h

    @cherrypy.expose
    def accountForecast_hourly(self, start='', stop='', chg_prior=0.5, period=720):
        start, stop, df = self.getClusterUsageHourTable (start, stop)

        htmlTemp = os.path.join(wai, 'acctImage.html')
        #print(htmlTemp)
        h = open(htmlTemp).read()

        return h

    @cherrypy.expose
    def clusterHistory(self, start='', stop='', days=3):
        start, stop  = MyTool.getStartStopTS (start, stop)

        #influxClient = InfluxQueryClient.getClientInstance()
        influxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
        ts2AllocNodeCnt, ts2MixNodeCnt, ts2IdleNodeCnt, ts2DownNodeCnt, ts2AllocCPUCnt, ts2MixCPUCnt, ts2IdleCPUCnt, ts2DownCPUCnt= influxClient.getSavedNodeHistory(days=days)
        runJidSet, ts2ReqNodeCnt, ts2ReqCPUCnt, pendJidSet, ts2PendReqNodeCnt, ts2PendReqCPUCnt = influxClient.getSavedJobRequestHistory (days=days)
        #it is more difficult to get the node number from running jobs as they may share 
        series11  = [
                     {'name': 'Allocated Nodes', 'data':[[ts, cnt] for ts, cnt in ts2AllocNodeCnt.items()]},
                     {'name': 'Mixed Nodes',     'data':[[ts, cnt] for ts, cnt in ts2MixNodeCnt.items()]},
                     {'name': 'Idle Nodes',      'data':[[ts, cnt] for ts, cnt in ts2IdleNodeCnt.items()]},
                     {'name': 'Down Nodes',      'data':[[ts, cnt] for ts, cnt in ts2DownNodeCnt.items()]},
                    ]
        series12  = [
                     {'name': 'Allocated Nodes',                  'data':[[ts, cnt+ts2MixNodeCnt[ts]] for ts, cnt in ts2AllocNodeCnt.items()]},
                     {'name': 'Running Job Requested Nodes',      'data':[[ts, cnt] for ts, cnt in ts2ReqNodeCnt.items()]},
                     {'name': 'Idle Nodes',                       'data':[[ts, cnt] for ts, cnt in ts2IdleNodeCnt.items()]},
                     {'name': 'Pending Job Requested Nodes',      'data':[[ts, cnt] for ts, cnt in ts2PendReqNodeCnt.items()]},
                    ]
        series21  = [
                     {'name': 'Allocated CPUs of Allocated Nodes', 'data':[[ts, cnt] for ts, cnt in ts2AllocCPUCnt.items()]},
                     {'name': 'Allocated CPUs of Mixed Nodes',     'data':[[ts, cnt] for ts, cnt in ts2MixCPUCnt.items()]},
                     {'name': 'Idle CPUs',                         'data':[[ts, cnt] for ts, cnt in ts2IdleCPUCnt.items()]},
                     {'name': 'Down CPUs',                         'data':[[ts, cnt] for ts, cnt in ts2DownCPUCnt.items()]},
                    ]
        series22  = [
                     {'name': 'Allocated CPUs',                   'data':[[ts, cnt+ts2MixCPUCnt[ts]] for ts, cnt in ts2AllocCPUCnt.items()]},
                     {'name': 'Running Job Requested CPUs',       'data':[[ts, cnt] for ts, cnt in ts2ReqCPUCnt.items()]},
                     {'name': 'Idle CPUs',                        'data':[[ts, cnt] for ts, cnt in ts2IdleCPUCnt.items()]},
                     {'name': 'Pending Job Requested CPUs',       'data':[[ts, cnt] for ts, cnt in ts2PendReqCPUCnt.items()]},
                    ]

        htmlTemp = os.path.join(wai, 'jobResourceReport.html')
        h = open(htmlTemp).read().format(start=time.strftime('%Y-%m-%d', time.localtime(start)),
                                         stop =time.strftime('%Y-%m-%d', time.localtime(stop)),
                                         series_11=series11, series_12=series12, xlabel1='Node Count',
                                         series_21=series21, series_22=series22, xlabel2='CPU Count')

        return h

    def getReason2Cate (self, reason):
        cateRegex = [('Sched','Dependency|BeginTime|JobArrayTaskLimit'), ('Resource','Resources|Priority|ReqNodeNotAvail*|Nodes_required_for_job_are_DOWN*'), ('GPU', 'Resources_GPU|Priority_GPU'), ('QoSGrp', 'QOSGrp*'), ('QoS', 'QOS*')]  #order matters
        if not reason:
            return 'Other'
        for (cate, regex) in cateRegex:
            if re.match(regex, reason):
               return cate
        return 'Other'

    @cherrypy.expose
    def pending_history(self, start='', stop='', days=''):
        note         = ''
        if days:
           start, stop  = MyTool.getStartStopTS ('', '', days=int(days))
        else:
           start, stop  = MyTool.getStartStopTS (start, stop, days=7)

        #influxClient = InfluxQueryClient.getClientInstance()
        influxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
        tsReason2Cnt, jidSet = influxClient.getPendingCount(start, stop)
        reasons      = [set(reasons.keys()) for ts, reasons in tsReason2Cnt.items()]
        reasons      = set([i2 for item in reasons for i2 in item])
        reason2cate  = dict([(reason, self.getReason2Cate(reason)) for reason in reasons])
        cates        = set(reason2cate.values())
        print('reasons={}, cates={}, reason2cate={}'.format(reasons, cates, reason2cate))
        
        #reformat the tsReason2Cnt to cate2ts_cnt
        other_reason_set = set()
        sav_set      = set()
        cate2ts_cnt  = defaultdict(list)         
        for ts, reason2Cnt in tsReason2Cnt.items():  # {ts: {reason:cnt, }}
            cate2cnt = defaultdict(int)
            for reason, cnt in reason2Cnt.items():
                cate = reason2cate[reason]
                cate2cnt[cate] += cnt
                if cate=='Other':
                   other_reason_set.add(reason)
            for cate in cates:
                cate2ts_cnt[cate].append ([ts, cate2cnt[cate]])
        #note = '{}'.format(sav_set) 

        cate2title = {'Resource':'Queued by Resource', 'GPU':'Queued by GPU Resource', 'QoSGrp':'Queued by Group QoS', 'QoS':'Queued by User QoS', 'Sched':'Queued by Job Defination', 'Other':'Queued by Other'}  #order matters
        series1   = []
        for cate in cate2title.keys():
            if cate in cate2ts_cnt:
               series1.append({'name': cate2title[cate], 'data': cate2ts_cnt[cate]})
        htmlTemp = os.path.join(wai, 'pendingJobReport.html')
        h = open(htmlTemp).read().format(start=time.strftime(DATE_DISPLAY_FORMAT, time.localtime(start)),
                                         stop=time.strftime(DATE_DISPLAY_FORMAT, time.localtime(stop)),
                                         series1=series1, title1='Cluster Job Queue Length', xlabel1='Queue Length', other_reason=list(other_reason_set),
                                         note=note)

        return h

    @cherrypy.expose
    def queueLengthReport(self, start='', stop='', queueTime=0):
    #def clusterJobQueueReport(self, start='', stop=''):

        start, stop, df = self.querySlurmClient.getClusterJobQueue (start, stop, int(queueTime))
        
        #df = index | time | value 
        #convert to highchart format, cpu, mem, energy (all 0, ignore)
        df['time'] = df['time'] * 1000
        series1   = [{'data': df[['time', 'value0']].values.tolist(), 'name': 'Job Queue Length'}]
        series2   = [{'data': df[['time', 'value1']].values.tolist(), 'name': 'Requested CPUs of Queued Job'}]

        htmlTemp = os.path.join(wai, 'seriesHC_2.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': series1, 'title1': 'Cluster Job Queue Length', 'xlabel1': 'Queue Length', 'aseries1':[], 
                                   'series2': series2, 'title2': 'Requested CPUs of Queued Job', 'xlabel2': 'CPUs', 'aseries2':[]} 


        return h

    @cherrypy.expose
    def clusterReport_hourly(self, start='', stop='', action=''):

        start, stop, df = self.getClusterUsageHourTable (start, stop)
        
        df['tdown_secs'] = df['down_secs']+df['pdown_secs']
        df['ts_ms']      = df['time_start'] * 1000

        #convert to highchart format, cpu, mem, energy (all 0, ignore)
        #get indexed of all cpu data, retrieve useful data
        dfg    = df.groupby  ('id_tres')
        cpuDf  = dfg.get_group(1)
        memDf  = dfg.get_group(2)
        eneDf  = dfg.get_group(3)

        cpu_alloc  = cpuDf[['ts_ms', 'alloc_secs']].values.tolist()
        cpu_down   = cpuDf[['ts_ms', 'tdown_secs']].values.tolist()
        cpu_idle   = cpuDf[['ts_ms', 'idle_secs']].values.tolist()
        cpu_resv   = cpuDf[['ts_ms', 'resv_secs']].values.tolist()
        cpu_over   = cpuDf[['ts_ms', 'over_secs']].values.tolist()
        cpu_series = [
                      {'data': cpu_alloc, 'name': 'Alloc secs'},
                      {'data': cpu_resv,  'name': 'Reserve secs'},
                      {'data': cpu_idle,  'name': 'Idle secs'},
                      {'data': cpu_down,  'name': 'Down secs'},
                      {'data': cpu_over,  'name': 'Over secs'},
                     ]  ##[{'data': [[1531147508000(ms), value]...], 'name':'userXXX'}, ...] 

        mem_alloc  = memDf.loc[:,['ts_ms', 'alloc_secs']].values.tolist()
        mem_down   = memDf.loc[:,['ts_ms', 'tdown_secs']].values.tolist()
        mem_idle   = memDf.loc[:,['ts_ms', 'idle_secs']].values.tolist()
        mem_resv   = memDf.loc[:,['ts_ms', 'resv_secs']].values.tolist()
        mem_over   = memDf.loc[:,['ts_ms', 'over_secs']].values.tolist()
        mem_series = [
                      {'data': mem_alloc, 'name': 'Alloc secs'},
                      {'data': mem_resv,  'name': 'Reserve secs'},
                      {'data': mem_idle,  'name': 'Idle secs'},
                      {'data': mem_down,  'name': 'Down secs'},
                      {'data': mem_over,  'name': 'Over secs'},
                     ]  ##[{'data': [[1531147508, value]...], 'name':'userXXX'}, ...] 

        cpu_ann    = cpuDf.groupby('count').first().loc[:,['ts_ms', 'total_secs']].reset_index().values.tolist()
        mem_ann    = memDf.groupby('count').first().loc[:,['ts_ms', 'total_secs']].reset_index().values.tolist()

        htmlTemp = os.path.join(wai, 'clusterHC.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': cpu_series, 'title1': 'Cluster CPU usage hourly report', 'xlabel1': 'CPU core secs', 'aseries1':cpu_ann, 
                                   'series2': mem_series, 'title2': 'Cluster Mem usage hourly report', 'xlabel2': 'MEM MB secs',   'aseries2':mem_ann}

        return h

    @cherrypy.expose
    def getNodeData(self):
        return repr(self.data)

    @cherrypy.expose
    def getJobData(self):
        return repr(self.currJobs)

    @cherrypy.expose
    def getAllJobData(self):
        return '({},{})'.format(self.updateTS, self.allJobs)

    @cherrypy.expose
    def getRawNodeData(self):
        return repr(self.pyslurmNodeData)

    # return earliest start time and all current jobs
    def getUserCurrJobs (self, uid):
        uid         = int(uid)
        jobs        = [ job for job in self.currJobs.values() if job.get('user_id',0) == uid]
        first_start = -1
        if jobs:
           first_start = min([j['start_time'] for j in jobs])  #curr jobs always have start_time
        return first_start, jobs

    def getUserJobStartTimes(self, uid):
        uid   = int(uid)
        stime = []
        for jid, jinfo in self.currJobs.items():
            if ( jinfo.get('user_id',-1) == uid ): stime.append(jinfo.get('start_time', 0)) 

        return stime

    @cherrypy.expose
    def getNode2Jobs1 (self):
        return "{}".format(self.node2jobs)

    def updateNode2Jobs (self, jobData):
        self.node2jobs = defaultdict(list)  #nodename: joblist
        for jid, jinfo in jobData.items():
            for nodename in jinfo.get('cpus_allocated', {}).keys():
                self.node2jobs[nodename].append(jid)

    #TODO: for the case that 1 user - n job
    def uid2jids  (self, uid, node):
        jids = list(filter(lambda x: self.currJobs[x]['user_id']==uid, self.node2jobs[node]))
        if len(jids) == 0:
           print ('WARNING: uid2jid user {} has no slurm jobs on node {} (jobs {}). Ignore proc data.'.format(uid, node, self.node2jobs[node]))
        elif len(jids) > 1:
           print ('WARNING: uid2jid user {} has multiple slurm jobs {} on node {}'.format(uid, jids, node))

        return jids 

    #get the total cpu time of uid on node
    def getJobNodeTotalCPUTime(self, jid, node):
        time_lst = [ d['cpu_time'] for d in self.jobNode2ProcRecord[jid][node][1].values() ]

        return sum(time_lst)

    def getJobUsageOnNode (self, jid, job, node):
        job_uid = job['user_id']
        if len(node) > USER_INFO_IDX:
           #09/09/2019 add jid
           #calculate each job's
           user_proc = [userInfo for userInfo in node[USER_INFO_IDX:] if userInfo[1]==job_uid ]
           if len(user_proc) != 1: 
              print("ERROR: user {} has {} record on {}. Ignore".format(job_uid, len(user_proc), node))
              return None, None, None, None, None, None

           job_proc = [proc for proc in user_proc[0][7] if proc[9]==jid]
           # summary data in job_proc
           # [pid, CPURate, 'create_time', 'user_time', 'system_time', 'rss', vms, cmdline, IOBps, jid]
           cpuUtil  = sum([proc[1]         for proc in job_proc])
           rss      = sum([proc[5]         for proc in job_proc])
           vms      = sum([proc[6]         for proc in job_proc])
           iobps    = sum([proc[8]         for proc in job_proc])
           fds      = sum([proc[10]        for proc in job_proc])
           return cpuUtil, rss, vms, iobps, len(job_proc), fds,
        else:
           return 0, 0, 0, 0, 0, 0

    def getSummaryTableData(self, hostData, jobData, node2jobs):
        result    = []
        for node, nodeInfo in sorted(hostData.items()):
            if len(nodeInfo) < USER_INFO_IDX:
               print("ERROR: getSummaryTableData nodeInfo wrong format {}:{}".format(node, nodeInfo)) 
               continue

            #status display no extra
            status, delay, ts = nodeInfo[0], nodeInfo[1], nodeInfo[2]
            if status.endswith(('@','+','$','#','~','*')):  #format status diaplay
               status = status[:-1]
            if ( node in node2jobs) and node2jobs[node]:  # node has running jobs
               for jid in node2jobs[node]:                # for each job on the node, add one item
                  job_user    = MyTool.getUser(jobData[jid]['user_id'])
                  job_coreCnt = jobData[jid]['cpus_allocated'][node]
                  job_cpuUtil, job_rss, job_vms, job_iobyteps, job_procCnt, job_fds = self.getJobUsageOnNode(jid, jobData[jid], nodeInfo)
                  if job_procCnt:
                     job_avg_cpu = self.getJobNodeTotalCPUTime(jid, node) / (ts-jobData[jid]['start_time']) if jobData[jid]['start_time'] > 0 else 0 
                     if job_avg_cpu < 0:
                        print ("ERROR: job_avg_cpu ({}, {}, {}) less than 0.".format(job_avg_cpu, ts, job_stime))
                     result.append([node, status, delay, jid, job_user, job_coreCnt, job_procCnt, job_cpuUtil, job_avg_cpu, job_rss, job_vms, job_iobyteps, job_fds])
                  else:
                     result.append([node, status, delay, jid, job_user, job_coreCnt, 0, 0, 0, 0, 0, 0, 0])
            else:
               result.append([node, status, delay])
                
        return result
        
    @cherrypy.expose
    def getNodeUtil (self, **args):
        return self.getNodeUtilData (self.pyslurmNodeData, self.data)

    def getCPUMemData (self, data1, data2):
        result = defaultdict(list)
        for hostname, values in data1.items():
            #mem in MB
            result[hostname].extend([values[key] for key in ['cpus', 'cpu_load', 'alloc_cpus', 'real_memory', 'free_mem', 'alloc_mem']])

        for hostname, values in data2.items():
            if len(values) > USER_INFO_IDX:
               #mem in B?
               for uname, uid, allocCore, procNum, load, rss, vms, pp in values[USER_INFO_IDX:]:
                   result[hostname].extend([allocCore, load, rss, vms])

        return result

    #data1=self.pyslurmNodeData, data2=self.data
    def getNodeUtilData (self, data1, data2):
        rawData = self.getCPUMemData (data1, data2)

        result  = []
        for hostname, values in rawData.items():
            if len(values) > 7:
               cpu_util = values[7] / values[0] #load/cpus
               result.append([hostname, cpu_util])
            else:
               result.append([hostname, 0])
        return repr(result)
                   
    # return a list of jobs with attribute long_label and disabled
    # generate jobs info {job_id, long_label, disabled}
    def getJobsWithLabel (self):
        jobs    = []
        for jid, jobinfo in self.currJobs.items():
            if jobinfo['tres_alloc_str']:  # alloc resource
               long_label = '{}({}) {} with {}'.format       (jid, jobinfo['user'], jobinfo['job_state'], jobinfo['tres_alloc_str'])
               disabled   = ""
            else:                          # wait resource
               long_label = '{}({}) {} waiting for {}'.format(jid, jobinfo['user'], jobinfo['job_state'], jobinfo['tres_req_str'])
               disabled   = "disabled"
            jobs.append({"job_id":jid, "long_label":long_label, "disabled":disabled})
        return jobs

    #generate users info {user, jobs, long_label}
    def getUsersWithLabel (self):
        users_dict = defaultdict(list)
        for jid, jobinfo in self.currJobs.items():
            if jobinfo['tres_alloc_str']: 
               users_dict[jobinfo['user']].append (jid)
        users = [ {"user":user, "jobs":jobs, "long_label":'{} with running jobs {}'.format(user, jobs)} for user, jobs in sorted(users_dict.items())]
        return users

    def getHeatmapData (self, gpudata):
        node2job= self.node2jobs

        jobs    = self.getJobsWithLabel ()  #jobs list
        users   = self.getUsersWithLabel()  #users list
        workers = []  #dataset1 in heatmap
        for hostname, hostinfo in sorted(self.data.items()):
            try:
               alloc_jobs = node2job.get(hostname, [])
               if 'ALLOCATED' in hostinfo[0] or 'MIXED' in hostinfo[0]:  #in use
                  state    = 1
               elif 'IDLE' in hostinfo[0]:                               #ready to use
                  state  = 0
               else:
                  state  = -1
               cpu_load = 0
               if len(hostinfo) > USER_INFO_IDX: #ALLOCATED, MIXED
                  cpu_load   = hostinfo[USER_INFO_IDX][4]

               node_cores   = self.pyslurmNodeData[hostname]['cpus']
               node_gpus,used_gpus    = MyTool.getGPUCount(self.pyslurmNodeData[hostname]['gres'], self.pyslurmNodeData[hostname]['gres_used'])
               job_accounts = [self.currJobs[jid].get('account', None)        for jid in alloc_jobs]
               job_users    = [MyTool.getUser(self.currJobs[jid]['user_id'])  for jid in alloc_jobs]
               job_cores    = [self.currJobs[jid]['cpus_allocated'][hostname] for jid in alloc_jobs]  #

               gpus         = {}   #{'gpu0':{'label':,'state':}}
               if node_gpus:  #has GPU
                  nodeLabel = '{} ({} cpus, {} gpus)'.format(hostname, node_cores, node_gpus)
                  if state==1:
                     nodeLabel = '{}: cpu_util {:.1%}, gpu_used {}'.format(nodeLabel, cpu_load/node_cores, used_gpus)
                     job_gpus  = [self.currJobs[jid]['gpus_allocated'].get(hostname,[]) for jid in alloc_jobs]  #list of gpu indexes[]
                     lst       = list(zip(alloc_jobs, job_users, job_cores, job_gpus))
                     label     = '{} jobs:{}'.format(nodeLabel, lst)
                  else:
                     nodeLabel = '{}: state {},'.format(nodeLabel, hostinfo[0])
                     if cpu_load:
                        nodeLabel = '{} cpu_util {:.1%},'.format(nodeLabel, cpu_load/node_cores)
                     label     = nodeLabel
                  
                  gpu2jid=dict([(gpu_idx, jid) for jid in alloc_jobs for gpu_idx in self.currJobs[jid]['gpus_allocated'].get(hostname,[])]) #TODO: assume gpu is not shared, gpus_allocated': {'workergpu14': [(0, 2)]
                  for i in range(0, node_gpus):
                      gpu_name  = 'gpu{}'.format(i)
                      gpu_state = state     # inherit node's state
                      state_str = hostinfo[1]
                      jid       = 0
                      if (gpu_state==1) and (i not in gpu2jid):
                         gpu_state, state_str = 0, "IDLE"
                      if gpu_state==1:
                         jid       = gpu2jid[i]
                         jobInfo   = self.currJobs[jid]
                         gpu_alloc = jobInfo['gpus_allocated'].get(hostname,[])
                         gpu_label = '{}:{} gpu_util {:.1%}, job ({},{},{} cpus,{} gpus {})'.format(hostname, gpu_name, gpudata[gpu_name][hostname], jid, MyTool.getUser(jobInfo['user_id']), jobInfo['cpus_allocated'][hostname], len(gpu_alloc), gpu_alloc)
                      else:
                         gpu_label = '{}:{} state:{}'.format(hostname, gpu_name, state_str)
                      gpus[gpu_name] = {'label':gpu_label, 'state': gpu_state, 'job': jid}       
               else:
                  nodeLabel = '{} ({} cpus)'.format(hostname, node_cores)
                  if state==1:
                     nodeLabel = '{}: cpu_util {:.1%}'.format(nodeLabel, cpu_load/node_cores)
                     lst       = list(zip(alloc_jobs, job_users, job_cores))
                     label     = '{} jobs:{}'.format(nodeLabel, lst)
                  else:
                     nodeLabel = '{}: state {},'.format(nodeLabel, hostinfo[0])
                     if cpu_load:
                        nodeLabel = '{} cpu_util {:.1%},'.format(nodeLabel, cpu_load/node_cores)
                     label     = nodeLabel

               workers.append([hostname, state, node_cores, cpu_load, alloc_jobs, job_accounts, label, gpus])
            except Exception as exp:
               print("ERROR getHeatmapData: {0}".format(exp))
                               
        return workers,jobs,users
                   
    @cherrypy.expose
    def utilHeatmap(self, **args):
        if type(self.data) == str: return self.getWaitMsg() # error of some sort.

        gpudata            = BrightRestClient().getLatestAllGPU()
        workers,jobs,users = self.getHeatmapData (gpudata)
        
        htmltemp = os.path.join(wai, 'heatmap.html')
        h        = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.updateTS).ctime(), 'data1': workers, 'data2': jobs, 'users':users, 'gpu':gpudata}
 
        return h 
        #return repr(data)

    @cherrypy.expose
    def forecast(self, page=None):
        htmltemp = os.path.join(wai, 'forecast.html')
        h = open(htmltemp).read()
 
        return h

    @cherrypy.expose
    def report(self, page=None):
        htmltemp = os.path.join(wai, 'report.html')
        h = open(htmltemp).read()
 
        return h

    @cherrypy.expose
    def getHeader(self, page=None):
        pages =["index",           "utilHeatmap", "pending",      "sunburst",       "usageGraph", "tymor2", "report", "inputSearch", "forecast"]
        titles=["Tabular Summary", "Host Util.",  "Pending Jobs", "Sunburst Graph", "File Usage","Tymor", "Report", "Search", "Forecast"]
 
        result=""
        for i in range (len(pages)):
           if ( pages[i] == page ):
              result += '<button class="tablinks active"><a href="/' + pages[i] + '">' + titles[i] + '</a></button>'
           else:
              result += '<button class="tablinks"><a href="/' + pages[i] + '">' + titles[i] + '</a></button>'

        return result

    @cherrypy.expose
    def index(self, **args):
        if type(self.data) == str: return self.getWaitMsg() # error of some sort.

        tableData = self.getSummaryTableData(self.data, self.currJobs, self.node2jobs)
        htmltemp  = os.path.join(wai, 'index3.html')
        h         = open(htmltemp).read()%{'tableData' : tableData, 'update_time': datetime.datetime.fromtimestamp(self.updateTS).ctime()}
        return h

    @cherrypy.expose
    def getRawData (self):
        l = [(w, len(info)) for w, info in self.rawData.items() if len(info)>3]
        return "{}\n{}".format(l, self.rawData)

    @cherrypy.expose
    def getJobNode2ProcRecord (self, jid):
        return "{}".format(self.jobNode2ProcRecord[int(jid)])

    @cherrypy.expose
    def getJobNode2ProcRecord_1 (self):
        return "{}".format(self.jobNode2ProcRecord_1)

    def addAttr2CurrJobs (self, ts, jobs={}, low_util=0.01, long_period=ONE_DAY_SECS, job_width=1, low_mem=0.3):
        #check self.currJobs and locate those jobs in question
        #TODO: 09/09/2019: add jid
        for jid, job in jobs.items():
            period            = ts - job['start_time']
            total_cpu_time    = 0
            total_rss         = 0
            total_node_mem    = 0           #proportional mem for shared nodes
            total_io_bps      = 0           
            total_cpu_util_curr = 0
            for node in job['cpus_allocated']:
                #check self.jobNode2ProcRecord to add up cpu_time and get utilization
                if (jid in self.jobNode2ProcRecord) and (node in self.jobNode2ProcRecord[jid]):
                   savTs, procs    = self.jobNode2ProcRecord[jid][node]
                   total_cpu_time += sum([ts_proc['cpu_time']    for pid,ts_proc in procs.items()])
                   total_rss      += sum([ts_proc['mem_rss_K']   for pid,ts_proc in procs.items()])
                   total_io_bps   += sum([ts_proc['io_bps_curr'] for pid,ts_proc in procs.items()])
                   total_cpu_util_curr += sum([ts_proc['cpu_util_curr'] for pid,ts_proc in procs.items()])
                   s               = self.pyslurmNodeData[node]['tres_fmt_str']
                   if s:
                      d = dict([s1.split('=') for s1 in s.split(',')])
                      if 'mem' in d:
                         if 'cpu' in d:
                            node_cpu = int(d['cpu'])
                            job_cpu  = int(job['cpus_allocated'][node])
                            prop     = job_cpu / node_cpu         # for shared cpu
                         else:
                            prop     = 1
                         total_node_mem += MyTool.convert2K(d['mem']) * prop
                      else:
                         print('ERROR: Node {} does not have mem {} in tres_fmt_str {}'.format(node, d, s))
                   else:
                      print('ERROR: Node {} does not have tres_fmt_str'.format(node))
                else:
                   if jid not in self.jobNode2ProcRecord:
                      print('ERROR: Job {} ({}) is not in self.jobNode2ProcRecord'.format(jid, job['nodes'])) 
                   else:
                      print('ERROR: Node {} of Job {} ({}) is not in self.jobNode2ProcRecord'.format(node, jid, job['nodes'])) 
               
            job['user']         = MyTool.getUser(job['user_id'])
            job['job_io_bps']   = total_io_bps
            job['job_inst_util']= total_cpu_util_curr
            if total_cpu_time: # has process informatoin
                  job['job_avg_util'] = total_cpu_time / period / job['num_cpus'] 
                  job['job_mem_util'] = total_rss / total_node_mem
            else: # no process information
                  #print('WARNING: Job {} does not have proc on nodes {}'.format(jid, job['nodes']))
                  job['job_avg_util'] = 0
                  job['job_mem_util'] = 0
            node_list = [self.pyslurmNodeData[node] for node in job['cpus_allocated']]
            job['gpus_allocated'] = MyTool.getGPUAlloc_layout(node_list, job['gres_detail'])
 
        return jobs

    @cherrypy.expose
    def getLowResourceJobs (self, job_length_secs=ONE_DAY_SECS, job_width_cpus=1, job_cpu_avg_util=0.1, job_mem_util=0.3):
        job_dict = self.getLongrunLowUtilJobs(self.updateTS, self.currJobs, float(job_cpu_avg_util), int(job_length_secs), int(job_width_cpus), float(job_mem_util))
        return json.dumps([self.updateTS, job_dict])

    def getLongrunLowUtilJobs (self, ts=1567635300, jobs={}, low_util=0.01, long_period=ONE_DAY_SECS, job_width=1, low_mem=0.3):
        #check self.currJobs and locate those jobs in question
        if not jobs:
           jobs   = self.currJobs
        result = {}            # return {jid:job,...}
        for jid, job in jobs.items():
            #if job run long enough
            period = ts - job['start_time']
            if (period > long_period) and (job.get('num_cpus',1)>job_width) and (job['job_avg_util'] < low_util) and (job['job_mem_util']<low_mem) and (job['job_inst_util'] < low_util*10):
               result[job['job_id']] = job
 
        return result

    @cherrypy.expose
    def getUnbalancedJobs (self, job_cpu_avg_util=0.1, job_mem_util=0.3, job_io_bps=1000000):
        jobs   = self.currJobs
        ts     = self.updateTS
        job_cpu_avg_util = float(job_cpu_avg_util)
        job_mem_util     = float(job_mem_util)
        job_io_bps       = int(job_io_bps)

        result = {}            # return {jid:job,...}
        for jid, job in jobs.items():
            #if job run long enough
            if (job['job_avg_util'] < job_cpu_avg_util) and (job['job_mem_util']>job_mem_util or job['job_io_bps'] > job_io_bps):
               result[job['job_id']] = job
        print('getUnbalancedJobs {}'.format(result.keys()))
        return json.dumps([ts, result])

    @cherrypy.expose
    def getUnbalLoadJobs (self, cpu_stdev, rss_stdev, io_stdev):
        cpu_stdev, rss_stdev, io_stdev = int(cpu_stdev), int(rss_stdev), int(io_stdev)
        self.calculateStat (self.currJobs, self.data)
        sel_jobs = [(jid, job) for jid, job in self.currJobs.items() 
                         if (job['node_cpu_stdev']>cpu_stdev) or (job['node_rss_stdev']>rss_stdev) or (job['node_io_stdev']>io_stdev)]
        return json.dumps ([self.updateTS, dict(sel_jobs)])

    #for a job, caclulate the deviaton of the cpu, mem, rss
    def calculateStat (self, jobs, nodes):
        for jid, job in jobs.items():
            if 'node_cpu_stdev' in job:     # already calculated
               return

            if job['num_nodes'] == 1:
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = 0,0,0     #cpu util, rss in KB, io bps
               continue
            #[u_name, uid, allocated_cpus, len(pp), totIUA_util, totRSS, totVMS, pp, totIO, totCPU_rate]
            proc_cpu=[proc[4] for node in MyTool.nl2flat(job['nodes']) for proc in nodes[node][3:]]
            proc_rss=[proc[5] for node in MyTool.nl2flat(job['nodes']) for proc in nodes[node][3:]]
            proc_io =[proc[8] for node in MyTool.nl2flat(job['nodes']) for proc in nodes[node][3:]]
            if len(proc_cpu) > 1:
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = MyTool.pstdev(proc_cpu), MyTool.pstdev(proc_rss)/1024, MyTool.pstdev(proc_io)     # cpu util
            else:
               #print('WARNING: Job {} has not enough process running on allocated nodes {} ({}) to calculate standard deviation.'.format(jid, job['nodes'], proc_cpu))
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = 0,0,0

    @cherrypy.expose
    def updateSlurmData(self, **args):
        #updated the data
        d =  cherrypy.request.body.read()
        #jobData and pyslurmNodeData comes from pyslurm
        self.updateTS, jobs, hn2info, self.pyslurmNodeData = cPickle.loads(zlib.decompress(d))
        self.updateTS = int(self.updateTS)
        self.allJobs  = jobs
        self.currJobs = dict([(jid,job) for jid, job in jobs.items() if job['job_state'] in ['RUNNING', 'CONFIGURING']])
        self.rawData = hn2info

        self.updateNode2Jobs (self.currJobs)
        if type(self.data) != dict: self.data = {}  #may have old data from last round
        for node,nInfo in hn2info.items(): 
            if self.updateTS - int(nInfo[2]) > 600: # Ignore data
               print("WARNING updateSlurmData: ignore old data of node {} at {}".format(node, MyTool.getTsString(nInfo[2]))) 
               continue
  
            #set the value of self.data
            self.data[node] = nInfo      #nInfo: status, delta, ts, procsByUser
            if len(nInfo) > USER_INFO_IDX and nInfo[USER_INFO_IDX]:
               for procsByUser in nInfo[USER_INFO_IDX:]:   #worker may has multiple users
                                                           #user_name, uid, hn2uid2allocated.get(hostname, {}).get(uid, -1), len(pp), totIUA, totRSS, totVMS, procs, totIO, totCPU])
                  #update the latest cpu_time for each proc
                  if len(procsByUser) > USER_PROC_IDX and procsByUser[USER_PROC_IDX]:
                     #09/09/2019, add jid to proc
                     jids     = set([proc[9] for proc in procsByUser[USER_PROC_IDX]])
                     jid2proc = defaultdict(list)
                     for proc in procsByUser[USER_PROC_IDX]:  
                         jid2proc[proc[9]].append(proc)
                     for jid in jids:
                         self.updateJobNode2ProcRecord (nInfo[2], jid, node, jid2proc[jid])  #nInfo[2] is ts
                     #jids         = self.uid2jids (procsByUser[1], node)
                     #for jid in jids: #TODO: user has multiple jobs on the same worker
                     #   self.updateJobNode2ProcRecord (nInfo[2], jid, node, procsByUser[USER_PROC_IDX])  #nInfo[2] is ts
            #TODO: total jids != self.node2jobs[node]
            elif 'ALLOCATED' in nInfo[0] or 'MIXED' in nInfo[0]: # no proc information reported
               print("WARNING updateSlurmData: {} - {}, no proc information".format(node, nInfo[0]))
               for jid in self.node2jobs[node]: 
                   self.updateJobNode2ProcRecord (nInfo[2], jid, node, [])  #nInfo[2] is ts
                
        self.addAttr2CurrJobs(self.updateTS, self.currJobs)          #add attribute job_avg_util, job_mem_util, job_io_bps to self.currJobs
        self.inMemCache.append(self.data, self.updateTS, self.allJobs)

        #check for long run low util jobs and send notice
        low_util = self.getLongrunLowUtilJobs(self.updateTS, self.currJobs)
        print('low_util={}'.format(low_util.keys()))
        if self.config['email']['flag']:
           self.jobNoticeSender.sendNotice(self.updateTS, low_util)
        
    def sacctData (self, criteria):
        cmd = ['sacct', '-n', '-P', '-o', 'JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'] + criteria
        try:
            #TODO: capture standard error separately?
            d = SUB.check_output(cmd, stderr=SUB.STDOUT)
        except SUB.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        return d.decode('utf-8')

    def sacctDataInWindow(self, criteria, day_cnt=SACCT_WINDOW_DAYS):
        t = datetime.date.today() + datetime.timedelta(days=-day_cnt)
        startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
        d = self.sacctData (['-S', startDate] + criteria)
        #print(repr(d))

        return d

    @cherrypy.expose
    def sacctReport(self, d, titles=['Job ID', 'Job Name', 'Allocated CPUS', 'State', 'Exit Code', 'User', 'Node List', 'Start', 'End'], skipJobStep=True):
        t = '<table class="slurminfo"><tbody><tr>'
        for th in titles: t += '<th>{}</th>'.format(th)
        t += '</tr>'
        
        jid2info = defaultdict(list)
        for l in d.splitlines():
            if not l: continue
            ff = l.split(sep='|')
            if (skipJobStep and '.' in ff[0]): continue # indicates a job step --- under what circumstances should these be broken out?
            if ( '.' in ff[0] ):
               ff0 = ff[0].split(sep='.')[0]
            else:
               ff0 = ff[0]

            f0p = ff0.split(sep='_')
            try:
                jId, aId = int(f0p[0]), int(f0p[1])
            except:
                jId, aId = int(f0p[0]), -1
            if ff[3].startswith('CANCELLED by '):
                uid = ff[3].rsplit(' ', 1)[1]
                try:
                    uname = pwd.getpwuid(int(uid)).pw_name
                except:
                    uname = '???'
                ff[3] = '%s (%s)'%(ff[3], uname)
            jid2info[jId].append((aId, ff))

        for jId, parts in sorted(jid2info.items(), reverse=True):
            for aId, ff in sorted(parts):
               t += '<tr><td>' + '</td><td>'.join(ff) + '</tr>\n'
        t += '\n</tbody>\n</table>\n'
        return t

    @cherrypy.expose
    def usageGraph(self, yyyymmdd=''):
        if not yyyymmdd:
            # only have census date up to yesterday, so we use that as the default.
            yyyymmdd = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        usageData_dict= fs2hc.gendata(yyyymmdd)
        for k,v in usageData_dict.items():
            v[2] = datetime.datetime.strptime(v[2],'%Y%m%d').strftime('%Y-%m-%d')
        htmlTemp = 'fileCensus.html'
        h = open(htmlTemp).read()%{'file_systems':fs2hc.FileSystems, 'yyyymmdd': 'yyyymmdd', 'data': usageData_dict}

        return h

    @cherrypy.expose
    def user_fileReport(self, user, start='', stop='', days=180):
        # click from File Usage
        start, stop   = MyTool.getStartStopTS (start, stop, '%Y-%m-%d', int(days))
        fc_seq,bc_seq = fs2hc.gendata_user(user, start, stop)
        
        htmlTemp = os.path.join(wai, 'userFile.html')
        h        = open(htmlTemp).read()%{
                                   'start':   time.strftime(DATE_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop':    time.strftime(DATE_DISPLAY_FORMAT, time.localtime(stop)),
                                   'spec_title': user,
                                   'series1': fc_seq,   
                                   'series2': bc_seq}
        return h

    @cherrypy.expose
    def fileReport_daily(self, fs='home', start='', stop='', top=5):
        start, stop  = MyTool.getStartStopTS (start, stop, '%Y-%m-%d')
        fcSer, bcSer = fs2hc.gendata_all(fs, start, stop, int(top))
        if not fcSer:
           return EMPTYDATA_MSG 

        start        = fcSer[0]['data'][0][0]/1000
        stop         = fcSer[0]['data'][-1][0]/1000
    
        htmlTemp = os.path.join(wai, 'seriesHC.html')
        h        = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': fcSer, 'title1': 'File count daily report', 'xlabel1': 'File Count', 'aseries1':[], 
                                   'series2': bcSer, 'title2': 'Byte count daily report', 'xlabel2': 'Byte Count',   'aseries2':[]}
        return h

    @cherrypy.expose
    def nodeGraph(self, node,start='', stop=''):
        start, stop = MyTool.getStartStopTS (start, stop)
        
        msg = self.nodeGraph_cache(node, start, stop)
        note  = 'cache'
        if not msg:
           print('Node {}: no data during {}-{} in cache'.format(node, start, stop))
           msg = self.nodeGraph_influx(node, start, stop)
           note  = 'influx'
        if not msg:
           print('Node {}: no data during {}-{} returned from influx'.format(node, start, stop))
           msg = self.nodeGraph_file(node, start, stop)
           note  = 'file'
        if not msg:
           return 'Node {}: no data during {}-{} in cache, influx and saved file'.format(node, start, stop)

        #ann_series = self.queryTxtClient.getNodeUpTS([node])[node]
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms
        if len(msg)==6:
           htmltemp = os.path.join(wai, 'nodeGraph.html')
           h = open(htmltemp).read()%{'spec_title': ' of {}'.format(node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_r' : msg[4],
                                   'iseries_w' : msg[5]}
        else:
           htmltemp = os.path.join(wai, 'nodeGraph_2.html')
           h = open(htmltemp).read()%{'spec_title': ' of {}'.format(node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_rw': msg[4]}
        return h
  
    @cherrypy.expose
    def nodeGraph_cache(self, node, start=None, stop=None):
        nodeInfo, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes= self.inMemCache.queryNode(node, start, stop)
        if nodeInfo:
           return nodeInfo['first_ts'], nodeInfo['last_ts'], cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes
        else:
           return None

    @cherrypy.expose
    def nodeGraph_influx(self, node, start='', stop=''):
        if not start and not stop:
           start, stop = MyTool.getStartStopTS (start, stop)

        # highcharts
        #influxClient = InfluxQueryClient()
        influxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
        uid2seq,start,stop = influxClient.getSlurmNodeMonData(node,start,stop)
        if not uid2seq:
           return None

        cpu_series,mem_series,io_series_r,io_series_w = [],[],[],[]
                                                 ##[{'data': [[1531147508000, value]...], 'name':'userXXX'}, ...] 
        for uid, d in uid2seq.items():
            uname = MyTool.getUser(uid)
            cpu_series.append  ({'name': uname, 'data':[[ts, d[ts][0]] for ts in d.keys()]})
            mem_series.append  ({'name': uname, 'data':[[ts, d[ts][3]] for ts in d.keys()]})
            io_series_r.append ({'name': uname, 'data':[[ts, d[ts][1]] for ts in d.keys()]})
            io_series_w.append ({'name': uname, 'data':[[ts, d[ts][2]] for ts in d.keys()]})

        return start, stop, cpu_series, mem_series, io_series_r, io_series_w

    def nodeGraph_file(self, node, start='', stop=''):
        hostData = IndexedHostData(self.config["fileStorage"]["dir"])
        cpu_all_seq, mem_all_seq, io_all_seq = hostData.queryDataHosts([node], start, stop)
        return start, stop, cpu_all_seq, mem_all_seq, io_all_seq

    def getNodeProc (self, node):
        if node not in self.data:
           return None, None, None

        #get node data from self.pyslurmNodeData
        pyslurmNode = self.pyslurmNodeData[node]  #'name', 'state', 'cpus', 'alloc_cpus', 
        newNode     = MyTool.sub_dict(pyslurmNode, ['name','cpus','alloc_cpus'])
        newNode['gpus'],newNode['alloc_gpus'] = MyTool.getGPUCount(pyslurmNode['gres'], pyslurmNode['gres_used'])

        #get data from self.data
        newNode['state']    = self.data[node][0]
        newNode['updateTS'] = self.data[node][2]
        #organize procs by job
        newNode['jobProc']  = defaultdict(lambda: {'job':{}, 'procs':[]})       #jid, {'job': , 'procs': }
        newNode['procCnt']  = 0
        for user in sorted(self.data[node][USER_INFO_IDX:]):
            for proc in user[7]:                          #user, uid, cpuCnt, procCnt, totCPURate, totRSS, totVMS, procs, totIOBps, totCPUTime
                newNode['jobProc'][proc[9]]['procs'].append([proc[i] for i in [0,1,5,6,8,7]])  #[pid(0), CPURate/1, create_time, user_time, system_time, rss/5, 'vms'/6, cmdline/7, IOBps/8, jid, read_bytes, write_bytes]
                newNode['procCnt'] += 1
        
        if -1 in newNode['jobProc']:  #TODO: deal with -1 jodid
           newNode['jobProc']['undefined'] = newNode['jobProc'][-1]
           del newNode['jobProc'][-1]

        #get data from self.currJobs
        for jid in newNode['jobProc']:
            if jid in self.currJobs:
               newNode['jobProc'][jid]['job'] = dict((k,v) for k, v in self.currJobs[jid].items() if v and v != True)
            else: 
               print("Job {} on node {}({}) is not in self.currJobs={}".format(jid, node, list(newNode['jobProc'].keys()), list(self.currJobs.keys())))
        newNode['jobProc']=dict(newNode['jobProc'])  #convert from defaultdict to dict
        newNode['jobCnt'] =len(newNode['jobProc'])
        newNode['alloc_cpus'] =sum([self.currJobs[jid]['cpus_allocated'][node] for jid in newNode['jobProc'] if jid in self.currJobs])
        
        jobCPUAlloc = dict([(jid, self.currJobs[jid]['cpus_allocated'][node]) for jid in self.node2jobs[node]])  #'cpus_allocated': {'worker1011': 28}

        return newNode

    @cherrypy.expose
    def nodeDetails(self, node):
        if type(self.data) == str: return self.getWaitMsg()# error of some sort.

        nodeData    = self.getNodeProc(node)
        nodeDisplay = MyTool.sub_dict_exist(self.pyslurmNodeData[node], ['features','partitions','tres_fmt_str','alloc_cpus'])
        if nodeData['gpus']:
           nodeDisplay['gpus'] = nodeData['gpus']
        if nodeData['alloc_gpus']:
           nodeDisplay['alloc_gpus'] = nodeData['alloc_gpus']
        if nodeData['jobProc']:
           nodeDisplay['running_jobs'] = list(nodeData['jobProc'].keys())
    
        nodeReport  = SlurmCmdQuery.sacct_getNodeReport(node, days=3)
        if not nodeData:
           return "Node {} is not monitored".format(node)
        array_het_jids = [ job['JobID'] for job in nodeReport if '_' in job['JobID'] or '+' in job['JobID']]

        htmlTemp   = os.path.join(wai, 'nodeDetail.html')
        htmlStr    = open(htmlTemp).read().format(update_time=MyTool.getTsString(nodeData['updateTS']), 
                                                  node_data=nodeData,
                                                  node_display_data=nodeDisplay,
                                                  array_het_jids=array_het_jids,node_report=nodeReport)
        return htmlStr

    @cherrypy.expose
    def userJobs(self, user):
        t = htmlPreamble
        t += '<h3>Running and Pending Jobs of user <a href="./userDetails?user={0}">{0}</a> (<a href="./userJobGraph?uname={0}">Running Jobs Graph</a>)</a></h3>Update time:{1}'.format(user, MyTool.getTsString(int(time.time())))
        t += self.sacctReport(SlurmCmdQuery.sacctCmd(['-u', user, '-s', 'RUNNING,PENDING'], output='JobID,JobName,State,Partition,NodeList,AllocCPUS,Submit,Start'),
                              titles=['Job ID', 'Job Name', 'State', 'Partition','NodeList','Allocated CPUS', 'Submit','Start'])
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def userDetails(self, user, days=3):
        userInfo       = MyTool.getUserStruct (uname=user)
        if not userInfo or not userInfo.pw_uid:
           return 'Cannot find uid of user {}!'.format(user)
        uid            = userInfo.pw_uid
        userAssoc      = SlurmCmdQuery.getUserAssoc(user)
        ins            = SlurmEntities.SlurmEntities()
        userjob        = ins.getUserJobsByState (uid)
        part           = ins.getAccountPartition (userAssoc['Account'], uid)
        for p in part:  #replace big number with n/a
            for k,v in p.items():
                if v == SlurmEntities.SlurmEntities.TCMO: p[k]='n/a'
        if type(self.data) == dict:
           note       = ''
           userworker = self.getUserNodeData(user)
           core_cnt   = sum([val[0] for val in userworker.values()]) 
           proc_cnt   = sum([val[1] for val in userworker.values()])
        else:
           note       = 'Note: {}'.format(self.data)
           userworker = {}
           core_cnt   = 0
           proc_cnt   = 0
        past_job      = SlurmCmdQuery.sacct_getUserJobReport(user, days=int(days))
        array_het_jids= [job['JobID'] for job in past_job if '_' in job['JobID'] or '+' in job['JobID']]  # array of heterogenour job

        running_jobs  = userjob.get('RUNNING',[])
        tres_alloc    = [MyTool.getTresDict(j['tres_alloc_str']) for j in running_jobs]
        userAssoc['uid']        = uid
        userAssoc['partitions'] = [p['name'] for p in part]
        userAssoc['runng_jobs'] = [j['job_id'] for j in userjob.get('RUNNING',[])]
        userAssoc['alloc_cpus'] = sum([t['cpu']  for t in tres_alloc])
        userAssoc['alloc_nodes']= sum([t['node'] for t in tres_alloc])
        userAssoc['alloc_mem']  = MyTool.sumOfListWithUnit([t['mem']  for t in tres_alloc])
               
        htmlTemp   = os.path.join(wai, 'userDetail.html')
        htmlStr    = open(htmlTemp).read().format(user=userInfo.pw_gecos.split(',')[0], uname=user, 
                                                  user_assoc = userAssoc,
                                                  update_time=ins.update_time.ctime(), 
                                                  running_jobs=json.dumps(userjob.get('RUNNING',[])), 
                                                  pending_jobs=json.dumps(userjob.get('PENDING',[])), 
                                                  worker_proc=json.dumps(userworker), note=note,
                                                  part_info=json.dumps(part),
                                                  array_het_jids=array_het_jids, job_history=past_job, day_cnt = days)
        return htmlStr
        # Currently, running jobs & pending jobs of the user processes in the worker nodes
        # Future, QoS and resource restriction
        # Past,   finished or cancelled jobs for the last 3 days
        
    #return dict (workername, workerinfo)
    def getUserNodeData (self, user):
        if type(self.data) == str: return {} # error of some sort.

        result = {}
        for node, d in self.data.items():
            if len(d) < USER_INFO_IDX: continue   # no user info
            for user_name, uid, alloc_core_cnt, proc_cnt, t_cpu, t_rss, t_vms, procs, t_io, *etc in d[USER_INFO_IDX:]:
                if user_name == user:
                   result[node]= [alloc_core_cnt, proc_cnt, t_cpu, t_rss, t_vms, procs, t_io]
        return result

    #return list of list
    #result[node_name]= [alloc_core_cnt, proc_cnt, total_cpu, t_rss, t_vms, rlt_procs, t_io]
    #rlt_procs.append ([pid, intervalCPUtimeAvg, job_avg_cpu, rss, vms, intervalIOByteAvg, cmdline])
    def getJobProc (self, job_info):
        if type(self.data) == str: return {} # error of some sort.

        result = {}
        nodes  = MyTool.nl2flat(job_info['nodes'])
        user   = MyTool.getUser(job_info['user_id'])
        for node_name in nodes:
            d  = self.data[node_name]
            ts = d[2]
            if len(d) < USER_INFO_IDX: continue   # no user info
            for user_name, uid, alloc_core_cnt, proc_cnt, t_cpu, t_rss, t_vms, procs, t_io, *etc in d[USER_INFO_IDX:]:
                if user_name == user:
                   #procs[[pid, intervalCPUtimeAvg, create_time, 'user_time', 'system_time, 'rss', 'vms', 'cmdline', intervalIOByteAvg],...]
                   rlt_procs = []
                   # 09/09/2019 add jid
                   for pid, intervalCPUtimeAvg, create_time, user_time, system_time, rss, vms, cmdline, intervalIOByteAvg, jid, num_fds, *etc in procs:
                       if jid == job_info['job_id']:
                          job_stime   = job_info.get('start_time', 0)
                          job_avg_cpu = (user_time+system_time) / (ts-job_stime) if job_stime > 0 else 0
                          rlt_procs.append ([pid, '{:.2f}'.format(intervalCPUtimeAvg), '{:.2f}'.format(job_avg_cpu), MyTool.getDisplayB(rss), MyTool.getDisplayB(vms), MyTool.getDisplayBps(intervalIOByteAvg), num_fds, ' '.join(cmdline)])
                   result[node_name]= [int(alloc_core_cnt), int(proc_cnt), t_cpu, t_rss, t_vms, rlt_procs, t_io]
        return result, ['PID', 'Inst CPU Util', 'Avg CPU Util', 'RSS', 'VMS', 'IO Rate', 'Num Fds', 'Command']

    @cherrypy.expose
    def jobByName(self, name='script', curr_jid=None):
        fields    =['id_job','job_name', 'id_user','state', 'nodelist', 'time_start','time_end', 'exit_code', 'tres_req', 'tres_alloc', 'gres_req', 'gres_alloc', 'work_dir']
        data      = SlurmDBQuery().getJobByName(name, fields)  #user, duration is added by the function
        d_flds    = {'id_job':'Job ID', 'state':'State', 'user':'User', 'nodelist':'Alloc Node', 'time_start':'Start', 'time_end':'End', 'duration':'Duration', 'exit_code':'Exit', 'tres_req':'Req Tres', 'gres_req':'Req Gres', 'work_dir':'Work Dir'}
        total_cnt = len(data)
        if len(data) > 100:
           data=data[-100:]
        for d in data:
           d['time_start'] = MyTool.getTsString(d['time_start'])
           d['time_end']   = MyTool.getTsString(d['time_end'])

        htmlTemp   = os.path.join(wai, 'jobByName.html')
        htmlStr    = open(htmlTemp).read().format(job_name=name, job_cnt=total_cnt, job_list=data, job_title=d_flds)
        return htmlStr
        #return '{}\n{}'.format(fields, data)
        
 
    @cherrypy.expose
    def jobDetails(self, jid):
        jid         = int(jid)
        ts          = int(time.time())
        jobs_report = SlurmCmdQuery.sacct_getJobReport(jid)
        job_report  = jobs_report[0]
        if not jobs_report or not job_report: return "Cannot find job {}".format(jid)

        start,nodes_cnt,cpu_cnt,user,job_name = MyTool.str2ts(job_report['Start']), int(job_report['AllocNodes']), int(job_report['AllocCPUS']), job_report['User'], job_report['JobName']
        msg_note    = ''
        worker_proc = {}
        proc_fields = []
        if job_report['State']=='RUNNING':
           if type(self.data) == str: msg_note = self.data 
           else: # should in the data
              ts   = self.updateTS
              if jid not in self.currJobs or not self.currJobs[jid]:
                 return "Cannot find job{} in the current data".format(jid)
              # job data is in self.currJobs
              worker_proc, proc_fields = self.getJobProc (self.currJobs[jid])     #from monitored data        
              if len(worker_proc) != nodes_cnt:
                 msg_note='WARNING: Job {} is running on {} nodes, which is less than {} allocated nodes.'.format(jid, len(worker_proc), nodes_cnt)
        else: # not RUNNING
           end       = int(MyTool.str2ts(job_report['End']))
           if (not end) or (end == 'Unknown'):
              msg_note='Can not find End time for a non-running job'
           else:
              #msg_note='Job start at {} and end at {}'.format(MyTool.getTsString(start), MyTool.getTsString(end))
        
              influxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
              # query influx cpu_proc_info between start and end where uid and hostname
              query, node2procs = influxClient.getUserProc (user, MyTool.nl2flat(job_report['NodeList']), start, end)
              if not node2procs:
                 msg_note="WARNING: no record of user's process has been saved in the database."
              else:
                 worker_proc = {}
                 proc_fields = ['PID', 'Avg CPU Util', 'RSS',  'VMS', 'IO Rate', 'Command']
                 #data_fields = ['pid', 'avg_util',     'mem_rss_K', 'mem_vms_K','avg_io'       , 'cmdline']
                 for node, procs in node2procs.items():
                    worker_proc[node]=[0,len(procs),0,0,0,[],0]
                    for proc in procs:
	             #{'time': 1568554426, 'cmdline': '[]', 'cpu_affinity': '[0, 1]', 'cpu_system_time': 0.64, 'cpu_user_time': 0.2, 'end_time': 1568577027, 'hostname': 'worker1031', 'io_read_bytes': 3629056, 'io_read_count': 4947, 'io_write_bytes': 8396800, 'io_write_count': 1701, 'mem_data': 213565440, 'mem_lib': 0, 'mem_rss': 20221952, 'mem_shared': 13574144, 'mem_text': 4096, 'mem_vms': 364032000, 'name': 'orted', 'num_fds': 166, 'pid': '224668', 'ppid': 224662, 'status': 'sleeping', 'uid': '1431'}
                       worker_proc[node][5].append([proc['pid'], '{:.2f}'.format(proc['avg_util']), MyTool.getDisplayKB(proc['mem_rss_K']), MyTool.getDisplayKB(proc['mem_vms_K']), MyTool.getDisplayBps(proc['avg_io']), proc['cmdline']])
                  
        #grafana_url = 'http://mon8:3000/d/jYgoAfiWz/yanbin-slurm-node-util?orgId=1&from={}{}&var-jobID={}&theme=light'.format(start*1000, '&var-hostname=' + '&var-hostname='.join(MyTool.nl2flat(job_report['NodeList'])), jid)
        proc_cnt   = sum([val[1] for val in worker_proc.values()])
        job_info   = MyTool.sub_dict_exist(job_report, ['State', 'User', 'AllocCPUS', 'NodeList', 'Start', 'End', 'ExitCode'])
        if '_' in job_report['JobID']:
           job_info['ArrayJobID']  = job_report['JobID']
        elif '+' in job_report['JobID']:
           job_info['HeterogeneousJobID']  = job_report['JobID']

        htmlTemp   = os.path.join(wai, 'jobDetail.html')
        htmlStr    = open(htmlTemp).read().format(job_id=jid, job_name=job_name, 
                                                  update_time=datetime.datetime.fromtimestamp(ts).ctime(),
        #                                          grafana_url=grafana_url,
                                                  job_info=job_info,
                                                  worker_proc=worker_proc, title_list=proc_fields,
                                                  worker_cnt=nodes_cnt, core_cnt=cpu_cnt, proc_cnt=proc_cnt,note=msg_note,
                                                  job_report=jobs_report)
        return htmlStr

    @cherrypy.expose
    def tymor(self,**args):
        if type(self.data) == str: return self.data # error of some sort.

        selfdata_o = self.data
        data_o     = self.currJobs
        selfdata   = {k:v for k,v in selfdata_o.items()}
        data       = {k:v for k,v in data_o.items()}

        #1: bar chart/pie chart data- this aggregates the node states, could also do for summing aggregate load,RSS,VMS
        x = [selfdata[j][0] for j in list(selfdata)]
        y = {i:x.count(i) for i in set(x)}
        #2:QOS bars: (table of nodes/cores by qos (i.e. ccb, cca, ))
        zqos   =[data[x][u'qos'] for x in list(data)]
        zcpus  =[data[x][u'num_cpus'] for x in list(data)]
        znodes =[data[x][u'num_nodes'] for x in list(data)]
        d = {}
        k = list(zip(zqos, zcpus))
        for (x,y) in k:
            if x in d:
                d[x] = d[x] + y 
            else:
                d[x] = y
        
        #1 Getting the data in the right form for the job view. Selfdata is the node information, we want to
        #turn the complicated nested lists into something simple
        more_data      ={i:selfdata[i] for i in selfdata if len(selfdata[i]) > USER_INFO_IDX}
        more_data_clean={i:more_data[i][0:USER_INFO_IDX]+ more_data[i][j][0:7] for i in more_data for j in range(USER_INFO_IDX,len(more_data[i])) if more_data[i][j][2]>=0 }
        idle_data1     ={i:selfdata[i][0:USER_INFO_IDX] for i in selfdata if len(selfdata[i])<=USER_INFO_IDX}
        idle_data2     ={i:more_data[i] for i in (set(more_data)-set(more_data_clean))}
        less_data      =dict(idle_data1,**idle_data2)
        selfdata_dict  =dict(more_data_clean,**less_data)
        
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in data.items():
            x  = [node for node, coreCount in jinfo.get(u'cpus_allocated').items()]
            d1 = {k: selfdata_dict[k] for k in x}
            data[jid]["node_info"] = d1

        keys_id  =(u'job_id',u'user_id',u'qos', u'nodes','node_info',u'num_nodes', u'num_cpus',u'run_time',u'run_time_str',u'start_time')
        data_dash={i:{k:data[i][k] for k in keys_id} for i in list(data)}

        for n, v in sorted(data_dash.items()):
            nodes =list(v['node_info'])
            nodes.sort(key=MyTool.natural_keys)
            data_dash[n]['list_nodes']    = nodes
            data_dash[n]['usernames_list']=[v['node_info'][i][USER_INFO_IDX] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['username']      =MyTool.most_common(v['usernames_list'])
            data_dash[n]['list_state']    =[v['node_info'][i][0] for i in nodes]
            data_dash[n]['list_cores']    =[round(v['node_info'][i][5],3) for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_load']     =[round(v['node_info'][i][7],3) for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_RSS']      =[v['node_info'][i][8] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_VMS']      =[v['node_info'][i][9] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_core_load_diff']=[round(v['node_info'][i][5] -v['node_info'][i][7],3) for i in nodes if len(v['node_info'][i])>=7]
                
        t = '''
<thead><tr><th>JobID</th><th>UserID</th><th>Nodes</th><th>CPUs</th><th>List_load_stndrd</th><th>List_load_diff_stndrd</th><th>List_VMS_stndrd</th><th>List_RSS_stndrd</th></tr></thead>
<tbody id="tbody-sparkline">
''' 
        t2 = '''
<thead><tr><th>JobID</th><th>UserID</th><th>Nodes</th><th>CPUs</th><th>List_load</th><th>List_core_load_diff</th><th>List_VMS</th><th>List_RSS</th></tr></thead><tbody id="tbody-sparkline">
'''

        list_nodes     =[data_dash[i]['list_nodes']    for i in data_dash]
        list_load      =[data_dash[i]['list_load']     for i in data_dash]
        list_loads_flat=[item for sublist in list_load for item in sublist]
        list_RSS       =[data_dash[i]['list_RSS']      for i in data_dash]
        list_RSS_flat  =[item for sublist in list_RSS  for item in sublist]
        list_VMS       =[data_dash[i]['list_VMS']      for i in data_dash]
        list_VMS_flat  =[item for sublist in list_VMS  for item in sublist]
        list_core_load_diff     =[data_dash[i]['list_core_load_diff']     for i in data_dash]
        list_core_load_diff_flat=[item for sublist in list_core_load_diff for item in sublist]

        load_mean =MyTool.mean(list_loads_flat)
        load_sd   =MyTool.pstdev(list_loads_flat)
        RSS_mean  =MyTool.mean(list_RSS_flat)
        RSS_sd    =MyTool.pstdev(list_RSS_flat)
        VMS_mean  =MyTool.mean(list_VMS_flat)
        VMS_sd    =MyTool.pstdev(list_VMS_flat)
        load_diff_mean=MyTool.mean(list_core_load_diff_flat)
        load_diff_sd  =MyTool.pstdev(list_core_load_diff_flat)

        for n, v in sorted(data_dash.items()):
            data_dash[n]['load_stndrd']=[round((v['list_load'][i]-load_mean)/load_sd,3) for i in range(len(v['list_load']))]
            data_dash[n]['RSS_stndrd']=[round((v['list_RSS'][i]-RSS_mean)/RSS_sd,3) for i in range(len(v['list_RSS']))]
            data_dash[n]['VMS_stndrd']=[round((v['list_VMS'][i]-VMS_mean)/VMS_sd,3) for i in range(len(v['list_VMS']))]
            data_dash[n]['load_diff_stndrd']=[round((v['list_core_load_diff'][i]-load_diff_mean)/load_diff_sd,3) for i in range(len(v['list_core_load_diff']))]

        for i in data_dash:
            jid      =data_dash[i][u'job_id']
            uid      =data_dash[i]['username']
            nodes    =data_dash[i]['nodes']
            num_cpus =data_dash[i][u'num_cpus']

            list_nodes           =", ".join(map(str,data_dash[i]['list_nodes']))
            list_load            =", ".join(map(str,data_dash[i]['list_load']))+" "+"; column"
            list_core_load_diff  =", ".join(map(str,data_dash[i]['list_core_load_diff']))+" "+"; column"
            list_VMS             =", ".join(map(str,data_dash[i]['list_VMS']))+" "+"; column"
            list_RSS             =", ".join(map(str,data_dash[i]['list_RSS']))+" "+"; column"
            list_load_stndrd     =", ".join(map(str,data_dash[i]['load_stndrd']))+" "+"; column"
            list_RSS_stndrd      =", ".join(map(str,data_dash[i]['RSS_stndrd']))+" "+"; column"
            list_VMS_stndrd      =", ".join(map(str,data_dash[i]['VMS_stndrd']))+" "+"; column"
            list_load_diff_stndrd=", ".join(map(str,data_dash[i]['load_diff_stndrd']))+" "+"; column"

            t  += '<tr><td><a href="%s/jobGraph?jobid=%s">%s</a></td><td><a href="%s/userDetails?user=%s">%s</a></td><td>%s</td><td>%s</td><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td class="categories" style="display:none;">%s</td></tr>\n'%(cherrypy.request.base,jid,jid,cherrypy.request.base,uid,uid,nodes,num_cpus,list_load_stndrd,list_load_diff_stndrd,list_VMS_stndrd,list_RSS_stndrd,list_nodes)
            t2 += '<tr><td><a href="%s/jobGraph?jobid=%s">%s</a></td><td><a href="%s/userDetails?user=%s">%s</a></td><td>%s</td><td>%s</td><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"\
/><td data-sparkline="%s"/><td class="categories" style="display:none;">%s</td></tr>\n'%(cherrypy.request.base,jid,jid,cherrypy.request.base,uid,uid,nodes,num_cpus,list_load,list_core_load_diff,list_VMS,list_RSS,list_nodes)

        t  += '</tbody>\n<a href="%s/tymor?refresh=1">&#8635</a>\n'%cherrypy.request.base
        t2 += '</tbody>\n<a href="%s/tymor?refresh=1">&#8635</a>\n'%cherrypy.request.base        

        htmlTemp = os.path.join(wai, 'sparkline.html')
        h        = open(htmlTemp).read()%{'tablespark' : t, 'tablespark2' : t2 }
        
        return h
    
    def getUserData(self):
        hostdata   = {host:v for host,v  in self.data.items()}
        jobdata    = {job:v  for jobid,v in self.currJobs.items()}

        hostUser   = [hostdata[h][4][0] for h in list(hostdata)]
        hostStatus = [hostdata[h][0] for h in list(hostdata)]
        hostSCount = {s:hostStatus.count(s) for s in set(hostStatus)}

        jobQos     = [jobdata[j][u'qos']       for j in list(jobdata)]
        jobCpus    = [jobdata[j][u'num_cpus']  for j in list(jobdata)]
        jobNodes   = [jobdata[j][u'num_nodes'] for j in list(jobdata)]

    @cherrypy.expose
    def tymor2(self,**args):
        if type(self.data) == str: return self.data # error of some sort.

        selfdata_o = self.data
        data_o     = self.currJobs
        selfdata   = {k:v for k,v in selfdata_o.items()}
        data       = {k:v for k,v in data_o.items()}
        #1: bar chart/pie chart data- this aggregates the node states, could also do for summing aggregate load,RSS,VMS                                                              
        x=[selfdata[j][0] for j in list(selfdata)]
        y={i:x.count(i) for i in set(x)}
        #2:QOS bars: (table of nodes/cores by qos (i.e. ccb, cca, ))                                                                                                                 
        zqos  =[data[x][u'qos'] for x in list(data)]
        zcpus =[data[x][u'num_cpus'] for x in list(data)]
        znodes=[data[x][u'num_nodes'] for x in list(data)]
        d = {}
        k = list(zip(zqos, zcpus))
        for (x,y) in k:
            if x in d:
                d[x] = d[x] + y
            else:
                d[x] = y

        #1 Getting the data in the right form for the job view. Selfdata is the node information, we want to                                                                        
        #turn the complicated nested lists into something simple  
        
        more_data={i:selfdata[i] for i in selfdata if len(selfdata[i]) > USER_INFO_IDX}
        more_data_clean={i:more_data[i][0:USER_INFO_IDX]+ more_data[i][j][0:7] for i in more_data for j in range(USER_INFO_IDX,len(more_data[i])) if more_data[i][j][2]>=0 }
        idle_data1={i:selfdata[i][0:USER_INFO_IDX] for i in selfdata if len(selfdata[i])<=USER_INFO_IDX}
        idle_data2={i:more_data[i] for i in (set(more_data)-set(more_data_clean))}
        less_data=dict(idle_data1,**idle_data2)
        selfdata_dict=dict(more_data_clean,**less_data)

        #this appends a dictionary for all of the node information to the job dataset                                                                                                
        for jid, jinfo in data.items():
            x = [node for node, coreCount in jinfo.get(u'cpus_allocated').items()]
            d1 = {k: selfdata_dict[k] for k in x}
            data[jid]["node_info"] = d1

        keys_id=(u'job_id',u'user_id',u'qos', u'nodes','node_info',u'num_nodes', u'num_cpus',u'run_time',u'run_time_str',u'start_time')
        data_dash={i:{k:data[i][k] for k in keys_id} for i in list(data)}

        for n, v in sorted(data_dash.items()):
            nodes=list(v['node_info'])
            nodes.sort(key=MyTool.natural_keys)
            data_dash[n]['list_nodes'] = nodes
            data_dash[n]['usernames_list']=[v['node_info'][i][USER_INFO_IDX] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['username']=MyTool.most_common(v['usernames_list'])
            data_dash[n]['list_state']=[v['node_info'][i][0] for i in nodes]
            data_dash[n]['list_cores']=[round(v['node_info'][i][5],3) for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_load']=[round(v['node_info'][i][7],3) for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_RSS']=[v['node_info'][i][8] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_VMS']=[v['node_info'][i][9] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['list_core_load_diff']=[round(v['node_info'][i][5] -v['node_info'][i][7],3) for i in nodes if len(v['node_info'][i])>=7]

        t = '''                                                                                                                                                                      <thead><tr><th>JobID</th><th>UserID</th><th>Nodes</th><th>CPUs</th><th>List_load_stndrd</th><th>List_load_diff_stndrd</th><th>List_VMS_stndrd</th><th>List_RSS_stndrd</th></tr></thead><tbody id="tbody-sparkline">                                                                                               
        '''
        t2 = '''                                                                                                                                                                     <thead><tr><th>JobID</th><th>UserID</th><th>Nodes</th><th>CPUs</th><th>List_load</th><th>List_core_load_diff</th><th>List_VMS</th><th>List_RSS</th></tr></thead><tbody id="tbody-sparkline">                                                                                                                                                                           
        '''

        list_nodes=[data_dash[i]['list_nodes'] for i in data_dash]
        list_load=[data_dash[i]['list_load'] for i in data_dash]
        list_loads_flat=[item for sublist in list_load for item in sublist]
        list_RSS=[data_dash[i]['list_RSS'] for i in data_dash]
        list_RSS_flat=[item for sublist in list_RSS for item in sublist]
        list_VMS=[data_dash[i]['list_VMS'] for i in data_dash]
        list_VMS_flat=[item for sublist in list_VMS for item in sublist]
        list_core_load_diff=[data_dash[i]['list_core_load_diff'] for i in data_dash]
        list_core_load_diff_flat=[item for sublist in list_core_load_diff for item in sublist]

        load_mean=MyTool.mean(list_loads_flat)
        load_sd=MyTool.pstdev(list_loads_flat)
        RSS_mean=MyTool.mean(list_RSS_flat)
        RSS_sd=MyTool.pstdev(list_RSS_flat)
        VMS_mean=MyTool.mean(list_VMS_flat)
        VMS_sd=MyTool.pstdev(list_VMS_flat)
        load_diff_mean=MyTool.mean(list_core_load_diff_flat)
        load_diff_sd=MyTool.pstdev(list_core_load_diff_flat)

        for n, v in sorted(data_dash.items()):
            data_dash[n]['load_stndrd']=[round((v['list_load'][i]-load_mean)/load_sd,3) for i in range(len(v['list_load']))]
            data_dash[n]['RSS_stndrd']=[round((v['list_RSS'][i]-RSS_mean)/RSS_sd,3) for i in range(len(v['list_RSS']))]
            data_dash[n]['VMS_stndrd']=[round((v['list_VMS'][i]-VMS_mean)/VMS_sd,3) for i in range(len(v['list_VMS']))]
            data_dash[n]['load_diff_stndrd']=[round((v['list_core_load_diff'][i]-load_diff_mean)/load_diff_sd,3) for i in range(len(v['list_core_load_diff']))]
        
        for i in data_dash:
            jid=data_dash[i][u'job_id']
            uid=data_dash[i]['username']
            nodes=data_dash[i]['nodes']
            num_cpus=data_dash[i][u'num_cpus']
            list_nodes=", ".join(map(str,data_dash[i]['list_nodes']))
            list_load=", ".join(map(str,data_dash[i]['list_load']))+" "+"; column"
            list_core_load_diff=", ".join(map(str,data_dash[i]['list_core_load_diff']))+" "+"; column"
            list_VMS=", ".join(map(str,data_dash[i]['list_VMS']))+" "+"; column"
            list_RSS=", ".join(map(str,data_dash[i]['list_RSS']))+" "+"; column"
            list_load_stndrd=", ".join(map(str,data_dash[i]['load_stndrd']))+" "+"; column"
            list_RSS_stndrd=", ".join(map(str,data_dash[i]['RSS_stndrd']))+" "+"; column"
            list_VMS_stndrd=", ".join(map(str,data_dash[i]['VMS_stndrd']))+" "+"; column"
            list_load_diff_stndrd=", ".join(map(str,data_dash[i]['load_diff_stndrd']))+" "+"; column"
            t += '<tr><td><a href="%s/jobGraph?jobid=%s">%s</a></td><td><a href="%s/userDetails?user=%s">%s</a></td><td>%s</td><td>%s</td><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td class="categories" style="display:none;">%s</td></tr>\n'%(cherrypy.request.base,jid,jid,cherrypy.request.base,uid,uid,nodes,num_cpus,list_load_stndrd,list_load_diff_stndrd,list_VMS_stndrd,list_RSS_stndrd,list_nodes)
            t2 += '<tr><td><a href="%s/jobGraph?jobid=%s">%s</a></td><td><a href="%s/userDetails?user=%s">%s</a></td><td>%s</td><td>%s</td><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td data-sparkline="%s"/><td class="categories" style="display:none;">%s</td></tr>\n'%(cherrypy.request.base,jid,jid,cherrypy.request.base,uid,uid,nodes,num_cpus,list_load,list_core_load_diff,list_VMS,list_RSS,list_nodes)

        t += '</tbody>\n<a href="%s/tymor?refresh=1">&#8635</a>\n'%cherrypy.request.base
        t2 += '</tbody>\n<a href="%s/tymor?refresh=1">&#8635</a>\n'%cherrypy.request.base

        htmlTemp = os.path.join(wai, 'sparkline_std.html')
        h = open(htmlTemp).read()%{'tablespark' : t, 'tablespark2' : t2 }

        return h

    @cherrypy.expose   
    def dashing(self,**args):
        
        selfdata_o = self.data
        data_o = self.currJobs
        selfdata = {k:v for k,v in selfdata_o.items()}
        data = {k:v for k,v in data_o.items()}
        
        more_data={i:selfdata[i][0:USER_INFO_IDX] + selfdata[i][USER_INFO_IDX][0:7] for i in selfdata if len(selfdata[i])>USER_INFO_IDX }
        less_data={i:selfdata[i][0:USER_INFO_IDX] for i in selfdata if len(selfdata[i])<=USER_INFO_IDX}
        selfdata_dict=dict(more_data,**less_data)
        #this appends a dictionary for all of the node information to the job dataset                                                                   
        for jid, jinfo in data.items():
            x = [node for node, coreCount in jinfo.get(u'cpus_allocated').items()]
            d1 = {k: selfdata_dict[k] for k in x}
            data[jid]["node_info"] = d1
            #Now let's just get the data we want for the first Dashboard                                                                                

        keys_id=(u'job_id',u'user_id',u'qos', u'nodes','node_info',u'num_nodes', u'num_cpus',u'run_time',u'run_time_str',u'start_time')
        data_dash={i:{k:data[i][k] for k in keys_id} for i in list(data)}

        for n, v in sorted(data_dash.items()):
           data_dash[n]['list_nodes'] = list(v['node_info'])
           data_dash[n]['list_state'] =[v['node_info'][i][0] for i in list(v['node_info'])]
           data_dash[n]['list_load']  =[round(v['node_info'][i][7],3) for i in list(v['node_info']) if len(v['node_info'][i])>=7]
           data_dash[n]['list_RSS']   =[v['node_info'][i][8] for i in list(v['node_info'])if len(v['node_info'][i])>=7]
           data_dash[n]['list_VMS']   =[v['node_info'][i][9] for i in list(v['node_info'])if len(v['node_info'][i])>=7]
           data_dash[n]['list_cores'] =[round(v['node_info'][i][5],3) for i in v['node_info'].keys() if len(v['node_info'][i])>=7]
           data_dash[n]['list_core_load_diff']=[round(v['node_info'][i][5] -v['node_info'][i][7],3) for i in list(v['node_info']) if len(v['node_info'][i])>=7]
           
        #1: bar chart/pie chart data- this aggregates the node states, could also do for summing aggregate load,RSS,VMS                                   
        x=[selfdata[j][0] for j in list(selfdata)]
        y={i:x.count(i) for i in set(x)}
        node_alloc=[{"type":"node_state","name":key, "value":value} for key, value in zip(list(y),y.values())]
        #2:QOS bars: (table of nodes/cores by qos (i.e. ccb, cca, ))                                                                                      
        zqos=[data[x][u'qos'] for x in list(data)]
        zcpus=[data[x][u'num_cpus'] for x in list(data)]
        znodes=[data[x][u'num_nodes'] for x in list(data)]
        d = {}
        k = list(zip(zqos, znodes))
        for (x,y) in k:
            if x in d:
                d[x] = d[x] + y                                                                     
            else:
                d[x] = y

        qos_nodes=[{"type":"qos_nodes","name":qos.encode("utf-8"),"value":nodes} for qos, nodes in zip(list(d),d.values())]
        node_alloc_qos=node_alloc+qos_nodes
        node_alloc_qos=sorted(node_alloc_qos, key=lambda k: k['name'])
        str_alloc=re.sub(r'[?|$|*|!]',r'',(str(node_alloc_qos)))
        node_alloc_qos = eval(str_alloc)

        #3: data prepping for several charts here- scatter plot run time (log(seconds) versus number of metrics for jobs, treemaps (of load for jobs)
        num_nodes=[data_dash[i][u'num_nodes'] for i in data_dash]
        run_time=[data_dash[i][u'run_time'] for i in data_dash]
        job_id=[data_dash[i][u'job_id'] for i in data_dash]
        states=[data_dash[i]['list_state'] for i in data_dash]
        load=[data_dash[i]['list_load'] for i in data_dash]
        #calculates summary stats on load, vms and rss for jobs
        sum_load=[round(sum(data_dash[i]['list_load']),3) for i in data_dash]
        avg_load=[round(sum(data_dash[i]['list_load'])/float(len(data_dash[i]['list_load'])),3) for i in data_dash if len(data_dash[i]['list_load'])>0]
        max_load=[max(data_dash[i]['list_load'])for i in data_dash if len(data_dash[i]['list_load'])>0]
        sum_load_diff=[round(sum(data_dash[i]['list_core_load_diff']),3) for i in data_dash if len(data_dash[i]['list_core_load_diff'])>0]
        avg_load_diff=[round(sum(data_dash[i]['list_core_load_diff'])/float(len(data_dash[i]['list_core_load_diff'])),3) for i in data_dash if len(data_dash[i]['list_core_load_diff'])>0]
        sum_vms=[round(sum(data_dash[i]['list_VMS']),3) for i in data_dash]
        sum_rss=[round(sum(data_dash[i]['list_RSS']),3) for i in data_dash]
        #put data into json format
        data_json=[{'job_id':job_id,'run_time': run_time, 'num_nodes': num_nodes,'sum_load' : sum_load, 'sum_vms' : sum_vms, 'sum_rss' : sum_rss,'avg_load': avg_load , 'max_load': max_load, 'sum_load_diff' : sum_load_diff, 'avg_load_diff' : avg_load_diff} for job_id, run_time, num_nodes, sum_load,sum_vms, sum_rss, avg_load,max_load, sum_load_diff, avg_load_diff in zip(job_id,run_time,num_nodes,sum_load,sum_vms,sum_rss,avg_load, max_load,sum_load_diff, avg_load_diff)]     
        
        htmlTemp = os.path.join(wai, 'dash.html')
        j = open(htmlTemp).read()%{'data1' : node_alloc,'data2' : node_alloc_qos,'data3' : data_json}
        return j
        
    @cherrypy.expose
    def jobGraph(self, jobid):
        jobid = int(jobid)
        msg   = self.jobGraph_cache(jobid)
        note  = 'cache'
        if not msg:
           print('Job {}: no data in cache'.format(jobid))
           msg = self.jobGraph_influx(jobid)
           note= 'influx'
        if not msg:
           print('Job {}: no data returned from influx'.format(jobid))
           msg = self.jobGraph_file(jobid)
           note='file'
        if not msg:
           return 'Job {}: no data in cache, influx and saved file'.format(jobid)
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms

        if len(msg)==6:
           htmltemp = os.path.join(wai, 'nodeGraph.html')
           h = open(htmltemp).read()%{'spec_title': ' of job {}'.format(jobid),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_r' : msg[4],
                                   'iseries_w' : msg[5]}
        else:
           htmltemp = os.path.join(wai, 'nodeGraph_2.html')
           h = open(htmltemp).read()%{'spec_title': ' of job {}'.format(jobid),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_rw': msg[4]}
        return h

    def jobGraph_file(self, jobid):
        hostData = IndexedHostData(self.config["fileStorage"]["dir"])
        jobid    = int(jobid)
        if jobid not in self.currJobs:
           jobid1, uid, nodelist, start, stop = SlurmCmdQuery().getSlurmJobInfo(jobid)       
        else:
           job      = self.currJobs[jobid]
           nodelist, start, stop, uid = list(job['cpus_allocated']), int(job['start_time']), int(time.time()), job['user_id']
        print("---{} {} {} {}".format(jobid, nodelist, start, stop, uid))
        cpu_all_seq, mem_all_seq, io_all_seq = hostData.queryDataHosts(nodelist, start, stop, uid)
        return start, stop, cpu_all_seq, mem_all_seq, io_all_seq

    def jobGraph_influx(self, jobid):
        jobid                 = int(jobid)
        influxClient          = InfluxQueryClient()
        start, stop, node2seq = influxClient.getSlurmJobData(jobid)  #{hostname: {ts: [cpu, mem, io_r, io_w] ... }}
        if not node2seq:
           return None

        mem_all_nodes  = []  ##[{'data': [[1531147508(s), value]...], 'name':'workerXXX'}, ...] 
        cpu_all_nodes  = []  ##[{'data': [[1531147508, value]...], 'name':'workerXXX'}, ...] 
        io_r_all_nodes = []  ##[{'data': [[1531147508, value]...], 'name':'workerXXX'}, ...] 
        io_w_all_nodes = []  ##[{'data': [[1531147508, value]...], 'name':'workerXXX'}, ...] 
        for hostname, hostdict in node2seq.items():
            cpu_all_nodes.append  ({'name': hostname, 'data': [[ts, hostdict[ts][0]] for ts in hostdict.keys()]})
            mem_all_nodes.append  ({'name': hostname, 'data': [[ts, hostdict[ts][1]] for ts in hostdict.keys()]})
            io_r_all_nodes.append ({'name': hostname, 'data': [[ts, hostdict[ts][2]] for ts in hostdict.keys()]})
            io_w_all_nodes.append ({'name': hostname, 'data': [[ts, hostdict[ts][3]] for ts in hostdict.keys()]})
        return start, stop, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes

    def jobGraph_cache(self, jobid):
        jobid       = int(jobid)
        job, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes= self.inMemCache.queryJob(jobid)

        # highcharts 
        if job:  #job is in cache
           if cpu_all_nodes and cpu_all_nodes[0]['data']:
              start = min([n['data'][0][0]  for n in cpu_all_nodes if n['data']])
              stop  = max([n['data'][-1][0] for n in cpu_all_nodes if n['data']])
           
              if start - job['submit_time'] < 120: # tolerate 120 seoconds  
                 return start, stop, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes
              else:
                 print("jobGraph_cache: job {} data in cache is not complete ({} << {})".format(jobid, job['submit_time'], start))
           else:
              print("jobGraph_cache: no job {} data in cache".format(jobid))
        else:
           print("jobGraph_cache: no job {} in cache".format(jobid))
        return None

    def nodeJobProcGraph_cache(self, node, jobid):
        return None

    def nodeJobProcGraph_file(self, node, jobid):
        return None

    def nodeJobProcGraph_influx(self, node, jobid, start=''):
        influxClient     = InfluxQueryClient()
        first, last, seq = influxClient.getNodeJobProcData(node, jobid, start)  #{pid: [(ts,cpu, mem, io_r, io_w) ... ]}
        if not seq:
           return None

        cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes  = [],[],[],[]  ##[{'data': [[1531147508, value]...], 'name':'workerXXX'}, ...] 
        for pid, pidSeq in seq.items():
            if start and (start < first):
               pidSeq.insert(0, (start, 0, 0, 0, 0))
            #convert cpu seconds to cpu util
            cpu_all_nodes.append  ({'name': pid, 'data': MyTool.getSeqDeri_x(pidSeq, 0, 1)})
            mem_all_nodes.append  ({'name': pid, 'data': [[item[0], item[2]] for item in pidSeq]})
            io_r_all_nodes.append ({'name': pid, 'data': MyTool.getSeqDeri_x(pidSeq, 0, 3)})
            io_w_all_nodes.append ({'name': pid, 'data': MyTool.getSeqDeri_x(pidSeq, 0, 4)})

        return start, last, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes

    def getJobStart (self, jobid):
        jobid = int(jobid)
        if self.currJobs and (jobid in self.currJobs):
           return self.currJobs[jobid]['start_time']
        job   = SlurmCmdQuery.sacct_getJobReport(jobid)[0]
        if job and job['Start']!='Unknown':
           return MyTool.str2ts(job['Start'])
        else:
           return None

    @cherrypy.expose
    def nodeGPUGraph(self, node):  #the GPU util for the node of last day
        client  = BrightRestClient()
        nodeData= pyslurm.node().get_node(node)
        if not nodeData:
           return 'Node {} is not in slurm cluster.'.format(node)
        if not nodeData[node]['gres']:
           return 'Node {} does not have gres resource'.format(node)
        data    = client.getDumpAllGPU(node)
        start   = min([item['data'][0][0] for item in data['data']])
        stop    = max([item['data'][-1][0] for item in data['data']])
        series  = data['data']       # [{name:, data:[[ts,val]],}]
        for item in series:
            item['data'] = [[i[0]*1000,i[1]] for i in item['data']]

        htmltemp = os.path.join(wai, 'nodeGPUGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' on {}'.format(node),
                                   #'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(data['data'][0]['data'][0][0])),
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(stop)),
                                   'series'    : series}
        return h

    @cherrypy.expose
    def nodeJobProcGraph(self, node, jid):
        jobid = int(jid)
        start = self.getJobStart(jobid)
        msg   = self.nodeJobProcGraph_cache(node, jobid)
        note  = 'cache'
        if not msg:
           print('Job {}: no data in cache'.format(jobid))
           msg = self.nodeJobProcGraph_influx(node, jobid, start)
           note= 'influx'
        if not msg:
           print('Job {}: no data returned from influx'.format(jobid))
           msg = self.nodeJobProcGraph_file(node, jobid)
           note='file'
        if not msg:
           return 'Job {}: no data in cache, influx and saved file'.format(jobid)
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms

        htmltemp = os.path.join(wai, 'nodeGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' of job {} on {}'.format(jobid, node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_r' : msg[4],
                                   'iseries_w' : msg[5]}
        return h

    #return jobstarttime, data, datadescription
    def getUserJobMeasurement (self, uid):
        # get the current jobs of uid
        start, jobs = self.getUserCurrJobs (uid)
        if not jobs:
            return None, None, None
        
        # get nodes and period, 
        # get the utilization of each jobs
        #queryClient = InfluxQueryClient.getClientInstance()
        ifxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
        jid2df    = {}
        jid2dsc   = {}   # jid description
        for job in jobs:
            if job.get('num_nodes',-1) < 1 or not job.get('nodes', None):
               print("WARNING getUserJobbMeasurement:job does not have allocated nodes information {}".format(job))
               continue

            #print ("getUserJobMeasurement nodes=" + repr(job['nodes']))
            ld    = ifxClient.queryJidMonData (job['job_id'], job['start_time'], '', MyTool.convert2list(job['nodes']), ['hostname', 'time', 'cpu_system_util', 'cpu_user_util', 'io_read_bytes', 'io_write_bytes', 'mem_rss_K'])
            t1    = time.time()
            # sum over hostname to get jobs data, PROBLEM: ts on different host is not synchornized
            df    = pandas.DataFrame (ld)
            if df.empty:
                continue
            sumDf = pandas.DataFrame({})
            for name, group in df.groupby('hostname'):
                group          = group.reset_index(drop=True)
                group['count'] = 1
                if sumDf.empty:
                   sumDf = group
                else:
                   sumDf = sumDf.add(group, fill_value=0)        #sum over the same artifical index, not accurate as assuming the same start time on all nodes of jobs
            # take the averge of time and value
            sumDf['time'] = sumDf['time']/sumDf['count']
            #print ("sumDf=" + repr(sumDf.head()))
                
            jid          = job['job_id']
            jid2df[jid]  = sumDf
            jid2dsc[jid] = '{} ({}, {} CPUs)'.format(jid, job['nodes'], job['num_cpus'])
            print ('getUserJobMeasurement reconstruct result takes {}'.format(time.time()-t1))
        return start, jid2df, jid2dsc
                
    def getWaitMsg (self):
        elapse_time = (int)(time.time() - self.startTime)

        return WAIT_MSG + repr(elapse_time) + " seconds since server restarted."

    @cherrypy.expose
    def userJobGraph(self,uname,start='', stop=''):
        if not self.currJobs: return self.getWaitMsg()

        #{jid: df, ...}
        uid                      = MyTool.getUid(uname)
        start, jid2df, jid2dsc   = self.getUserJobMeasurement (uid)
        if not jid2df:          return "User does not have running jobs at this time"
      
        #{'name': jid, 'data':[[ts, value], ...]
        series       = {'cpu':[], 'mem':[], 'io':[]}
        for jid, df in jid2df.items():
            df['cpu_time'] = df['cpu_system_util'] + df['cpu_user_util']
            df['io_bytes'] = df['io_read_bytes']   + df['io_write_bytes']
            df['time']     = df['time'] * 1000
            series['cpu'].append({'name': jid2dsc[jid], 'data':df[['time','cpu_time']].values.tolist()})
            series['mem'].append({'name': jid2dsc[jid], 'data':df[['time','mem_rss_K']].values.tolist()})
            series['io'].append ({'name': jid2dsc[jid], 'data':df[['time','io_bytes']].values.tolist()})
        
        #if len(msg)==6:
        htmltemp = os.path.join(wai, 'nodeGraph_2.html')
        h        = open(htmltemp).read()%{'spec_title': ' of user {}'.format(uname),
                                   'note'      : 'influxdb',
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(time.time())),
                                   'lseries'   : series['cpu'],
                                   'mseries'   : series['mem'],
                                   'iseries_rw' : series['io'],
                                   }
        return h

    @cherrypy.expose
    def userGraph(self,uname,start='', stop=''):
        if type(self.data) == str: return self.data # error of some sort.

        uid          = MyTool.getUid(uname)
        #get the start time from jobData, the earliest time of all jobs belong to the user
        if start:
            start = time.mktime(time.strptime(start, '%Y-%m-%d'))
        else:
           start    = min(self.getUserJobStartTimes(uid))
        if stop:
            stop  = time.mktime(time.strptime(stop,  '%Y-%m-%d'))
        else:
           stop     = time.time()

        return self.userNodeGraphData(uid, uname, start, stop)

    def userNodeGraphData (self, uid, uname, start, stop):
        #{hostname: {ts: [cpu, io, mem] ... }}
        influxClient = InfluxQueryClient(self.config['influxdb']['host'],'slurmdb')
        node2seq     = influxClient.getSlurmUidMonData_All(uid, start,stop)

        mem_all_nodes = []  ##[{'data': [[1531147508000(ms), value]...], 'name':'workerXXX'}, ...]
        cpu_all_nodes = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...]
        io_all_nodes  = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...]
        for hostname, hostdict in node2seq.items():
            mem_node={'name': hostname}
            mem_node['data']= [[ts, hostdict[ts][2]] for ts in hostdict.keys()]
            mem_all_nodes.append (mem_node)

            cpu_node={'name': hostname}
            cpu_node['data']= [[ts, hostdict[ts][0]] for ts in hostdict.keys()]
            cpu_all_nodes.append (cpu_node)

            io_node={'name': hostname}
            io_node['data']= [[ts, hostdict[ts][1]] for ts in hostdict.keys()]
            io_all_nodes.append (io_node)
        ann_series = []

        # highcharts
        htmltemp = os.path.join(wai, 'smGraphHighcharts.html')
        h = open(htmltemp).read()%{'spec_title': ' of user ' + uname,
                                   'start'     : time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop'      : time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'lseries'   : cpu_all_nodes,
                                   'mseries'   : mem_all_nodes,
                                   'iseries'   : io_all_nodes,
                                   'aseries'   : ann_series}
        return h

    @cherrypy.expose
    def sunburst(self):
        if type(self.data) == str: return self.data# error of some sort.

        #prepare required information in data_dash
        more_data    = {k:v[0:USER_INFO_IDX] + v[USER_INFO_IDX][0:7] for k,v in self.data.items() if len(v)>USER_INFO_IDX } #flatten hostdata
        less_data    = {k:v[0:USER_INFO_IDX]                         for k,v in self.data.items() if len(v)<=USER_INFO_IDX }
        hostdata_flat= dict(more_data,**less_data)
        #print("more_data=" + repr(more_data))
        #print("less_data=" + repr(less_data))
        #print("hostdata_flat={}".format(hostdata_flat))

        keys_id      =(u'job_id',u'user_id',u'qos', u'num_nodes', u'num_cpus')
        data_dash    ={jid:{k:jinfo[k] for k in keys_id} for jid,jinfo in self.currJobs.items()} #extract set of keys
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in self.currJobs.items():
            data_dash[jid]["node_info"] = {n: hostdata_flat.get(n,[]) for n in jinfo.get(u'cpus_allocated').keys()}
        
        #print("data_dash=" + repr(data_dash))
        for jid, jinfo in sorted(data_dash.items()):
            username = pwd.getpwuid(jinfo[u'user_id']).pw_name
            data_dash[jid]['cpu_list'] = [jinfo['node_info'][i][5] for i in jinfo['node_info'].keys() if len(jinfo['node_info'][i])>=7]
            if not data_dash[jid]['cpu_list']:
                #print ('Pruning:', repr(jinfo), file=sys.stderr)
                data_dash.pop(jid)
                continue

            nodes = list(jinfo['node_info'].keys())
            #print("nodes={} \n{}".format(nodes, jinfo))
            data_dash[jid]['list_nodes'] = nodes
            data_dash[jid]['list_state'] =[jinfo['node_info'][i][0]          if len(jinfo['node_info'][i])>0  else 'UNDEFINED' for i in nodes]
            data_dash[jid]['list_cores'] =[round(jinfo['node_info'][i][5],3) if len(jinfo['node_info'][i])>=7 else -1          for i in nodes]
            data_dash[jid]['list_load']  =[round(jinfo['node_info'][i][7],3) if len(jinfo['node_info'][i])>=7 else 0.0         for i in nodes]
            data_dash[jid]['list_RSS']   =[jinfo['node_info'][i][8]          if len(jinfo['node_info'][i])>=7 else 0           for i in nodes]
            data_dash[jid]['list_VMS']   =[jinfo['node_info'][i][9]          if len(jinfo['node_info'][i])>=7 else 0           for i in nodes]
  
            num_nodes = jinfo[u'num_nodes']
            data_dash[jid]['list_jobid']   =[jid]          * num_nodes
            data_dash[jid]['list_username']=[username]     * num_nodes
            data_dash[jid]['list_qos']     =[jinfo[u'qos']]* num_nodes
            data_dash[jid]['list_group']   =[MyTool.getUserOrgGroup(username)]* num_nodes
        
        #need to filter data_dash so that it no longer contains users that are "None"->this was creating sunburst errors 
        #open('/tmp/sunburst.tmp', 'w').write(repr(data_dash))

        if len(data_dash) == 0:
            return EMPTYPROCDATA_MSG + '\n\n' + repr(self.data)

        # get flat list corresponding to each node
        list_nodes_flat=reduce((lambda x,y: x+y), [v['list_nodes'] for v in data_dash.values()])
        list_loads_flat=reduce((lambda x,y: x+y), [v['list_load']  for v in data_dash.values()])
        list_cpus_flat =reduce((lambda x,y: x+y), [v['cpu_list']   for v in data_dash.values()])
        list_RSS_flat  =reduce((lambda x,y: x+y), [v['list_RSS']   for v in data_dash.values()])
        list_VMS_flat  =reduce((lambda x,y: x+y), [v['list_VMS']   for v in data_dash.values()])
        list_job_flatn =reduce((lambda x,y: x+y), [v['list_jobid'] for v in data_dash.values()])
        list_part_flatn=reduce((lambda x,y: x+y), [v['list_qos']   for v in data_dash.values()])
        list_usernames_flatn=reduce((lambda x,y: x+y), [v['list_username']   for v in data_dash.values()])
        list_group_flatn    =reduce((lambda x,y: x+y), [v['list_group']   for v in data_dash.values()])

        # merge above list into nested list
        listn  =[[list_group_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_loads_flat[i]] for i in range(len(list_nodes_flat))]
        listrss=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_RSS_flat[i]]   for i in range(len(list_nodes_flat))]
        listvms=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_VMS_flat[i]]   for i in range(len(list_nodes_flat))]
        listns =[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i]]                    for i in range(len(list_nodes_flat))]
        #print("listn=" + repr(listn))

        data_dfload =pandas.DataFrame(listn,   columns=['partition','user','job','node','load'])
        data_dfrss  =pandas.DataFrame(listrss, columns=['partition','user','job','node','rss'])
        data_dfvms  =pandas.DataFrame(listvms, columns=['partition','user','job','node','vms'])
        #print("data_dfload=" + repr(data_dfload))
        
        #node_states =[[j.encode("utf-8"),hostdata[j][0]] for j in hostdata.keys()]
        node_states =[[j,hostdata_flat[j][0]] for j in hostdata_flat.keys()]
        data_df     =pandas.DataFrame(listns,     columns=['partition','user','job','node'])
        state       =pandas.DataFrame(node_states,columns=['node','state'])
        data_dfstate=pandas.merge(state, data_df, on='node',how='left')
        data_dfstate['partition'].fillna('Not_Allocated', inplace=True)
        data_dfstate['user'].fillna     ('Not_Allocated', inplace=True)
        data_dfstate['job'].fillna      ('Not_Allocated', inplace=True)
        data_dfstate['load']=28
        order       =['partition','user','job','state','node','load']
        data_dfstate=data_dfstate[order]
        
        d_load    = MyTool.createNestedDict("load",  ["partition","user","job","node"],          data_dfload,  "load")
        json_load = json.dumps(d_load,  sort_keys=False, indent=2)
        d_vms     = MyTool.createNestedDict("VMS",   ["partition","user","job","node"],          data_dfvms,   "vms")
        json_vms  =json.dumps(d_vms, sort_keys=False,indent=2)
        d_rss     = MyTool.createNestedDict("RSS",   ["partition","user","job","node"],          data_dfrss,   "rss")
        json_rss  =json.dumps(d_rss, sort_keys=False, indent=2)
        d_state   = MyTool.createNestedDict("state", ["partition","user","job","state", "node"], data_dfstate, "load")
        json_state= json.dumps(d_state, sort_keys=False, indent=2)

        #get all the usernames
        set_usernames = sorted(set(list_usernames_flatn))

        htmltemp = os.path.join(wai, 'sunburst2.html')
        h = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.updateTS).ctime(), 'data1' : json_load, 'data2' : json_state, 'data3' : json_vms, 'data4' : json_rss, 'users':set_usernames}
        return h

    sunburst.exposed = True

    @cherrypy.expose
    def inputSearch(self):
        userLst    = sorted(MyTool.getAllUsers ())
        jobLst     = [str(jid) for jid in sorted(pyslurm.job().get().keys())]
        nodeLst    = sorted(pyslurm.node().get().keys())
        partLst    = sorted(pyslurm.partition().get().keys())
        htmlTemp   = os.path.join(wai, 'search.html')
        htmlStr    = open(htmlTemp).read().format(users=userLst, jobs=jobLst, nodes=nodeLst, partitions=partLst)
        return htmlStr

def error_page_500(status, message, traceback, version):
    return "Error %s - Well, I'm very sorry but the page your requested is not implemented!" % status

cherrypy.config.update({#'environment': 'production',
                        'log.access_file':    '/tmp/slurm_util/smcpgraph-html-sun.log',
#                        'error_page.500':     error_page_500,
                        'server.socket_host': '0.0.0.0', 
                        'server.socket_port': WebPort})
conf = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(wai, 'public'),
    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(wai, 'public/images/sf.ico'),
    },
}

cherrypy.quickstart(SLURMMonitor(), '/', conf)
