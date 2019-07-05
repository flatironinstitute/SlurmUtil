import cherrypy, _pickle as cPickle, csv, datetime, json, os
import pandas, pwd, re, subprocess as SUB, sys, time, zlib
from collections import defaultdict
import fbprophet as fbp
import matplotlib.pyplot as plt
from functools import reduce

import fs2hc
import scanSMSplitHighcharts
from queryInflux import InfluxQueryClient
from querySlurm import SlurmCmdQuery, SlurmDBQuery
from queryTextFile import TextfileQueryClient

import MyTool
import SlurmEntities

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

@cherrypy.expose
class SLURMMonitor(object):

    def __init__(self):
        self.data            = 'No data received yet. Wait a minute and come back.'
        self.runningJob         = {}
        self.pyslurmNodeData = None
        self.updateTime      = None
        self.queryTxtClient  = TextfileQueryClient(os.path.join(wai, 'host_up_ts.txt'))
        self.querySlurmClient= SlurmDBQuery()
        self.jobNodeHistory  = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))  #jid: node: pid: 'ts' 'cpu_time'
        self.node2Jobs       = {}

        self.cvtDict     = {}
        self.startTime   = time.time()
        self.rawData         = {}

    # add processes info of jid
    # TODO: it is in fact userNodeHistory
    def addJobNodeHistory (self, ts, jid, node, processes):
        for p in processes:   # pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, intervalIOByteAvg
            if ( ts > self.jobNodeHistory[jid][node][p[0]]['ts'] ):
               assert (self.jobNodeHistory[jid][node][p[0]]['cpu'] <= p[3] + p[4])        #increasing
               self.jobNodeHistory[jid][node][p[0]]['ts']      = ts
               self.jobNodeHistory[jid][node][p[0]]['cpu_time']= p[3] + p[4]
              # else discard older information

        if len(self.jobNodeHistory) > 100:
           self.cleanJobNodeHistory (self.runningJob)

    #remove done job from history
    def cleanJobNodeHistory (self, jobData):
        done_job = [jid for jid, jinfo in jobData.items() if jinfo['job_state'] not in ['RUNNING', 'PENDING', 'PREEMPTED']]
        for jid in done_job:
            self.jobNodeHistory.pop(jid, {})
        
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
        ins        = SlurmEntities.SlurmEntities()
        fields     = ['name', 'state', 'tres_fmt_str', 'features', 'gres', 'alloc_cpus','alloc_mem', 'cpu_load', 'gres_used', 'run_jobs']
        titles     = ['Node', 'State', 'TRES',         'Features', 'Gres', 'Alloc CPU', 'Alloc Mem', 'CPU Load', 'Gres_used', 'Run Job']
        p_keys     = ['allow_accounts', 'allow_alloc_nodes', 'allow_groups', 'allow_qos', 'def_mem_per_cpu', 'default_time_str', 'flags', 
                      'max_cpus_per_node', 'max_mem_per_node', 'max_nodes', 'max_share']
        p, nodes   = ins.getPartitionInfo (partition, fields)
        #titles     = list(set([key for n in nodes for key in n]))
        t          = dict(zip(fields, titles))

        for node in nodes:
            node['name']='<a href="./nodeDetails?node={nm}">{nm}</a>'.format(nm=node['name'])

        htmlTemp   = os.path.join(wai, 'partitionDetail.html')
        htmlStr    = open(htmlTemp).read().format(p_name=partition, p_detail=p, p_nodes=nodes, n_titles=t)
        return htmlStr
       
    @cherrypy.expose
    def pending(self, start='', stop='', state=3):
        ins        = SlurmEntities.SlurmEntities()
        pendingLst = ins.getPendingJobs()
        partLst    = ins.getPartitions ()
        
        htmlTemp   = os.path.join(wai, 'table.html')
	#htmlStr    = open(htmlTemp).read()%{'partitions_input' : partLst}
        timestr    = ins.update_time.ctime()
        htmlStr    = open(htmlTemp).read().format(update_time=timestr, pending_jobs_input=pendingLst, partitions_input=partLst)
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
    def test(self, start='', stop='', days=3):
        start, stop  = MyTool.getStartStopTS (start, stop)

        influxClient = InfluxQueryClient.getClientInstance()
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

    @cherrypy.expose
    def testtest(self, start='', stop=''):
        start, stop  = MyTool.getStartStopTS (start, stop)
        influxClient = InfluxQueryClient.getClientInstance()
        tsReason2Cnt, jidSet = influxClient.getPendingCount(start, stop)
        bySched      = 'Dependency|Priority|BeginTime|JobArrayTaskLimit' 
        byResource   = 'Resources|ReqNodeNotAvail*|Nodes_required_for_job_are_DOWN*'
        byQOS        = 'QOS*'
        byGPU        = 'Resources_GPU'
        reasons      = [set(reasons.keys()) for ts, reasons in tsReason2Cnt.items()]
        reasons      = set([i2 for item in reasons for i2 in item])
        cate2ts_cnt  = defaultdict(list)
        other_reason_set = set()
        for ts, reason2Cnt in tsReason2Cnt.items():
            for reason, cnt in reason2Cnt.items():
                cate = 'Other'
                if not reason:
                   other_reason_set.add(reason)
                elif re.match(bySched, reason):
                   cate = 'Sched'
                elif re.match(byResource, reason):
                   cate = 'Resource'
                elif re.match(byGPU, reason):
                   cate = 'GPU'
                elif re.match(byQOS, reason):
                   cate = 'QoS'
                else:
                   other_reason_set.add(reason)
                cate2ts_cnt[cate].append ([ts, cnt])
        series1   = [
                     {'name': 'Queued by QoS',            'data':cate2ts_cnt['QoS']},
                     {'name': 'Queued by Resource',       'data':cate2ts_cnt['Resource']},
                     {'name': 'Queued by GPU Resource',   'data':cate2ts_cnt['GPU']},
                     {'name': 'Queued by Job Defination', 'data':cate2ts_cnt['Sched']},
                     {'name': 'Queued by Other',    'data':cate2ts_cnt['Other']}]
        htmlTemp = os.path.join(wai, 'jobResourceReport.html')
        h = open(htmlTemp).read().format(start=time.strftime('%Y-%m-%d', time.localtime(start)),
                                         stop=time.strftime('%Y-%m-%d', time.localtime(stop)),
                                         series_00=series1, series_01=series1, title1='Cluster Job Queue Length', xlabel1='Queue Length', 
                                         series_10=series1, series_11=series1, title2='Cluster Job Queue Length 2', xlabel2='Queue Length2') 

        return h

    @cherrypy.expose
    def pending_history(self, start='', stop=''):
        start, stop  = MyTool.getStartStopTS (start, stop)

        influxClient = InfluxQueryClient.getClientInstance()
        tsReason2Cnt, jidSet = influxClient.getPendingCount(start, stop)
        bySched      = 'Dependency|BeginTime|JobArrayTaskLimit' 
        byResource   = 'Resources|Priority|ReqNodeNotAvail*|Nodes_required_for_job_are_DOWN*'
        byQOS        = 'QOS*'
        byGPU        = 'Resources_GPU|Priority_GPU'
        
        reasons      = [set(reasons.keys()) for ts, reasons in tsReason2Cnt.items()]
        reasons      = set([i2 for item in reasons for i2 in item])
        #print("INFO: reasons {}".format(reasons))
        
        #reformat the tsReason2Cnt to cate2ts_cnt
        cate2ts_cnt  = defaultdict(list)
        other_reason_set = set()
        for ts, reason2Cnt in tsReason2Cnt.items():
            for reason, cnt in reason2Cnt.items():
                cate = 'Other'
                if not reason:
                   other_reason_set.add(reason)
                elif re.match(bySched, reason):
                   cate = 'Sched'
                elif re.match(byResource, reason):
                   cate = 'Resource'
                elif re.match(byGPU, reason):
                   cate = 'GPU'
                elif re.match(byQOS, reason):
                   cate = 'QoS'
                else:
                   other_reason_set.add(reason)

                cate2ts_cnt[cate].append ([ts, cnt])

        series1   = [
                     {'name': 'Queued by Resource',       'data':cate2ts_cnt['Resource']},
                     {'name': 'Queued by GPU Resource',   'data':cate2ts_cnt['GPU']},
                     {'name': 'Queued by QoS',            'data':cate2ts_cnt['QoS']},
                     {'name': 'Queued by Job Defination', 'data':cate2ts_cnt['Sched']},
                     {'name': 'Queued by Other',    'data':cate2ts_cnt['Other']}]

        htmlTemp = os.path.join(wai, 'pendingJobReport.html')
        h = open(htmlTemp).read().format(start=time.strftime('%Y-%m-%d', time.localtime(start)),
                                         stop=time.strftime('%Y-%m-%d', time.localtime(stop)),
                                         series1=series1, title1='Cluster Job Queue Length', xlabel1='Queue Length', other_reason=list(other_reason_set))

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
        return repr(self.runningJob)

    @cherrypy.expose
    def getRawNodeData(self):
        return repr(self.pyslurmNodeData)

    # return earliest start time and all current jobs
    def getUserCurrJobs (self, uid):
        uid    = int(uid)
        jobs   = []
        start  = 0
        for jid, jinfo in self.runningJob.items():
            if ( jinfo.get('user_id',-1) == uid ): 
               jobs.append(jinfo)
               if ( jinfo.get('start_time', 0) > start ): start = jinfo.get('start_time', 0)  

        return start, jobs
        

    def getUserJobStartTimes(self, uid):
        uid   = int(uid)
        stime = []
        for jid, jinfo in self.runningJob.items():
            if ( jinfo.get('user_id',-1) == uid ): stime.append(jinfo.get('start_time', 0)) 

        return stime

    @cherrypy.expose
    def getNode2Jobs1 (self):
        return "{}".format(self.node2Jobs)

    def updateNode2Jobs (self, jobData):
        self.node2Jobs = defaultdict(list)  #nodename: joblist
        for jid, jinfo in jobData.items():
            for nodename, coreCount in jinfo.get(u'cpus_allocated', {}).items():
                self.node2Jobs[nodename].append(jid)

    def jid2uid (self, jid):
        return self.runningJob[jid]['user_id']

    def uid2jids  (self, uid, node):
        jids = list(filter(lambda x: self.runningJob[x]['user_id']==uid, self.node2Jobs[node]))
        if len(jids) == 0:
           print ('WARNING: uid2jid user {} has no slurm jobs on node {}'.format(uid, node))
        elif len(jids) > 1:
           print ('WARNING: uid2jid user {} has multiple slurm jobs {} on node {}'.format(uid, jids, node))

        return jids 

    #get the total cpu time of uid on node
    def getJobNodeTotalCPUTime(self, jid, node):
        time_lst = [ d['cpu_time'] for d in self.jobNodeHistory[jid][node].values() ]
        return sum(time_lst)

    def getSummaryTableData(self, hostData, jobData):
        #print("getSummaryTableData")
        node2jobs = self.node2Jobs

        result=[]
        for node, nodeInfo in sorted(hostData.items()):
            #status display no extra
            status = nodeInfo[0]
            ts     = nodeInfo[2]
            if status.endswith(('@','+','$','#','~','*')):
               status = status[:-1]

            if len(nodeInfo) < USER_INFO_IDX:
               print("ERROR: getSummaryTableData nodeInfo wrong format {}:{}".format(node, nodeInfo)) 
               continue

            delay= nodeInfo[1]
            if ( node2jobs.get(node) ):
               for jid in node2jobs.get(node):
                  # add explictly job, uname mapping
                  uid = self.jid2uid (jid)
                  if len(nodeInfo) > USER_INFO_IDX:
                     for uname, p_uid, coreNum, proNum, cpuUtil, rss, vms, pp, io, *etc in nodeInfo[USER_INFO_IDX:]:
                        if p_uid == uid:
                           job_stime = self.runningJob[jid].get('start_time', 0)
                           job_avg_cpu = self.getJobNodeTotalCPUTime(jid, node) / (ts-job_stime) if job_stime > 0 else 0 #TODO: have problem is one user has multiple job on the same node
                           result.append([node, status, jid, delay, uname, coreNum, proNum, cpuUtil, job_avg_cpu, rss, vms, io*8])
                        else:
                           print ("WARNING: getSummaryTableData: proc_uid {} != job_uid {} on worker {}, ignore.".format(p_uid, uid, node))
                  else:
                     result.append([node, status, jid, delay ])
            else:
               result.append([node, status, ' ', delay])
                
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
                   
    def getHeatmapData (self):
        node2job= self.node2Jobs

        #generate jobs info {job_id, long_label, disabled}
        jobs = []
        for jobid, jobinfo in self.runningJob.items():
            user=MyTool.getUser(jobinfo['user_id'])
            if jobinfo['tres_alloc_str']:
               long_label = '{}({}) {} with {}'.format(jobid, user, jobinfo['job_state'], jobinfo['tres_alloc_str'])
               disabled   = ""
            else:
               long_label = '{}({}) {} waiting {}'.format(jobid, user, jobinfo['job_state'], jobinfo['tres_req_str'])
               disabled   = "disabled"
            job = {"job_id":jobid, "long_label":long_label, "disabled":disabled}
 
            #jobinfo['user']=MyTool.getUser(jobinfo['user_id'])
            #job=MyTool.sub_dict_exist(jobinfo, ['job_id', 'user', 'job_state', 'account', 'tres_alloc_str', 'tres_req_str'])
            jobs.append(job)

        result  = []  #dataset1 in heatmap
        for hostname, values in sorted(self.data.items()):
            try:
               node_cores = self.pyslurmNodeData[hostname]['cpus']
               alloc_jobs = node2job.get(hostname, [])
               if len(values) > USER_INFO_IDX: #ALLOCATED, MIXED
                  cpu_load   = values[USER_INFO_IDX][4]
                  state      = 1
               else:
                  cpu_load   = -1
                  if values[0] in ['ALLOCATED$','ALLOCATED','ALLOCATED@']:
                      state    = 1
                      cpu_load = 0
                  elif values[0] == 'IDLE':
                      state  = 0
                  else:
                      state  = -1

               job_accounts  = [self.runningJob[jid].get('account', None)        for jid in alloc_jobs]
               job_users     = [MyTool.getUser(self.runningJob[jid]['user_id'])  for jid in alloc_jobs]
               job_cores     = [self.runningJob[jid]['cpus_allocated'][hostname] for jid in alloc_jobs]

               if state == 1 :
                  lst   = list(zip(alloc_jobs, job_users, job_cores))
                  label = '{} util:{:.1%} core:{} jobs:{}'.format(hostname, cpu_load/node_cores, node_cores, lst)
               else:
                  label = '{} core:{} state:{}'.format(hostname, node_cores, values[0])
               result.append([hostname, state, node_cores, cpu_load, alloc_jobs, job_accounts, label])
            except Exception as exp:
               print("ERROR getHeatmapData: {0}".format(exp))
                               
        return result,jobs
                   
    @cherrypy.expose
    def utilHeatmap(self, **args):
        if type(self.data) == str: return self.data # error of some sort.

        data,jobDict     = self.getHeatmapData ()
        
        htmltemp = os.path.join(wai, 'heatmap.html')
        h        = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.updateTime).ctime(), 'data1' :  data, 'data2': jobDict}
 
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
        pages =["index",           "utilHeatmap", "pending",      "sunburst",       "usageGraph", "tymor2", "report", "forecast"]
        titles=["Tabular Summary", "Host Util.",  "Pending Jobs", "Sunburst Graph", "Usage Graph","Tymor", "Report", "Forecast"]
 
        result=""
        for i in range (len(pages)):
           if ( pages[i] == page ):
              result += '<button class="tablinks active"><a href="/' + pages[i] + '">' + titles[i] + '</a></button>'
           else:
              result += '<button class="tablinks"><a href="/' + pages[i] + '">' + titles[i] + '</a></button>'

        return result

    @cherrypy.expose
    def index(self, **args):
        if type(self.data) == str: return self.data # error of some sort.

        tableData = self.getSummaryTableData(self.data, self.runningJob)
        
        htmltemp = os.path.join(wai, 'index3.html')
        h = open(htmltemp).read()%{'tableData' : tableData, 'update_time': datetime.datetime.fromtimestamp(self.updateTime).ctime()}
 
        return h

    def updateCvtDict (self):
        if len(self.cvtDict)==0 or self.cvtDict.get('timestamp') < self.updateTime:
           node2job = defaultdict(dict)
           job2node = defaultdict(dict)
           for jid, jinfo in self.runningJob.items():
               for nodename, coreCount in jinfo.get(u'cpus_allocated', {}).items():
                   node2job[nodename][jid]=coreCount
                   job2node[jid][nodename]=coreCount

           self.cvtDict['timestamp'] = self.updateTime
           self.cvtDict['node2job']  = node2job
           self.cvtDict['job2node']  = job2node

    
    @cherrypy.expose
    def getNode2Job (self):
        self.updateCvtDict()
        return self.cvtDict['node2job']
           
    @cherrypy.expose
    def getJob2Node (self):
        self.updateCvtDict()
        return self.cvtDict['job2node']

    @cherrypy.expose
    def getRawData (self):
        l = [(w, len(info)) for w, info in self.rawData.items() if len(info)>3]
        return "{}\n{}".format(l, self.rawData)

    @cherrypy.expose
    def updateSlurmData(self, **args):
        #updated the data
        d =  cherrypy.request.body.read()
        #jobData and pyslurmNodeData comes from pyslurm
        self.updateTime, jobs, hn2info, self.pyslurmNodeData = cPickle.loads(zlib.decompress(d))
        self.runningJob = dict([(jid,job) for jid, job in jobs.items() if job['job_state']=='RUNNING'])
        self.rawData = hn2info

        self.updateNode2Jobs (self.runningJob)
        if type(self.data) != dict: self.data = {}  #may have old data from last round
        for node,nInfo in hn2info.items(): 
            self.data[node] = nInfo      #nInfo: status, delta, ts, procsByUser
            if len(nInfo) > USER_INFO_IDX and nInfo[USER_INFO_IDX]:
               for procsByUser in nInfo[USER_INFO_IDX:]:   #worker may has multiple users
                                                           #user_name, uid, hn2uid2allocated.get(hostname, {}).get(uid, -1), len(pp), totIUA, totRSS, totVMS, pp, totIO, totCPU])
                  if len(procsByUser) > USER_PROC_IDX and procsByUser[USER_PROC_IDX]:
                     jids         = self.uid2jids (procsByUser[1], node)
                     for jid in jids: #TODO: user has multiple jobs on the same worker
                        self.addJobNodeHistory (nInfo[2], jid, node, procsByUser[USER_PROC_IDX])

    def sacctData (self, criteria):
        cmd = ['sacct', '-n', '-P', '-o', 'JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'] + criteria
        try:
            #TODO: capture standard error separately?
            d = SUB.check_output(cmd, stderr=SUB.STDOUT)
        except SUB.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        return d.decode('utf-8')

    def sacctDataInWindow(self, criteria):
        t = datetime.date.today() + datetime.timedelta(days=-SACCT_WINDOW_DAYS)
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
        htmlTemp = 'fileCensus.html'

        h = open(htmlTemp).read()%{'file_systems':fs2hc.FileSystems, 'yyyymmdd': 'yyyymmdd', 'label': "label", 'data': usageData_dict}

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

        #htmlTemp = 'fileCensus.html'
        #h = open(htmlTemp).read()%{'yyyymmdd': yyyymmdd, 'label': label, 'data': usageData}

        return h

    #retrieve data from cpu_load, thus no user information 
    @cherrypy.expose
    def nodeGraph_1(self, node, start='', stop=''):
        start, stop = MyTool.getStartStopTS (start, stop, '%Y-%m-%d')

        influxClient = InfluxQueryClient.getClientInstance()
        ts2data      = influxClient.getNodeMonData_1(node,start,stop)
        #{1551721265000: {'cpu_times_idle': 1631079.17, 'cpu_times_iowait': 138.63, 'cpu_times_system': 47880.6, 'cpu_times_user': 2567541.02, 'disk_io_read_bytes': 1970365952, 'disk_io_read_count': 79077, 'disk_io_read_time': 176227, 'disk_io_write_bytes': 882140160, 'disk_io_write_count': 51308, 'disk_io_write_time': 1193976, 'load_15min': 44.11, 'load_1min': 44.06, 'load_5min': 44.05, 'mem_available': 372355952640, 'mem_buffers': 2265088, 'mem_cached': 1858367488, 'mem_free': 375478579200, 'mem_total': 405661532160, 'mem_used': 28322320384, 'net_io_rx_bytes': 676755879, 'net_io_rx_drop': 0, 'net_io_rx_err': 0, 'net_io_rx_packets': 2895866, 'net_io_tx_bytes': 683209239, 'net_io_tx_drop': 0, 'net_io_tx_err': 0, 'net_io_tx_packets': 2000433, 'proc_run': 44, 'proc_total': 1173}, 
       
        t1 = time.time()
        cpu_series = []  ##[{'data': [[1531147508000, value]...], 'name':'cpu_times_xx'}, ...] 
        for key in ['cpu_times_iowait','cpu_times_system','cpu_times_user']:
            cpu_series.append({'name': key,   'data': [[ts, d.get(key,0)]   for ts, d in ts2data.items()]})
        io_series = []
        for key in ['disk_io_read_bytes','disk_io_write_bytes','net_io_rx_bytes', 'net_io_tx_bytes']:
            io_series.append({'name': key,   'data': [[ts, d.get(key,0)]   for ts, d in ts2data.items()]})
        mem_series = []
        for key in ['mem_buffers', 'mem_cached', 'mem_used']:
            mem_series.append({'name': key,   'data': [[ts, d.get(key,0)]   for ts, d in ts2data.items()]})

        #node restart time
        restart_ts = self.queryTxtClient.getNodeUpTS([node])[node]
        ann_series = [[ts, 'Node Restart'] for ts in restart_ts]
        #job start/stop time
        job_df     = self.querySlurmClient.getNodeRunJobs(node, start, stop)
        #print ('nodeGraph_1 job_df={}'.format(job_df))
        lst  = job_df[['time_start', 'id_job', 'user']].values.tolist()
        ann_series.extend ([[ts*1000, 'Job {} ({}) Start'.format(jid, user)] for [ts, jid, user] in lst])
        lst  = job_df[['time_end', 'id_job']].values.tolist()
        ann_series.extend ([[ts*1000, 'Job {} End'.format(jid)]     for [ts, jid] in lst if ts > 0])
        lst  = job_df[['time_suspended', 'id_job']].values.tolist()
        ann_series.extend ([[ts*1000, 'Job {} Suspend'.format(jid)] for [ts, jid] in lst if ts > 0])
        #print("nodeGraph_1 prepare format take time {}".format(time.time()-t1))
        
        htmlTemp = os.path.join(wai, 'nodeGraph_1.html')
        h = open(htmlTemp).read().format(
                                   ltitle= 'CPU Usage of Node {} from {} to {}'.format(node, MyTool.getTS_strftime(start), MyTool.getTS_strftime(stop)),
                                   mtitle= 'Mem Usage of Node {} from {} to {}'.format(node, MyTool.getTS_strftime(start), MyTool.getTS_strftime(stop)),
                                   ititle= 'I/O Usage of Node {} from {} to {}'.format(node, MyTool.getTS_strftime(start), MyTool.getTS_strftime(stop)),
                                   lseries= cpu_series, iseries=io_series, mseries=mem_series,
                                   aseries= ann_series)
        return h

    @cherrypy.expose
    def nodeGraph(self, node, start='', stop=''):
        start, stop = MyTool.getStartStopTS (start, stop)

        # highcharts
        influxClient = InfluxQueryClient()
        uid2seq,start,stop = influxClient.getSlurmNodeMonData(node,start,stop)

        cpu_series = []  ##[{'data': [[1531147508000, value]...], 'name':'userXXX'}, ...] 
        mem_series = []  ##[{'data': [[1531147508000(ms), value]...], 'name':'userXXX'}, ...] 
        io_series_r  = []  ##[{'data': [[1531147508000, value]...], 'name':'userXXX'}, ...] 
        io_series_w  = []  ##[{'data': [[1531147508000, value]...], 'name':'userXXX'}, ...] 
        for uid, d in uid2seq.items():
            uname = MyTool.getUser(uid)
            cpu_node={'name': uname}
            cpu_node['data']= [[ts, d[ts][0]] for ts in d.keys()]
            cpu_series.append (cpu_node)

            mem_node={'name': uname}
            mem_node['data']= [[ts, d[ts][3]] for ts in d.keys()]
            mem_series.append (mem_node)

            io_node={'name': uname}
            io_node['data'] = [[ts, d[ts][1]] for ts in d.keys()]
            io_series_r.append (io_node)

            io_node={'name': uname}
            io_node['data'] = [[ts, d[ts][2]] for ts in d.keys()]
            io_series_w.append (io_node)

        ann_series = self.queryTxtClient.getNodeUpTS([node])[node]
        #print ('nodeGraph annotation=' + repr(ann_series))

        htmlTemp = os.path.join(wai, 'smGraphHighcharts.html')
        h = open(htmlTemp).read()%{'spec_title': ' on node  ' + str(node),
                                   'start': time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop': time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'lseries': cpu_series,
                                   'iseries_r': io_series_r,
                                   'iseries_w': io_series_w,
                                   'mseries': mem_series,
                                   'aseries': ann_series}

        return h


    @cherrypy.expose
    def nodeDetails(self, node):
        #yanbin: rewrite to seperate data and UI
        if type(self.data) == str: return self.data # error of some sort.

        nodeInfo = self.data.get(node, None)
        if nodeInfo:
           status = nodeInfo[0]
           skew   = "{0:.2f}".format(nodeInfo[1])
        else:
           status = 'Unknown'
           skew   = 'Undefined'

        procData = []
        for user, uid, cores, procs, load, trm, tvm, cmds, io, *etc in sorted(nodeInfo[USER_INFO_IDX:]):
            procData.append ([user, cores, load, cmds])
            
        jobs_alloc = self.getNode2Job()[node]
        acctReport = str(self.sacctReport(self.sacctDataInWindow(['-N', node])))
        htmlTemp   = os.path.join(wai, 'nodeDetails.html')
        #    ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
        parameters = {'node': node, 'procData': procData, 'acctReport':acctReport, 'acctWindow': SACCT_WINDOW_DAYS, 'nodeStatus':status, 'delay':skew, 'jobs':jobs_alloc}
        htmlStr    = open(htmlTemp).read().format(**parameters)
        return htmlStr

    @cherrypy.expose
    def userJobs(self, user):
        cmd = SlurmCmdQuery()

        t = htmlPreamble
        t += '<h3>Running and Pending Jobs of user <a href="./userDetails?user={0}">{0}</a> (<a href="./userJobGraph?uname={0}">Graph</a>)</a></h3>Update time:{1}'.format(user, MyTool.getTimeString(int(time.time())))
        t += self.sacctReport(cmd.sacctCmd(['-u', user, '-s', 'RUNNING,PENDING'], output='JobID,JobName,State,Partition,NodeList,AllocCPUS,Submit,Start'),
                              titles=['Job ID', 'Job Name', 'State', 'Partition','NodeList','Allocated CPUS', 'Submit','Start'])
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def userDetails(self, user):
        if type(self.data) == str: return self.data # error of some sort.

        t = htmlPreamble
        t += '<h3>User: %s (<a href="./userJobGraph?uname=%s">Graph</a>), <a href="#sacctreport">(jump to sacct)</a></h3>\n'%(user, user)
        for node, d in sorted(self.data.items()):
            if len(d) < USER_INFO_IDX: continue
            for nuser, uid, cores, procs, tc, trm, tvm, cmds, io, *etc in sorted(d[USER_INFO_IDX:]):
                if nuser != user: continue
                ac = MyTool.loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
                t += '<hr><em{0}><a href="./nodeDetails?node={1}">{1}</em></a> {2}<pre>'.format(ac, node, cores)
                t += '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'
        t += '<h4 id="sacctreport">sacct report (last %d days for user %s):</h4>\n'%(SACCT_WINDOW_DAYS, user)
        t += self.sacctReport(self.sacctDataInWindow(['-u', user]))
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def jobDetails(self, jobid):
        if type(self.data) == str: return self.data # error of some sort.

        jid    = int(jobid)
        jinfo  = self.runningJob.get(jid)
        if ( jinfo is None):
            return "Cannot find information of the job"
        start      = jinfo.get('start_time', '')
        if not start:
           start, stop=MyTool.getStartStopTS ('', '')
        
        # get current report
        alloc_str = ''
        node_str  = ''
        for node, coreCount in jinfo.get(u'cpus_allocated', {}).items():
            d          = self.data.get(node)
            node_str  += '&var-hostname={}'.format(node)
            
            if len(d) < USER_INFO_IDX: 
                alloc_str += '<hr><em>%s</em> %d<pre>'%(node, coreCount) + '\n</pre>\n'
            else:
                for user, uid, cores, procs, tc, trm, tvm, cmds, io, *etc in sorted(d[USER_INFO_IDX:]):
                    ac = MyTool.loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
                    alloc_str += '<hr><em%s>%s</em> %d<pre>'%(ac, node, coreCount) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'

        grafana_url = 'http://mon8:3000/d/jYgoAfiWz/yanbin-slurm-node-util?orgId=1&from={}{}&var-jobID={}&theme=light'.format(start*1000, node_str,jid)
        t = htmlPreamble
        t += '<h3>Job: {0} (<a href="{1}/jobGraph?jobid={0}">Graph</a>, <a href="{5}">Grafana Graph</a>), State: {2}, Num_Nodes: {3}, Num_CPUs: {4} <a href="#sacctreport">(jump to sacct)</a></h3>\n'.format(jobid, cherrypy.request.base, jinfo.get(u'job_state'), jinfo.get(u'num_nodes'), jinfo.get(u'num_cpus'), grafana_url)
        # get sacct report
        t += alloc_str    
        t += self.sacctReport(self.sacctData (['-j', jobid]), skipJobStep=False)
 
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def tymor(self,**args):
        if type(self.data) == str: return self.data # error of some sort.

        selfdata_o = self.data
        data_o     = self.runningJob
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
        jobdata    = {job:v  for jobid,v in self.runningJob.items()}

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
        data_o     = self.runningJob
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
        data_o = self.runningJob
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
           data_dash[n]['list_state']=[v['node_info'][i][0] for i in list(v['node_info'])]
           data_dash[n]['list_load']=[round(v['node_info'][i][7],3) for i in list(v['node_info']) if len(v['node_info'][i])>=7]
           data_dash[n]['list_RSS']=[v['node_info'][i][8] for i in list(v['node_info'])if len(v['node_info'][i])>=7]
           data_dash[n]['list_VMS']=[v['node_info'][i][9] for i in list(v['node_info'])if len(v['node_info'][i])>=7]
           data_dash[n]['list_cores']=[round(v['node_info'][i][5],3) for i in v['node_info'].keys() if len(v['node_info'][i])>=7]
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
    def doneJobGraph(self,jobid):
        #graph for jobs that are already finished
        slurmClient  = SlurmCmdQuery()

        info         = slurmClient.getSlurmJobInfo(jobid)       
        return self.jobGraphData(jobid, info[1], info[2], info[3], info[4])
    
    def jobGraphData (self, jobid, userid, nodelist, start, stop):
        #{hostname: {ts: [cpu, io, mem] ... }}
        influxClient = InfluxQueryClient('scclin011','slurmdb')
        node2seq     = influxClient.getSlurmUidMonData(userid, nodelist,start,stop)
      
        mem_all_nodes  = []  ##[{'data': [[1531147508000(ms), value]...], 'name':'workerXXX'}, ...] 
        cpu_all_nodes  = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...] 
        io_r_all_nodes = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...] 
        io_w_all_nodes = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...] 
        for hostname, hostdict in node2seq.items():
            cpu_node={'name': hostname}
            cpu_node['data']= [[ts, hostdict[ts][0]] for ts in hostdict.keys()]
            cpu_all_nodes.append (cpu_node)

            io_node={'name': hostname}
            io_node['data']= [[ts, hostdict[ts][1]] for ts in hostdict.keys()]
            io_r_all_nodes.append (io_node)

            io_node={'name': hostname}
            io_node['data']= [[ts, hostdict[ts][2]] for ts in hostdict.keys()]
            io_w_all_nodes.append (io_node)

            mem_node={'name': hostname}
            mem_node['data']= [[ts, hostdict[ts][3]] for ts in hostdict.keys()]
            mem_all_nodes.append (mem_node)

        # highcharts 
        htmltemp = os.path.join(wai, 'smGraphHighcharts.html')
        h = open(htmltemp).read()%{'spec_title': ' of job {}'.format(jobid),
                                   'start'     : time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop'      : time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'lseries'   : cpu_all_nodes,
                                   'mseries'   : mem_all_nodes,
                                   'iseries_r' : io_r_all_nodes,
                                   'iseries_w' : io_w_all_nodes}
        return h

        
    @cherrypy.expose
    def jobGraph(self,jobid,start='', stop=''):
        if type(self.data) == str: return self.data # error of some sort.

        jobid    = int(jobid)
        if jobid not in self.runningJob:
           return self.doneJobGraph(jobid) 

        nodelist = list(self.runningJob[jobid][u'cpus_allocated'])
        start    = self.runningJob[jobid][u'start_time']
        if not stop:
           stop     = time.time()
        userid  = self.runningJob[jobid][u'user_id']
 
        return self.jobGraphData(jobid, userid, nodelist, start, stop)

    def userNodeGraphData (self, uid, uname, start, stop):
        #{hostname: {ts: [cpu, io, mem] ... }}
        influxClient = InfluxQueryClient('scclin011','slurmdb')
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

    #return jobstarttime, data, datadescription
    def getUserJobMeasurement (self, uid):
        # get the current jobs of uid
        start, jobs = self.getUserCurrJobs (uid)
        if not jobs:
            return None, None, None
        
        # get nodes and period, TODO: mutiple jobs at one node at the same time is a problem
        # get the utilization of each jobs
        queryClient = InfluxQueryClient.getClientInstance()
        jid2df      = {}
        jid2dsc     = {}   # jid description
        for job in jobs:
            if job.get('num_nodes',-1) < 1 or not job.get('nodes', None):
               continue

            #print ("getUserJobMeasurement nodes=" + repr(job['nodes']))
            ld    = queryClient.queryUidMonData (job['user_id'], job['start_time'], '', MyTool.convert2list(job['nodes']))
            df    = pandas.DataFrame (ld)
            if df.empty:
                continue

            if 'io_read_bbytes' in df.columns:
               df    = df[['hostname', 'time', 'cpu_system_util', 'cpu_user_util', 'io_read_bytes', 'io_write_bytes', 'mem_rss']]
            else:
               df    = df[['hostname', 'time', 'cpu_system_util', 'cpu_user_util', 'mem_rss']]
               df['io_read_bytes'] = 0
               df['io_write_bytes'] = 0
            # sum over hostname to get jobs data
            sumDf = pandas.DataFrame({})
            for name, group in df.groupby('hostname'):
                group          = group.reset_index(drop=True)[['time', 'cpu_system_util', 'cpu_user_util', 'io_read_bytes', 'io_write_bytes', 'mem_rss']]
                group['count'] = 1
                if sumDf.empty:
                   sumDf = group
                else:
                   sumDf = sumDf.add(group, fill_value=0)        #sum over the same artifical index, not accurate as assuming the same start time on all nodes of jobs

            sumDf['time'] = sumDf['time']/sumDf['count']
            #print ("sumDf=" + repr(sumDf.head()))
                
            jid = job['job_id']
            jid2df[jid]  = sumDf
            jid2dsc[jid] = str(jid) + ' (' + job['nodes'] + ')' 

        return start, jid2df, jid2dsc
                
    def getWaitMsg (self):
        elapse_time = (int)(time.time() - self.startTime)

        return WAIT_MSG + repr(elapse_time) + " seconds since server restarted."

    @cherrypy.expose
    def userJobGraph(self,uname,start='', stop=''):
        if not self.runningJob: return self.getWaitMsg()

        #{jid: df, ...}
        uid                      = MyTool.getUid(uname)
        start, jid2df, jid2dsc   = self.getUserJobMeasurement (uid)
        if not jid2df:          return "User does not have running jobs at this time"
      
        #{'name': jid, 'data':[[ts, value], ...]
        series       = {'cpu':[], 'mem':[], 'io':[]}
        for jid, df in jid2df.items():
            df['cpu_time'] = df['cpu_system_util'] + df['cpu_user_util']
            df['io_bytes'] = df['io_read_bytes']   + df['io_write_bytes']
            series['cpu'].append({'name': jid2dsc[jid], 'data':df[['time','cpu_time']].values.tolist()})
            series['mem'].append({'name': jid2dsc[jid], 'data':df[['time','mem_rss']].values.tolist()})
            series['io'].append ({'name': jid2dsc[jid], 'data':df[['time','io_bytes']].values.tolist()})
        
        htmltemp = os.path.join(wai, 'scatterHC.html')
        h = open(htmltemp).read()%{'start'  : time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop'   : time.strftime('%Y-%m-%d', time.localtime(time.time())),
                                   'series1': series['cpu'], 'title1': 'CPU usage of '    +uname, 'xlabel1': 'CPU time', 'aseries1':[], 
                                   'series2': series['mem'], 'title2': 'Mem RSS usage of '+uname, 'xlabel2': 'Mem Bytes',               'aseries2':[], 
                                   'series3': series['io'],  'title3': 'IO usage of '     +uname, 'xlabel3': 'IO Bytes',             'aseries3':[], 
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

    @cherrypy.expose
    def sunburst(self):

        #sunburst
        if type(self.data) == str: return self.data# error of some sort.

        #prepare required information in data_dash
        more_data    = {k:v[0:USER_INFO_IDX] + v[USER_INFO_IDX][0:7] for k,v in self.data.items() if len(v)>USER_INFO_IDX } #flatten hostdata
        less_data    = {k:v[0:USER_INFO_IDX]                         for k,v in self.data.items() if len(v)<=USER_INFO_IDX }
        hostdata_flat= dict(more_data,**less_data)
        #print("more_data=" + repr(more_data))
        #print("less_data=" + repr(less_data))
        #print("hostdata_flat={}".format(hostdata_flat))

        keys_id      =(u'job_id',u'user_id',u'qos', u'num_nodes', u'num_cpus')
        data_dash    ={jid:{k:jinfo[k] for k in keys_id} for jid,jinfo in self.runningJob.items()} #extract set of keys
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in self.runningJob.items():
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
        #listn  =[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_loads_flat[i]] for i in range(len(list_nodes_flat))]
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
        h = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.updateTime).ctime(), 'data1' : json_load, 'data2' : json_state, 'data3' : json_vms, 'data4' : json_rss, 'users':set_usernames}
        return h

    sunburst.exposed = True
            
cherrypy.config.update({#'environment': 'production',
                        'log.access_file':    '/tmp/slurm_util/smcpgraph-html-sun.log',
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
