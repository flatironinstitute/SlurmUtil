import cherrypy, _pickle as cPickle, datetime, glob, json, os
import pandas, logging, pwd, re, subprocess as SUB, sys, time, zlib
from collections import defaultdict, OrderedDict
import operator
from functools import reduce

import fs2hc
import pyslurm
from queryInflux     import InfluxQueryClient
from querySlurm      import SlurmCmdQuery, PyslurmQuery
from querySlurmDB    import SlurmDBQuery
from queryTextFile   import TextfileQueryClient
from queryBright     import BrightRestClient
from IndexedDataFile import IndexedHostData
from EmailSender     import JobNoticeSender
from bulletinboard   import BulletinBoard

import config
import MyTool
import SlurmEntities
import inMemCache
from SlurmMonitorData import SLURMMonitorData

logger  = config.logger

# Directory where processed monitoring data lives.
SACCT_WINDOW_DAYS  = 3 # number of days of records to return from sacct.
USER_INFO_IDX      = 3
USER_PROC_IDX      = 7
WAIT_MSG           = 'No data received yet. Please wait a minute and come back. '
EMPTYDATA_MSG      = 'There is no data retrieved according to the constraints.'
EMPTYPROCDATA_MSG  = 'There is no process data present in the retrieved data.'
ONE_HOUR_SECS      = 3600
ONE_DAY_SECS       = 86400
TIME_DISPLAY_FORMAT= '%m/%d/%y %H:%M'
DATE_DISPLAY_FORMAT= '%m/%d/%y'

@cherrypy.expose
class SLURMMonitorUI(object):
    def __init__(self, monData):
        self.monData         = monData
        self.updateTS        = monData.updateTS
        self.config          = config.APP_CONFIG

        self.queryTxtClient  = TextfileQueryClient(os.path.join(config.APP_DIR, 'host_up_ts.txt'))
        self.querySlurmClient= SlurmDBQuery()
        self.jobNoticeSender = JobNoticeSender()
        self.startTime       = time.time()
        self.data            = 'No data received yet. Please wait a minute and come back.'
        self.pyslurmNode     = None

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

    @cherrypy.expose
    def qosDetail(self, qos='gpu'):
        qos           = pyslurm.qos().get()[qos]
        #ts_qos        = py_qos.lastUpdate()   # does not have lastUpdate
        htmlTemp      = os.path.join(config.APP_DIR, 'qosDetail.html')
        htmlStr       = open(htmlTemp).read().format(update_time=MyTool.getTsString(time.time()),
                                                     qos        =json.dumps(qos))
        return htmlStr

    @cherrypy.expose
    def partitionDetail(self, partition='gpu'):
        ins           = SlurmEntities.SlurmEntities()
        p_info, nodes = ins.getPartitionAndNodes (partition)
        if 'gres/gpu' in p_info['tres_fmt_str']:
           p_info['avail_tres_fmt_str']='cpu={},node={},gres/gpu={}'.format(p_info.get('avail_cpus_cnt',0),p_info.get('avail_nodes_cnt',0),p_info.get('avail_gpus_cnt',0))
        else:
           p_info['avail_tres_fmt_str']='cpu={},node={}'.format(p_info.get('avail_cpus_cnt',0),p_info.get('avail_nodes_cnt',0))

        htmlTemp      = os.path.join(config.APP_DIR, 'partitionDetail.html')
        htmlStr       = open(htmlTemp).read().format(update_time=MyTool.getTsString(ins.ts_node_dict),
                                                     p_detail   =json.dumps(p_info),
                                                     p_nodes    =json.dumps(nodes))
        return htmlStr

    @cherrypy.expose
    def pending(self, start='', stop='', state=3):
        ins        = SlurmEntities.SlurmEntities()
        pendingLst = ins.getPendingJobs()
        #latest_eval_ts= set([j['last_sched_eval'] for j in pendingLst])  #more than one
        relaxQoS   = ins.relaxQoS()
        partLst    = ins.getPartitions ()
        partS      = config.getSetting("part_avail")
        for p in partLst:
            p['running_jobs'] = ' '.join(str(e) for e in p['running_jobs'])
            p['pending_jobs'] = ' '.join(str(e) for e in p['pending_jobs'])
            if p['total_nodes'] and p['total_cpus'] and p['total_gpus']:
               if p['avail_nodes_cnt']*100/p['total_nodes'] > partS['node'] or p['avail_cpus_cnt']*100/p['total_cpus'] > partS['cpu'] or (p['total_gpus'] and p['avail_gpus_cnt']*100/p['total_gpus'] > partS['gpu']):
                  p['display_class'] = 'inform'

        htmlTemp   = os.path.join(config.APP_DIR, 'pending.html')
        htmlStr    = open(htmlTemp).read().format(update_time       =MyTool.getTsString(ins.ts_job_dict),
                                                  job_cnt           =len(pendingLst),
                                                  pending_jobs_input=json.dumps(pendingLst),
                                                  note              = '{}'.format(relaxQoS),
                                                  partitions_input  =json.dumps(partLst))
        return htmlStr

    @cherrypy.expose
    def cdfTimeReport(self, cluster, upper=90, start='', stop='', state=3):
        start,stop,cdfData = SlurmDBQuery.getJobTime(cluster, start, stop, upper)

        series     = []
        for fld in ['time_run_log', 'time_cpu_log']:
            s  = [{'type': 'spline', 'name': 'CDF', 'yAxis':1, 'zIndex': 10, 'data': cdfData[fld]['count'].values.tolist()}]
            for acct,d in cdfData[fld]['account'].items():
                s.append({'type': 'column', 'name': acct, 'data': d.values.tolist()})
            series.append (s)

        htmlTemp   = os.path.join(config.APP_DIR, 'CDFHC.html')
        t1         = 'Cluster {}: Distribution of Job Wall Time'.format(cluster)
        t2         = 'Cluster {}: Distribution of Job CPU Time'.format(cluster)
        parameters = {'start': time.strftime('%Y-%m-%d', time.localtime(start)),
                      'stop':  time.strftime('%Y-%m-%d', time.localtime(stop)),
                      'series': series[0], 'title': t1, 'xMax': int(cdfData['time_run_log']['upper_x']+10),'xLabel': 'Seconds',  'yLabel': 'Count',
                      'series2':series[1], 'title2':t2, 'xMax2':int(cdfData['time_cpu_log']['upper_x'])+10,'xLabel2':'Seconds',  'yLabel2':'Count'}
        htmlStr    = open(htmlTemp).read().format(**parameters)
        return htmlStr

    @cherrypy.expose
    def cdfReport(self, cluster, upper=90, start='', stop=''):
        start,stop,cdfData = SlurmDBQuery.getJobCount (cluster, start, stop, upper)

        series     = []
        for fld in ['cpus_req', 'cpus_alloc', 'nodes_req', 'nodes_alloc']:
            s = [{'type': 'spline', 'name': 'CDF', 'yAxis':1, 'zIndex': 10, 'data': cdfData[fld]['count'].values.tolist()}]
            for acct,d in cdfData[fld]['account'].items():
                s.append({'type': 'column', 'name': acct, 'data': d.values.tolist()})
            series.append (s)

        htmlTemp   = os.path.join(config.APP_DIR, 'CDFHC_3.html')
        t1         = 'Cluster {}: Distribution of Job Requested CPUs'.format(cluster)
        t2         = 'Cluster {}: Distribution of Job Allocated CPUs'.format(cluster)
        t3         = 'Cluster {}: Distribution of Job Requested Nodes'.format(cluster)
        t4         = 'Cluster {}: Distribution of Job Allocated Nodes'.format(cluster)
        parameters = {'start': time.strftime('%Y-%m-%d', time.localtime(start)),
                      'stop':  time.strftime('%Y-%m-%d', time.localtime(stop)),
                      'series': series[0], 'title': t1, 'xMax': int(cdfData['cpus_req']['upper_x']+10),  'xLabel': 'Number of CPUs',  'yLabel': 'Count',
                      'series2':series[1], 'title2':t2, 'xMax2':int(cdfData['cpus_alloc']['upper_x'])+10,'xLabel2':'Number of CPUs',  'yLabel2':'Count',
                      'series3':series[2], 'title3':t3, 'xMax3':int(cdfData['nodes_req']['upper_x']+10),'xLabel3':'Number of Nodes','yLabel3':'Count',
                      'series4':series[3], 'title4':t4, 'xMax4':int(cdfData['nodes_alloc']['upper_x']+10),'xLabel4':'Number of Nodes','yLabel4':'Count'}
        htmlStr    = open(htmlTemp).read().format(**parameters)
        return htmlStr

    @cherrypy.expose
    def accountReport_hourly(self, cluster, start='', stop=''):
        #sumDf index ['id_tres','acct', 'time_start'],

        start, stop, sumDf = SlurmDBQuery.getAccountUsage_hourly(cluster,start, stop)

        sumDfg      = sumDf.groupby('id_tres')
        tresSer     = {} # {1: [{'data': [[ms,value],...], 'name': uid},...], 2:...}
        for tres in sumDf.index.get_level_values('id_tres').unique():
            tresSer[tres] = []
            acctIdx       = sumDfg.get_group(tres).index.get_level_values('acct').unique()
            for acct in acctIdx:
                sumDf.loc[(tres,acct,), ]
                tresSer[tres].append({'name': acct, 'data':sumDf.loc[(tres,acct,), ['ts_ms', 'alloc_ratio']].values.tolist()})

        #generate data
        htmlTemp = os.path.join(config.APP_DIR, 'scatterHC.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': tresSer[1], 'title1': '{}: CPU Allocation'.format(cluster),  'xlabel1': 'Avg CPU',    'aseries1':[],
                                   'series2': tresSer[2], 'title2': '{}: Mem Allocation'.format(cluster), 'xlabel2': 'Avg MEM MB', 'aseries2':[],
                                   'series3': tresSer[4], 'title3': '{}: Node Allocation'.format(cluster), 'xlabel3': 'Avg Node',   'aseries3':[]}

        return h

    @cherrypy.expose
    def userReport_hourly(self, cluster, start='', stop='', top=5, acct='all'):
        # get top 5 user for each resource
        if acct=='all':
           acct = None
        start, stop, tresSer  = SlurmDBQuery.getUserReport_hourly(cluster, start, stop, int(top), acct)

        #cpuLst   = tresSer[1]
        #start    = min(cpuLst, key=(lambda item: (item['data'][0][0])))['data'][0][0]  /1000
        #stop     = max(cpuLst, key=(lambda item: (item['data'][-1][0])))['data'][-1][0]/1000
        htmlTemp = os.path.join(config.APP_DIR, 'scatterHC.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': tresSer[1], 'title1': "{}: Top Users' CPU Allocation".format(cluster), 'xlabel1': 'CPU', 'aseries1':[],
                                   'series2': tresSer[2], 'title2': "{}: Top Users' Mem Allocation".format(cluster), 'xlabel2': 'MEM MB',   'aseries2':[],
                                   'series3': tresSer[4], 'title3': "{}: Top Users' Node Allocation".format(cluster),'xlabel3': 'Node', 'aseries3':[]}

        return h

    @cherrypy.expose
    def forecast_old(self, page=None):
        clu_dict = PyslurmQuery.getSlurmDBClusters()
        clu_name = list(clu_dict.keys())
        htmltemp = os.path.join(config.APP_DIR, 'forecast.html')
        #h        = open(htmltemp).read().format(clusters=clu_name)
        h        = open(htmltemp).read()
 
        return h

    @cherrypy.expose
    def forecast(self):
        clusters = list(PyslurmQuery.getSlurmDBClusters())
        clusters = ['slurm', 'slurm_plus_day']
        f_lst   = sorted(glob.glob('./public/images/{}_cpuAllocDF_*_forecast.png'.format(clusters[0])))
        f_lst   = [os.path.splitext(os.path.split(fname)[1])[0] for fname in f_lst]     #filename no dir no ext
        f_lst = [fname.split('_')[-2] for fname in f_lst]
        htmlTemp = os.path.join(config.APP_DIR, 'forecastImage.html')
        h = open(htmlTemp).read().format(clusters=clusters, accounts=f_lst)
        return h

    @cherrypy.expose
    def clusterForecast_hourly(self, cluster):
        f_lst = sorted(glob.glob('./public/images/{}_cpuAllocDF_*_forecast.png'.format(cluster)))
        f_lst = [os.path.splitext(os.path.split(fname)[1])[0] for fname in f_lst]     #filename no dir no ext
        f_lst = [fname.split('_')[-2] for fname in f_lst]
        htmlTemp = os.path.join(config.APP_DIR, 'forecastImage.html')
        h = open(htmlTemp).read().format(cluster=cluster, accounts=f_lst)
        return h

    @cherrypy.expose
    def clusterHistory(self, start='', stop='', days=7):
        start, stop  = MyTool.getStartStopTS (start, stop)

        #influxClient = InfluxQueryClient.getClientInstance()
        influxClient = InfluxQueryClient(self.config['influxdb']['host'])
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

        htmlTemp = os.path.join(config.APP_DIR, 'jobResourceReport.html')
        h = open(htmlTemp).read().format(start=time.strftime('%Y-%m-%d', time.localtime(start)),
                                         stop =time.strftime('%Y-%m-%d', time.localtime(stop)),
                                         series_11=series11, series_12=series12, xlabel1='Node Count',
                                         series_21=series21, series_22=series22, xlabel2='CPU Count')

        return h

    PENDING_REGEX = OrderedDict([('Sched','Dependency|BeginTime|JobArrayTaskLimit'), ('Resource','Resources|Priority|ReqNodeNotAvail*|Nodes_required_for_job_are_DOWN*'), ('GPU', 'Resources_GPU|Priority_GPU'), ('QoSGrp', 'QOSGrp*'), ('QoS', 'QOS*')])  #order matters
    def getReason2Cate (self, reason):
        if not reason:
            return 'Other'
        for cate, regex in SLURMMonitorUI.PENDING_REGEX.items():
            if re.match(regex, reason):
               return cate
        return 'Other'

    @cherrypy.expose
    def pending_history(self, start='', stop='', days=''):
        note         = ''
        days         = int(days) if days else 7
        start, stop  = MyTool.getStartStopTS (start, stop, days, setStop=False)

        influxClient = InfluxQueryClient(self.config['influxdb']['host'])
        start, stop, tsReason2Cnt = influxClient.getPendingCount(start, stop)
        reasons      = [set(reasons.keys()) for ts, reasons in tsReason2Cnt.items()]
        reasons      = set([i2 for item in reasons for i2 in item])  # the unique reasons
        reason2cate  = dict([(reason, self.getReason2Cate(reason)) for reason in reasons])
        cates        = set(reason2cate.values())
        #print('reasons={}, cates={}, reason2cate={}'.format(reasons, cates, reason2cate))

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
        cates_sort = [cate for cate in cate2title.keys() if cate in cates]
        series1    = [{'name':cate2title[cate], 'data':cate2ts_cnt[cate]} for cate in cates_sort]
        htmlTemp   = os.path.join(config.APP_DIR, 'pendingJobReport.html')
        h          = open(htmlTemp).read().format(start=time.strftime(DATE_DISPLAY_FORMAT, time.localtime(start)),
                                                  stop =time.strftime(DATE_DISPLAY_FORMAT, time.localtime(stop)),
                                                  series1=series1, title1='Cluster Job Queue Length', xlabel1='Queue Length',
                                                  other_reason=list(other_reason_set),
                                                  note=note)

        return h

    @cherrypy.expose
    def jobHistory(self, cluster='slurm', start='', stop=''):
        start, stop     = MyTool.getStartStopTS (start, stop, '%Y-%m-%d', days=30)     #default curr-30days, curr
        start, stop, dfs = SlurmDBQuery.getClusterJobHistory (cluster, start, stop)
        for df in dfs:
            df['time'] = df['time'] * 1000
        total_cpu  = 63096
        total_node = 940
        series1    = [{'data': dfs[0][['time', 'count']].values.tolist(),  'name': 'Total Job'},
                      {'data': dfs[1][['time', 'count']].values.tolist(), 'name': 'Exec. Job'}]
        series2    = [{'data': dfs[2][['time', 'count']].values.tolist(),  'name': 'Req CPUs'},
                      {'data': dfs[3][['time','count']].values.tolist(),  'name': 'Req CPUs of Exec. Jobs'},
                      {'data': dfs[4][['time', 'count']].values.tolist(),  'name': 'Alloc. CPUs'},
                      {'data': [[start*1000,total_cpu], [stop*1000,total_cpu]], 'name': 'Total CPUs'}]
        series3    = [{'data': dfs[5][['time', 'count']].values.tolist(),  'name': 'Req Nodes'},
                      {'data': dfs[6][['time', 'count']].values.tolist(),  'name': 'Req Nodes of Exec. Jobs'},
                      {'data': dfs[7][['time', 'count']].values.tolist(),  'name': 'Alloc. Nodes'},
                      {'data': [[start*1000,total_node], [stop*1000,total_node]], 'name': 'Total Nodes'}]
        htmlTemp = os.path.join(config.APP_DIR, 'jobHistory.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': series1, 'title1': 'Cluster Job',   'xlabel1': '# Job', 'aseries1':[],
                                   'series2': series2, 'title2': 'Cluster CPUs',  'xlabel2': '# CPU', 'aseries2':[],
                                   'series3': series3, 'title3': 'Cluster Nodes', 'xlabel3': '# Node','aseries3':[]}

        return h

    @cherrypy.expose
    def queueLengthReport(self, cluster, start='', stop='', queueTime=0):
        start, stop     = MyTool.getStartStopTS (start, stop, '%Y-%m-%d', days=30)     #default curr-30days, curr
        start, stop, df = self.querySlurmClient.getClusterJobQueue (cluster, start, stop, int(queueTime))

        #df = index | time | value
        #convert to highchart format, cpu, mem, energy (all 0, ignore)
        df['time'] = df['time'] * 1000
        series1   = [{'data': df[['time', 'value0']].values.tolist(), 'name': 'Job Queue Length'}]
        series2   = [{'data': df[['time', 'value1']].values.tolist(), 'name': 'Requested CPUs of Queued Job'}]

        htmlTemp = os.path.join(config.APP_DIR, 'seriesHC_2.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': series1, 'title1': 'Cluster Job Queue Length', 'xlabel1': 'Queue Length', 'aseries1':[],
                                   'series2': series2, 'title2': 'Requested CPUs of Queued Job', 'xlabel2': 'CPUs', 'aseries2':[]}

        return h

    @cherrypy.expose
    def clusterReport_hourly(self, start='', stop='', action='', cluster=''):
        start, stop, cpuDf, memDf = SlurmDBQuery.getClusterUsage_hourly(cluster, start, stop)

        #convert to highchart format, cpu, mem, energy (all 0, ignore)
        #get indexed of all cpu data, retrieve useful data
        cpu_alloc  = cpuDf[['ts_ms', 'alloc_secs']].values.tolist()
        cpu_down   = cpuDf[['ts_ms', 'tdown_secs']].values.tolist()
        cpu_idle   = cpuDf[['ts_ms', 'idle_secs']].values.tolist()
        cpu_resv   = cpuDf[['ts_ms', 'resv_secs']].values.tolist()
        cpu_over   = cpuDf[['ts_ms', 'over_secs']].values.tolist()
        cpu_series = [
                      {'data': cpu_over,  'name': 'Over secs', 'visible': False},
                      {'data': cpu_down,  'name': 'Down secs'},
                      {'data': cpu_idle,  'name': 'Idle secs'},
                      {'data': cpu_resv,  'name': 'Reserve secs'},
                      {'data': cpu_alloc, 'name': 'Alloc secs'},
                     ]  ##[{'data': [[1531147508000(ms), value]...], 'name':'userXXX'}, ...]

        mem_alloc  = memDf.loc[:,['ts_ms', 'alloc_secs']].values.tolist()
        mem_down   = memDf.loc[:,['ts_ms', 'tdown_secs']].values.tolist()
        mem_idle   = memDf.loc[:,['ts_ms', 'idle_secs']].values.tolist()
        mem_resv   = memDf.loc[:,['ts_ms', 'resv_secs']].values.tolist()
        mem_over   = memDf.loc[:,['ts_ms', 'over_secs']].values.tolist()
        mem_series = [
                      {'data': mem_over,  'name': 'Over secs'},
                      {'data': mem_down,  'name': 'Down secs'},
                      {'data': mem_idle,  'name': 'Idle secs'},
                      {'data': mem_resv,  'name': 'Reserve secs'},
                      {'data': mem_alloc, 'name': 'Alloc secs'},
                     ]  ##[{'data': [[1531147508, value]...], 'name':'userXXX'}, ...]

        cpu_ann    = cpuDf.groupby('count').first().loc[:,['ts_ms', 'total_secs']].reset_index().values.tolist()
        mem_ann    = memDf.groupby('count').first().loc[:,['ts_ms', 'total_secs']].reset_index().values.tolist()

        htmlTemp = os.path.join(config.APP_DIR, 'seriesHC_2.html')
        h = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': json.dumps(cpu_series), 'title1': "Cluster {}: CPU Hourly Report".format(cluster), 'xlabel1': 'CPU core secs', 'aseries1':cpu_ann,
                                   'series2': mem_series, 'title2': 'Cluster {}: Mem Hourly Report'.format(cluster), 'xlabel2': 'MEM MB secs',   'aseries2':mem_ann}

        return h

    @cherrypy.expose
    def getAllJobData(self):
        return '({},{})'.format(self.monData.updateTS, self.monData.allJobs)

    def getSummaryTableData_1(self, gpudata, gpu_jid2data=None):
        lst_lst = self.getSummaryTableData (self.monData.data, self.monData.currJobs, self.monData.node2jids, self.monData.pyslurmNodes, gpudata, gpu_jid2data)
        # in self.monData.currJobs, if gpus_allocated, then get the value
        # gpudata[gpu_name][node_name]
        return [dict(zip(config.SUMMARY_TABLE_COL, lst)) for lst in lst_lst]

    def getSummaryTableData(self, hostData, jobData, node2jobs, pyslurmNode, gpudata=None, gpu_jid2data=None):
        result    = []
        for node, nodeInfo in sorted(hostData.items()):
            if node not in pyslurmNode:
               logger.error("getSummaryTableData node {} not in pyslurmNode".format(node))
               continue

            node_mem_M = pyslurmNode[node]['real_memory']
            if len(nodeInfo) < USER_INFO_IDX:
               logger.error("getSummaryTableData nodeInfo wrong format {}:{}".format(node, nodeInfo))
               continue

            #status display no extra
            status, delay, ts = nodeInfo[0], nodeInfo[1], nodeInfo[2]
            if status.endswith(('@','+','$','#','~','*')):  #format status diaplay
               status = status[:-1]
            if ( node in node2jobs) and node2jobs[node]:  # node has running jobs
               for jid in node2jobs[node]:                # for each job on the node, add one item
                  job_user    = MyTool.getUser(jobData[jid]['user_id'])
                  job_coreCnt = jobData[jid]['cpus_allocated'][node]
                  job_runTime = int(ts)-jobData[jid]['start_time']
                  if job_runTime < 0:
                     logger.warning("job {} rumTime <0 {}-{}".format(jid, ts, jobData[jid]['start_time']))
                  # check job proc information
                  job_cpuUtil, job_rss, job_vms, job_iobyteps, job_procCnt, job_fds = self.monData.getJobUsageOnNode(jid, jobData[jid], node, nodeInfo)
                  if job_procCnt and (job_runTime>0):     # has proc information
                     job_avg_cpu = self.monData.getJobNodeTotalCPUTime(jid, node) / job_runTime if jobData[jid]['start_time'] > 0 else 0
                     job_info    = [node, status, delay, node_mem_M, jid, job_user, job_coreCnt, job_runTime, job_procCnt, job_cpuUtil, job_avg_cpu, job_rss, job_vms, job_iobyteps, job_fds]
                  else:
                     job_info    = [node, status, delay, node_mem_M, jid, job_user, job_coreCnt, job_runTime, 0,           0,           0,           0,       0,       0,            0]
                  # check job gpu information
                  #logger.info("jobData{}={}".format(jid, jobData[jid]))
                  job_gpuCnt  = len(jobData[jid]['gpus_allocated'][node]) if node in jobData[jid].get('gpus_allocated',{}) else 0
                  if job_gpuCnt:
                     job_gpuUtil    = self.getJobGPUUtil_node(jobData[jid], node, gpudata) if gpudata else 0
                     job_avgGPUUtil = gpu_jid2data[node][jid]                              if gpu_jid2data and node in gpu_jid2data else 0
                     job_info.extend ([job_gpuCnt, job_gpuUtil, job_avgGPUUtil])
                  result.append(job_info)
            else:                                          # node has no allocated jobs
               result.append([node, status, delay, node_mem_M])

        return result

    def getJobGPUUtil_node (self, job, nodename, gpudata):
        if not gpudata:
           return 0
        #gpudata[gpuname][nodename]
        if nodename in job['gpus_allocated']:
           return sum([gpudata['gpu{}'.format(idx)].get(nodename,0) for idx in job['gpus_allocated'][nodename]])
        else:
           return 0

    @cherrypy.expose
    def getNodeUtil (self, **args):
        return self.getNodeUtilData (self.pyslurmNode, self.data)

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

    #data1=self.pyslurmNode, data2=self.data
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
    def getAllocJobsWithLabel (self):
        jobs    = []
        for jid, jobinfo in self.monData.currJobs.items():  # running jobs
            if jobinfo['tres_alloc_str']:  # alloc resource
               long_label = '{}({}) alloc {}'.format (jid, jobinfo['user'], jobinfo['tres_alloc_str'])
               disabled   = ""
            else:
               logger.error("getAllocJobsWithLabel should not come here {}".format(jobinfo))
            jobs.append({"job_id":jid, "long_label":long_label, "disabled":disabled})
        return jobs

    #generate users with job allocation with info {user, jobs, long_label}
    def getAllocUsersWithLabel (self):
        users_dict = defaultdict(list)
        for jid, jobinfo in self.monData.currJobs.items():
            if jobinfo['tres_alloc_str']:
               users_dict[jobinfo['user']].append (jid)
        users      = []
        for user, jobs in sorted(users_dict.items()):
            job_count = len(jobs)
            jobs_str  = '{}'.format(jobs) if job_count <=5 else '{}'.format(jobs[0:5]).replace(']',', ...]')
            users.append({"user":user, "jobs":jobs, "long_label":'{} has {} job {}'.format(user, job_count, jobs_str)})
        return users

    def getHeatMapSetting (self):
        settings = config.getSettings()
        return settings["heatmap_avg"], settings["heatmap_weight"]

        #if 'user' in cherrypy.session:
        #   return cherrypy.session["settings"]["heatmap_avg"], cherrypy.session["settings"]["heatmap_weight"]
        #else:
        #   return self.config["settings"]["heatmap_avg"], self.config["settings"]["heatmap_weight"]

    def getHeatmapData (self, gpudata, weight, avg_minute=0):
        workers = self.monData.getHeatmapWorkerData(gpudata, weight, avg_minute)
        jobs    = self.getAllocJobsWithLabel ()  #jobs list  [{job_id, long_label, disabled}, ...]
        users   = self.getAllocUsersWithLabel()  #users list [{user, jobs, long_label}, ...]
        return workers,jobs,users

    def getNoDataPage (self, title, page):
        msg      = self.getWaitMsg ()
        htmltemp = os.path.join(config.APP_DIR, 'noData.html')
        h        = open(htmltemp).read().format(update_time = MyTool.getTsString(int(time.time())),
                                                title       = title,
                                                page        = page,
                                                msg         = msg)
        return h

    @cherrypy.expose
    def utilHeatmap(self, **args):
        if not self.monData.hasData():
           return self.getNoDataPage ('Host Utilization Heatmap', 'utilHeatmap')

        avg_minute, weight   = self.getHeatMapSetting()
        gpu_nodes,max_gpu_cnt= PyslurmQuery.getGPUNodes(self.monData.pyslurmNodes)
        logger.info ("gpu_nodes={}".format(gpu_nodes))
        gpu_ts, gpudata      = BrightRestClient().getAllGPUAvg (gpu_nodes, minutes=avg_minute["gpu"], max_gpu_cnt=max_gpu_cnt)
        workers,jobs,users   = self.getHeatmapData (gpudata, weight, avg_minute["cpu"])

        htmltemp = os.path.join(config.APP_DIR, 'heatmap.html')
        h        = open(htmltemp).read()%{'update_time': MyTool.getTsString(self.monData.updateTS),
                                          'data1'      : json.dumps(workers),
                                          'data2'      : json.dumps(jobs),
                                          'users'      : json.dumps(users),
                                          'gpu'        : json.dumps(gpudata),
                                          'gpu_update_time' : MyTool.getTsString(gpu_ts),
                                          'gpu_avg_minute'  : json.dumps(avg_minute["gpu"])}

        return h

    @cherrypy.expose
    def report(self, page=None):
        accounts = sorted(SlurmCmdQuery.getAccounts())
        htmltemp = os.path.join(config.APP_DIR, 'report.html')
        h = open(htmltemp).read().format(accounts=accounts)

        return h

    @cherrypy.expose
    def getHeader(self, page=None):
        pages =["index", "utilHeatmap", "pending", "sunburst", "usageGraph", "bulletinboard", "report",   "forecast", "settings"]
        titles=["Summary", "Host Util.",  "Pending Jobs", "Sunburst Graph", "File Usage", "Bulletin Board", "Report", "Forecast", "Settings"]
        result='<ul class="nav__inner">'

        for i in range (len(pages)):
           if ( pages[i] == page ):
              result += '<li class="nav__item nav__item--active"><a class="nav__link" href="/' + pages[i] + '">' + titles[i] + '</a></li>'
           else:
              result += '<li class="nav__item"><a class="nav__link" href="/' + pages[i] + '">' + titles[i] + '</a></li>'

        if ( page == "inputSearch"):
            result += '<li class="nav__item nav__item--active nav__item--search"><a class="nav__link nav__link--search" href="/inputSearch"></a></li></ul>'
        else:
            result += '<li class="nav__item nav__item nav__item--search"><a class="nav__link nav__link--search" href="/inputSearch"></a></li></ul>'

        return result

    @cherrypy.expose
    def getBreadcrumbs(self, path=None):
        crumbs='<a href="/" class="crumb hero__title--crumb">Home</a>'
        # TODO: Add function to parse worker and job route

        # splitPath=path.split('/')
        # for i in range (len(splitPath)):
        #     crumbs += '<a href="/" class="crumb">' + i + '</a>'

        return crumbs


    def getSummaryColumn (self):
        settings = config.getSettings()
        return settings["summary_column"]
    def getSummaryUtilAlarm (self):
        settings = config.getSettings()
        return settings["summary_low_util"], settings["summary_high_util"]

    @cherrypy.expose
    def index(self, **args):
        if not self.monData.hasData():
           return self.getNoDataPage ('Tabular Summary', 'index')

        column      = [key for key, val in self.getSummaryColumn().items() if val]
        gpudata     = None
        gpu_jid2data= None
        if 'gpu_util' in column:
           gpu_nodes,max_gpu_cnt   = self.monData.getCurrJobGPUNodes()
           logger.info("max_gpu_cnt={},gpu_nodes={}".format(max_gpu_cnt, gpu_nodes))
           if max_gpu_cnt:
              gpu_ts, gpudata      = BrightRestClient().getAllGPUAvg (gpu_nodes, minutes=5, max_gpu_cnt=max_gpu_cnt)
        if 'avg_gpu_util' in column:
           min_start_ts, gpu_detail= self.monData.getCurrJobGPUDetail()   #gpu_detail include job's start_time
           minutes                 = int(int(time.time()) - min_start_ts)/60
           gpu_ts_d, gpu_jid2data  = BrightRestClient().getAllGPUAvg_jobs(gpu_detail, minutes)
           #logger.info('---index gpudata_d={}'.format(gpu_jid2data))
        data      = self.getSummaryTableData_1 (gpudata, gpu_jid2data)
        alarms    = self.getSummaryUtilAlarm()
        user      = config.getUser()
        htmltemp  = os.path.join(config.APP_DIR, 'index.html')
        h         = open(htmltemp).read().format(table_data =json.dumps(data),
                                                 update_time=MyTool.getTsString(self.monData.updateTS),
                                                 column     =column,
                                                 alarms     =json.dumps(alarms),
                                                 user       =json.dumps(user))
        return h

    def getUserAllocGPU (uid, node_dict):
        rlt      = {}
        rlt_jobs = []
        jobs     = PyslurmQuery.getUserCurrJobs(uid)
        if jobs:
           for job in jobs:
               job_gpus = SLURMMonitorData.getJobAllocGPU(job, node_dict)
               for node, gpu_ids in job_gpus.items():
                   rlt_jobs.append(job)
                   if node in rlt:
                      rlt[node].extend (gpu_ids)
                   else:
                      rlt[node] = gpu_ids
        return rlt, rlt_jobs



    @cherrypy.expose
    def getLowResourceJobs (self, job_length_secs=ONE_DAY_SECS, job_width_cpus=1, job_cpu_avg_util=0.1, job_mem_util=0.3):
        return self.monData.getLowResourceJobs(job_length_secs, job_width_cpus, job_cpu_avg_util, job_mem_util)

    def getLUJSettings (self):
        luj_settings = config.getSetting('low_util_job')
        return luj_settings['cpu']/100, luj_settings['gpu']/100, luj_settings['mem']/100, luj_settings['run_time_hour'], luj_settings['alloc_cpus']

    def getLongrunLowUtilJobs (self):
        low_util, low_gpu, low_mem, long_period, job_width = self.getLUJSettings()
        long_period       = long_period * 3600          # hours -> seconds
        jobs              = self.monData.getCurrLUJobs (low_util, long_period, job_width, low_mem)
        #msgs              = BulletinBoard.getLowUtilJobMsg (jobs)
        BulletinBoard.setLowUtilJobMsg (jobs)

        return list(jobs.values())

    @cherrypy.expose
    def getUnbalancedJobs (self, job_cpu_avg_util=0.1, job_mem_util=0.3, job_io_bps=1000000):
        jobs   = self.monData.currJobs
        ts     = self.monData.updateTS
        job_cpu_avg_util = float(job_cpu_avg_util)
        job_mem_util     = float(job_mem_util)
        job_io_bps       = int(job_io_bps)

        result = {}            # return {jid:job,...}
        for jid, job in jobs.items():
            #if job run long enough
            if (job['job_avg_util'] < job_cpu_avg_util) and (job['job_mem_util']>job_mem_util or job['job_io_bps'] > job_io_bps):
               result[job['job_id']] = job
        logger.info('getUnbalancedJobs {}'.format(result.keys()))
        return json.dumps([ts, result])

    @cherrypy.expose
    def getUnbalLoadJobs (self, cpu_stdev, rss_stdev, io_stdev):
        cpu_stdev, rss_stdev, io_stdev = int(cpu_stdev), int(rss_stdev), int(io_stdev)
        self.calculateStat (self.monData.currJobs, self.data)
        sel_jobs = [(jid, job) for jid, job in self.monData.currJobs.items()
                         if (job['node_cpu_stdev']>cpu_stdev) or (job['node_rss_stdev']>rss_stdev) or (job['node_io_stdev']>io_stdev)]
        return json.dumps ([self.monData.updateTS, dict(sel_jobs)])

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
        ts          = int(time.time())
        update_time = MyTool.getTsString(int(time.time()))
        if not yyyymmdd:
            # in general, only have census date up to yesterday, if not availabe, return the latest date
            yyyymmdd = MyTool.getTS_strftime(ts, '%Y%m%d')
        usageData_dict= fs2hc.gendata(yyyymmdd)
        for k,v in usageData_dict.items():
            v[2] = '{}-{}-{}'.format(v[2][0:4], v[2][4:6], v[2][6:8]) #convert from '%Y%m%d' to '%Y-%m-%d'
        htmlTemp    = 'fileCensus.html'
        h           = open(htmlTemp).read().format(file_systems=fs2hc.FileSystems,data=usageData_dict, update_time=update_time)

        return h

    @cherrypy.expose
    def user_fileReport(self, uid, start='', stop='', days=180):
        # click from File Usage
        start, stop   = MyTool.getStartStopTS (start, stop, '%Y-%m-%d', int(days))
        fc_seq,bc_seq = fs2hc.gendata_user(int(uid), start, stop)

        htmlTemp = os.path.join(config.APP_DIR, 'userFile.html')
        h        = open(htmlTemp).read()%{
                                   'start':   time.strftime(DATE_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop':    time.strftime(DATE_DISPLAY_FORMAT, time.localtime(stop)),
                                   'spec_title': MyTool.getUser(uid),
                                   'series1': json.dumps(fc_seq),
                                   'series2': json.dumps(bc_seq)}
        return h

    @cherrypy.expose
    def fileReport_daily(self, fs='home', start='', stop='', top=5):
        start, stop  = MyTool.getStartStopTS (start, stop, '%Y-%m-%d')
        fcSer, bcSer = fs2hc.gendata_all(fs, start, stop, int(top))
        if not fcSer:
           return EMPTYDATA_MSG

        start        = fcSer[0]['data'][0][0]/1000
        stop         = fcSer[0]['data'][-1][0]/1000

        htmlTemp = os.path.join(config.APP_DIR, 'seriesHC.html')
        h        = open(htmlTemp).read()%{
                                   'start':   time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop':    time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'series1': json.dumps(fcSer), 'title1': 'File count daily report', 'xlabel1': 'File Count', 'aseries1':[],
                                   'series2': json.dumps(bcSer), 'title2': 'Byte count daily report', 'xlabel2': 'Byte Count',   'aseries2':[]}
        return h

    @cherrypy.expose
    def nodeGraph(self, node,start='', stop=''):
        start, stop = MyTool.getStartStopTS (start, stop, formatStr='%Y-%m-%d')
        logger.info("start={},stop={}".format(MyTool.getTsString(start), MyTool.getTsString(stop)))

        msg = self.nodeGraph_cache(node, start, stop)
        note  = 'cache'
        if not msg:
           logger.info('Node {}: no data during {}-{} in cache'.format(node, start, stop))
           msg = self.nodeGraph_influx(node, start, stop)
           note  = 'influx'
        if not msg:
           logger.info('Node {}: no data during {}-{} returned from influx'.format(node, start, stop))
           msg = self.nodeGraph_file(node, start, stop)
           note  = 'file'
        if (not msg) or (len(msg)<5):
           return 'Node {}: no data during {}-{} in cache, influx and saved file. Note: {}'.format(node, start, stop, msg)

        #ann_series = self.queryTxtClient.getNodeUpTS([node])[node]
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms

        if len(msg)==6: #from cache or influx, 'first_ts', 'last_ts', cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes
           htmltemp = os.path.join(config.APP_DIR, 'nodeGraph.html')
           h = open(htmltemp).read()%{'spec_title': ' of {}'.format(node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_r' : msg[4],
                                   'iseries_w' : msg[5]}
        else: #from file, start, stop, cpu_all_seq, mem_all_seq, io_all_seq
           htmltemp = os.path.join(config.APP_DIR, 'nodeGraph_2.html')
           h = open(htmltemp).read()%{'spec_title': ' of {}'.format(node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_rw': msg[4]}
        return h

    def nodeGraph_cache(self, node, start, stop):
        nodeInfo, cpu_all_nodes, mem_all_nodes, io_all_nodes = self.monData.inMemCache.queryNode(node, start, stop)
        if nodeInfo:
           return nodeInfo['first_ts'], nodeInfo['last_ts'], cpu_all_nodes, mem_all_nodes, io_all_nodes
        else:
           return None

    def nodeGraph_influx(self, node, start, stop):
        # highcharts
        #influxClient = InfluxQueryClient()
        influxClient = InfluxQueryClient(self.config['influxdb']['host'])
        uid2seq,start,stop = influxClient.getSlurmNodeMonData(node,start,stop)
        if not uid2seq:
           return None

        cpu_series,mem_series,io_series_r,io_series_w = [],[],[],[]
                                                 ##[{'data': [[1531147508000, value]...], 'name':'userXXX'}, ...]
        for uid, d in uid2seq.items():
            uname = MyTool.getUser(uid)
            cpu_series.append  ({'name': uname, 'data':[[ts, d[ts][0]] for ts in d.keys()]})
            mem_series.append  ({'name': uname, 'data':[[ts, d[ts][1]] for ts in d.keys()]})
            io_series_r.append ({'name': uname, 'data':[[ts, d[ts][2]] for ts in d.keys()]})
            io_series_w.append ({'name': uname, 'data':[[ts, d[ts][3]] for ts in d.keys()]})

        return start, stop, cpu_series, mem_series, io_series_r, io_series_w

    def nodeGraph_file(self, node, start, stop):
        hostData                             = IndexedHostData(self.config["fileStorage"]["dir"])
        cpu_all_seq, mem_all_seq, io_all_seq = hostData.queryDataHosts([node], start, stop)
        if (not cpu_all_seq) and (not mem_all_seq) and (not mem_all_seq):
           return ["No data in file for node {} during {}-{}".format(node, MyTool.getTsString(start), MyTool.getTsString(stop))]

        return start, stop, cpu_all_seq, mem_all_seq, io_all_seq

    @cherrypy.expose
    def nodeDetails(self, node):
        if not self.monData.hasData():
           return self.getWaitMsg() # error of some sort.
        nodeData    = self.monData.getNodeProc(node)
        if not nodeData:
           return "Node {} is not monitored".format(node)

        nodeDisplay = pyslurm.node().get()[node]
        if nodeData['gpus']:
           nodeDisplay['gpus']         = nodeData['gpus']
        if nodeData['alloc_gpus']:
           nodeDisplay['alloc_gpus']   = nodeData['alloc_gpus']
        if nodeData['jobProc']:
           nodeDisplay['running_jobs'] = list(nodeData['jobProc'].keys())

        nodeReport     = SlurmCmdQuery.sacct_getNodeReport(node, days=3)
        array_het_jids = [ job['JobID'] for job in nodeReport if '_' in job['JobID'] or '+' in job['JobID']]
        htmlTemp       = os.path.join(config.APP_DIR, 'nodeDetail.html')
        htmlStr        = open(htmlTemp).read().format(update_time    = MyTool.getTsString(nodeData['updateTS']),
                                                      node_name      = node,
                                                      node_data      = nodeData,
                                                      node_info      = json.dumps(nodeDisplay),
                                                      array_het_jids = array_het_jids,
                                                      node_report    = nodeReport)
        return htmlStr

    @cherrypy.expose
    def userDetails(self, user, days=3):
        uid            = MyTool.getUid(user)
        if not uid:
           return 'Cannot find uid of user {}!'.format(user)

        userAssoc      = SlurmCmdQuery.getUserAssoc(user)
        ins            = SlurmEntities.SlurmEntities()
        userjob        = ins.getUserJobsByState (uid)  # can also get from sacct -u user -s 'RUNNING, PENDING'
        part           = ins.getAccountPartition (userAssoc['Account'], uid)
        for p in part:  #replace big number with n/a
            for k,v in p.items():
                if v == SlurmEntities.MAX_LIMIT: p[k]='n/a'
        if self.monData.hasData():
           note       = ''
           userworker = self.monData.getUserNodeData(user)
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
        pending_jobs  = userjob.get('PENDING',[])
        tres_alloc    = [MyTool.getTresDict(j['tres_alloc_str']) for j in running_jobs]
        userAssoc['uid']        = uid
        userAssoc['partitions'] = [p['name'] for p in part]
        userAssoc['running_jobs'] = [j['job_id'] for j in running_jobs]
        if pending_jobs:
           userAssoc['pending_jobs'] = [j['job_id'] for j in pending_jobs]
        userAssoc['alloc_cpus'] = sum([t['cpu']  for t in tres_alloc])
        userAssoc['alloc_nodes']= sum([t['node'] for t in tres_alloc])
        userAssoc['alloc_gpus'] = sum([t.get('gres/gpu',0) for t in tres_alloc])
        userAssoc['alloc_mem']  = MyTool.sumOfListWithUnit([t['mem']  for t in tres_alloc if 'mem' in t])

        htmlTemp   = os.path.join(config.APP_DIR, 'userDetail.html')
        htmlStr    = open(htmlTemp).read().format(user        =MyTool.getUserFullName(uid),
                                                  uid         =uid,
                                                  uname       =user,
                                                  user_assoc  = userAssoc,
                                                  update_time = MyTool.getTsString(ins.ts_job_dict),
                                                  running_jobs=json.dumps(running_jobs),
                                                  pending_jobs=json.dumps(pending_jobs),
                                                  worker_proc =json.dumps(userworker), note=note,
                                                  part_info   =json.dumps(part),
                                                  array_het_jids=array_het_jids, job_history=past_job, day_cnt = days)
        return htmlStr
        # Currently, running jobs & pending jobs of the user processes in the worker nodes
        # Future, QoS and resource restriction
        # Past,   finished or cancelled jobs for the last 3 days

    @cherrypy.expose
    def jobByName(self, name='script', curr_jid=None):
        fields    =['id_job','job_name', 'id_user','state', 'nodelist', 'time_start','time_end', 'exit_code', 'tres_req', 'tres_alloc', 'gres_req', 'gres_alloc', 'work_dir']
        data      = SlurmDBQuery.getJobByName(name, fields)  #user, duration is added by the function
        d_flds    = {'id_job':'Job ID', 'state':'State', 'user':'User', 'nodelist':'Alloc Node', 'time_start':'Start', 'time_end':'End', 'duration':'Duration', 'exit_code':'Exit', 'tres_req':'Req Tres', 'gres_req':'Req Gres', 'work_dir':'Work Dir'}
        total_cnt = len(data)
        if len(data) > 100:
           data=data[-100:]
        for d in data:
           d['time_start'] = MyTool.getTsString(d['time_start'])
           d['time_end']   = MyTool.getTsString(d['time_end'])

        htmlTemp   = os.path.join(config.APP_DIR, 'jobByName.html')
        htmlStr    = open(htmlTemp).read().format(job_name=name, job_cnt=total_cnt, job_list=data, job_title=d_flds)
        return htmlStr
        #return '{}\n{}'.format(fields, data)

    @cherrypy.expose
    def userGPUGraph (self, user):
        uid                     = MyTool.getUid (user)
        jobs_gpu_alloc,jobs_gpu = SLURMMonitorUI.getUserAllocGPU(uid, pyslurm.node().get())  #{'workergpu01':[0,1]
        if not jobs_gpu_alloc:                            #TODO: done jobs
           return "User {} does not have jobs using GPU.".format(user)

        start_ts     = min([job['start_time'] for job in jobs_gpu])
        brightClient = BrightRestClient()
        max_gpu_id   = max([i for id_lst in jobs_gpu_alloc.values() for i in id_lst])
        gpu_dict     = brightClient.getGPU (list(jobs_gpu_alloc.keys()), start_ts, max_gpu_id=max_gpu_id)
        series       = []
        idx          = 0
        for node,gpu_list in jobs_gpu_alloc.items():
            for gpu_id in gpu_list:
                name = '{}.gpu{}'.format(node, gpu_id)
                if gpu_dict[name][0][0] < (start_ts - 600) * 1000:
                   gpu_dict[name][0][0] = start_ts * 1000
                series.append ({'name':'{} ({})'.format(jobs_gpu[idx]['job_id'], name), 'data':gpu_dict[name]})
            idx +=1

        htmltemp = os.path.join(config.APP_DIR, 'nodeGPUGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' of User {}'.format(user),
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(start_ts)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(int(time.time()))),
                                   'series'    : json.dumps(series)}
        return h

    @cherrypy.expose
    def jobGPUGraph (self, jid):
        jid = int(jid)
        job = PyslurmQuery.getCurrJob(jid)
        if not job:                            #TODO: done jobs
           return "Job {} is not running/pending or done in last 600 seconds(slurm.conf::MinJobAge.)".format(jid)

        job_start    = job['start_time']
        if not job['gres_detail']:
           return "No GPU Alloc for job {}".format(jid)
        job_gpu_alloc= SLURMMonitorData.getJobAllocGPU(job, pyslurm.node().get())  #{'workergpu01':[0,1]
        max_gpu_id   = max([i for id_lst in job_gpu_alloc.values() for i in id_lst])
        d            = BrightRestClient().getNodesGPU_Mem (list(job_gpu_alloc.keys()), job_start, max_gpu_id=max_gpu_id, msec=True)
        gpu_dict     = d['gpu_utilization']
        mem_dict     = d['gpu_fb_used']

        #display only the allocated gid
        series       = []
        series2      = []
        for node,gpu_list in job_gpu_alloc.items():
            for gpu_id in gpu_list:
                name = '{}.gpu{}'.format(node, gpu_id)
                series.append  ({'name':name, 'data':gpu_dict[name]})
                series2.append ({'name':name, 'data':mem_dict[name]})

        htmltemp = os.path.join(config.APP_DIR, 'nodeGPUGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' of Job {}'.format(jid),
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(job_start)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(int(time.time()))),
                                   'series'    : json.dumps(series),
                                   'series2'   : json.dumps(series2)}
        return h

    def getDoneJobProc (self, jid, job_report):
        if job_report['End'] == 'Unknown':
           return 'Can not find End time for a non-running job', {}

        influxClient = InfluxQueryClient(self.config['influxdb']['host'])
        node2procs   = influxClient.queryJobProc (jid, MyTool.nl2flat(job_report['NodeList']), MyTool.str2ts(job_report['Start']), MyTool.str2ts(job_report['End']))
        if not node2procs:
           return "WARNING: no record of user's process has been saved in the database.", {}

        worker2proc    = {}
        for node, procs in node2procs.items():
            worker2proc[node]=[0,len(procs),0,0,0,[],0]  #[alloc_core_cnt, proc_cnt, total_cpu, t_rss, t_vms, rlt_procs, t_io]
            for proc in procs:
                worker2proc[node][5].append([proc['pid'], '{:.2f}'.format(proc['avg_util']), MyTool.getDisplayKB(proc.get('mem_rss_K',0)), MyTool.getDisplayKB(proc.get('mem_vms_K',0)), MyTool.getDisplayBps(proc.get('avg_io',0)), proc['cmdline']])

        return ['PID', 'Avg CPU Util', 'RSS',  'VMS', 'IO Rate', 'Command'], worker2proc

    @cherrypy.expose
    def jobDetails(self, jid):
        if ',' in jid:
           jid         = jid.split(',')[0]
        jid            = int(jid)
        ts             = int(time.time())

        jobstep_report = SlurmCmdQuery.sacct_getJobReport(jid)
        if not jobstep_report:
           return "Cannot find job {}".format(jid)
        job_report     = jobstep_report[0]
        job            = PyslurmQuery.getCurrJob(jid)
        if not job:
           job         = PyslurmQuery.getSlurmDBJob(jid)

        msg_note       = ''
        worker2proc    = {}
        proc_disp_field= []
        if not job or (job['job_state'] not in ['RUNNING','PENDING']):           #job is not running or pending
           job_name    = job_report['JobName']
           proc_disp_field, worker2proc = self.getDoneJobProc(jid, job_report)   #get result from influx
           if not worker2proc:
              msg_note = proc_disp_field
        else:                                                                    #running job's proc is in self.data
           job_name    = job['name']
           if job['job_state'] == 'RUNNING':
              if not self.monData.hasData():
                 msg_note = self.getWaitMsg()
              else: # should in the data
                 ts                           = self.monData.updateTS
                 proc_disp_field, worker2proc = self.monData.getJobProc (jid)
                 if not worker2proc:
                    msg_note = "Cannot find Job {} in the current data".format(jid)
                 else:
                    if len(worker2proc) != int(job_report['AllocNodes']):
                       msg_note='WARNING: Job {} is running on {} nodes, which is less than {} allocated nodes.'.format(jid, len(worker2proc), job_report['AllocNodes'])
        job_report['ArrayJobID']         = job_report['JobID'] if '_' in job_report['JobID'] else None
        job_report['HeterogeneousJobID'] = job_report['JobID'] if '+' in job_report['JobID'] else None
        if job:
           job['user']                   = MyTool.getUser(job['user_id'])
           job['ArrayJobID']             = job_report['ArrayJobID']
           job['HeterogeneousJobID']     = job_report['HeterogeneousJobID']

        #grafana_url = 'http://mon8:3000/d/jYgoAfiWz/yanbin-slurm-node-util?orgId=1&from={}{}&var-jobID={}&theme=light'.format(start*1000, '&var-hostname=' + '&var-hostname='.join(MyTool.nl2flat(job_report['NodeList'])), jid)

        htmlTemp   = os.path.join(config.APP_DIR, 'jobDetail.html')
        htmlStr    = open(htmlTemp).read().format(job_id     =jid,
                                                  job_name   =job_name,
                                                  update_time=datetime.datetime.fromtimestamp(ts).ctime(),
                                                  job        =json.dumps(job),
                                                  job_info   =json.dumps(job_report),
                                                  title_list =proc_disp_field,
                                                  proc_cnt   =sum([val[1] for val in worker2proc.values()]),
                                                  worker_proc=json.dumps(worker2proc),
                                                  note       =msg_note,
                                                  job_report =json.dumps(jobstep_report))
        return htmlStr

    @cherrypy.expose
    def tymor(self,**args):
        if not self.monData.hasData():
           return self.getNoDataPage('Tymor', 'tymor2') # error of some sort.

        selfdata_o = self.monData.data
        data_o     = self.monData.currJobs
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
            x  = [node for node, coreCount in jinfo.get(u'cpus_allocated').items()]
            d1 = {k: selfdata_dict[k] for k in x if k in selfdata_dict}
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

        htmlTemp = os.path.join(config.APP_DIR, 'sparkline_std.html')
        #h = open(htmlTemp).read()%{'tablespark' : t, 'tablespark2' : t2 }
        h = open(htmlTemp).read()%{'tablespark' : t }

        return h

    @cherrypy.expose
    def jobGraph(self, jobid, history="FULL", start="", stop=""):
        jobid = int(jobid)
        if history != "FULL":
           start, stop = MyTool.getStartStopTS(start, stop, formatStr='%Y-%m-%dT%H:%M')
        else:
           start, stop = None, None
        msg   = self.jobGraph_cache(jobid, start, stop)
        note  = 'cache'
        if not msg:
           logger.info('Job {}: no data in cache'.format(jobid))
           msg = self.jobGraph_influx(jobid, start, stop)
           note= 'influx'
        if not msg:
           logger.info('Job {}: no data returned from influx'.format(jobid))
           msg = self.jobGraph_file(jobid)
           note='file'
        if (not msg) or (len(msg)<5):
           return 'Job {}: no data in cache, influx and saved file. Note: {}'.format(jobid, msg)
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms

        if len(msg)==6:
           htmltemp = os.path.join(config.APP_DIR, 'nodeGraph.html')
           h = open(htmltemp).read()%{'spec_title': ' of job {}'.format(jobid),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_r' : msg[4],
                                   'iseries_w' : msg[5]}
        else:
           htmltemp = os.path.join(config.APP_DIR, 'nodeGraph_2.html')
           h = open(htmltemp).read()%{'spec_title': ' of job {}'.format(jobid),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : msg[2],
                                   'mseries'   : msg[3],
                                   'iseries_rw': msg[4]}
        return h

    def jobGraph_file(self, jobid):
        if jobid in self.monData.currJobs:
           job = self.monData.currJobs[jobid]
           if 'user' not in job:
              job['user'] = MyTool.getUser(job['user_id'])
        else:
           job = PyslurmQuery.getSlurmDBJob (jobid, req_fields=['start_time', 'end_time', 'user', 'nodes'])
        logger.info("job={}".format(job))
        if not job:
           return ["No data in file for job {}:{}".format(jobid, job)]

        start    = job['start_time']
        stop     = job['end_time'] if job['end_time'] else int(time.time())
        nodelist = MyTool.nl2flat(job['nodes'])
        hostData = IndexedHostData(self.config["fileStorage"]["dir"])
        cpu_all_seq, mem_all_seq, io_all_seq = hostData.queryDataHosts(nodelist, job['start_time'], stop, job['user'])
        if (not cpu_all_seq) and (not mem_all_seq) and (not mem_all_seq):
           return ["No data in file for job {}:{}".format(jobid, job)]
        else:
           return start, stop, cpu_all_seq, mem_all_seq, io_all_seq

    def jobGraph_influx(self, jobid, start=None, stop=None):
        jobid        = int(jobid)
        influxClient = InfluxQueryClient()
        queryRlt     =influxClient.getJobMonData_hc(jobid, start, stop)
        return queryRlt

    def jobGraph_cache(self, jobid, start=None, stop=None):
        jobid  = int(jobid)
        #job, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes= self.monData.inMemCache.queryJob(jobid)
        jobRlt = self.monData.inMemCache.queryJob(jobid, start, stop)

        # highcharts
        if jobRlt:  #job is in cache
           job, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes = jobRlt
           if job and cpu_all_nodes:  # has job in cache
              count = sum([len(n['data']) for n in cpu_all_nodes])
              if count:               # has data in cache
                 # check whether cached enough data
                 minTS = min([n['data'][0][0]  for n in cpu_all_nodes if n['data']])
                 maxTS = max([n['data'][-1][0] for n in cpu_all_nodes if n['data']])
                 if (not start) or (start < job['start_time']):
                    start = job['start_time']
                 if minTS < job['start_time'] + 180: # tolerate 3 minutes
                    return minTS, maxTS, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes
                 else:
                    logger.warning("jobGraph_cache: job {} data in cache is not complete ({} << {})".format(jobid, job['submit_time'], minTS))

        logger.info("jobGraph_cache: no job {} in cache".format(jobid))
        return None

    def nodeJobProcGraph_cache(self, node, jobid):
        return None

    def nodeJobProcGraph_file(self, node, jobid):
        return None

    def nodeJobProcGraph_influx(self, node, jobid, start):
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

        return first, last, cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes

    @cherrypy.expose
    def nodeGPUGraph(self, node, hours=72):  #the GPU util for the node of last hours 
        nodeData= pyslurm.node().get_node(node)
        if not nodeData:
           return 'Node {} is not in slurm cluster.'.format(node)
        if not nodeData[node]['gres']:
           return 'Node {} does not have gres resource'.format(node)
        stop      = int(time.time())
        start     = stop - hours * 60 * 60
        data      = BrightRestClient().getNodesGPU_Mem({node:[0,1,2,3]}, start, msec=True)
        util_data = data['gpu_utilization']
        mem_data  = data['gpu_fb_used']
   
        series  = []
        for node_gpu,s in util_data.items():
            series.append({'name':node_gpu, 'data':s})
        series2  = []
        for node_gpu,s in mem_data.items():
            series2.append({'name':node_gpu, 'data':s})

        htmltemp = os.path.join(config.APP_DIR, 'nodeGPUGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' on {}'.format(node),
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(stop)),
                                   'series'    : json.dumps(series),
                                   'series2'    : json.dumps(series2)}
        return h

    @cherrypy.expose
    def nodeJobProcGraph(self, node, jid):
        jobid = int(jid)
        job   = self.monData.getJob (jobid, req_fields=['start_time'])
        if not job:
           return 'Job {}: cannot find the job.'.format(jobid)
        if not job['start_time']:
           return 'Job {}: job is not started.'.format(jobid)
        msg   = self.nodeJobProcGraph_cache  (node, jobid)
        note  = 'cache'
        if not msg:
           logger.info('Job {}: no data in cache'.format(jobid))
           msg = self.nodeJobProcGraph_influx(node, jobid, job['start_time'])
           note= 'influx'
        if not msg:
           logger.info('Job {}: no data returned from influx'.format(jobid))
           msg = self.nodeJobProcGraph_file(node, jobid)
           note='file'
        if not msg:
           return 'Job {}: no data in cache, influx and saved file'.format(jobid)
        for idx in range(2,len(msg)):
            for seq in msg[idx]:
                dict_ms = [ [ts*1000, value] for ts,value in seq['data']]
                seq['data'] = dict_ms

        htmltemp = os.path.join(config.APP_DIR, 'nodeGraph.html')
        h = open(htmltemp).read()%{'spec_title': ' of job {} on {}'.format(jobid, node),
                                   'note'      : note,
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
                                   'lseries'   : json.dumps(msg[2]),
                                   'mseries'   : json.dumps(msg[3]),
                                   'iseries_r' : json.dumps(msg[4]),
                                   'iseries_w' : json.dumps(msg[5])}
        return h

    #return jobstarttime, data, datadescription
    def getUserJobMeasurement (self, uid):
        # get the current jobs of uid
        uid  = int(uid)
        jobs = PyslurmQuery.getUserCurrJobs (uid, self.monData.currJobs)
        if not jobs:
            return None, None, None
        start = min([j['start_time'] for j in jobs])  #curr jobs always have start_time

        # get nodes and period,
        # get the utilization of each jobs
        #queryClient = InfluxQueryClient.getClientInstance()
        ifxClient = InfluxQueryClient(self.config['influxdb']['host'])
        jid2df    = {}
        jid2dsc   = {}   # jid description
        for job in jobs:
            if job.get('num_nodes',-1) < 1 or not job.get('nodes', None):
               logger.warning("WARNING getUserJobbMeasurement:job does not have allocated nodes information {}".format(job))
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
                del group['hostname']
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
            logger.info('getUserJobMeasurement reconstruct result takes {}'.format(time.time()-t1))
        return start, jid2df, jid2dsc

    def getWaitMsg (self):
        elapse_time = (int)(time.time() - self.startTime)
        return WAIT_MSG + repr(elapse_time) + " seconds since server restarted."

    @cherrypy.expose
    def userJobGraph(self,user,start='', stop=''):
        if not self.monData.hasData(): return self.getWaitMsg()

        #{jid: df, ...}
        uid                      = MyTool.getUid(user)
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
        htmltemp = os.path.join(config.APP_DIR, 'nodeGraph_2.html')
        h        = open(htmltemp).read()%{'spec_title': ' of user {}'.format(user),
                                   'note'      : 'influxdb',
                                   'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(start)),
                                   'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(time.time())),
                                   'lseries'   : json.dumps(series['cpu']),
                                   'mseries'   : series['mem'],
                                   'iseries_rw' : series['io'],
                                   }
        return h

    @cherrypy.expose
    def userGraph(self,user,start='', stop=''):
        uid          = MyTool.getUid(user)
        start, stop  = MyTool.getStartStopTS (start, stop, formatStr='%Y-%m-%d')

        return self.userNodeGraphData(uid, user, start, stop)

    def userNodeGraphData (self, uid, uname, start, stop):
        #{hostname: {ts: [cpu, io, mem] ... }}
        influxClient = InfluxQueryClient(self.config['influxdb']['host'])
        node2seq     = influxClient.getSlurmUidMonData_All(uid, start,stop)

        mem_all_nodes = []  ##[{'data': [[1531147508000(ms), value]...], 'name':'workerXXX'}, ...]
        cpu_all_nodes = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...]
        io_all_nodes  = []  ##[{'data': [[1531147508000, value]...], 'name':'workerXXX'}, ...]
        for hostname, hostdict in node2seq.items():
            mem_node={'name': hostname}
            mem_node['data']= [[ts, hostdict[ts][1]] for ts in hostdict.keys()]
            mem_all_nodes.append (mem_node)

            cpu_node={'name': hostname}
            cpu_node['data']= [[ts, hostdict[ts][0]] for ts in hostdict.keys()]
            cpu_all_nodes.append (cpu_node)

            io_node={'name': hostname}
            io_node['data']= [[ts, hostdict[ts][2]+hostdict[ts][3]] for ts in hostdict.keys()]
            io_all_nodes.append (io_node)
        ann_series = []

        # highcharts
        #if len(msg)==6: #from cache or influx, 'first_ts', 'last_ts', cpu_all_nodes, mem_all_nodes, io_r_all_nodes, io_w_all_nodes
        #   htmltemp = os.path.join(config.APP_DIR, 'nodeGraph_2.html')
        #   h = open(htmltemp).read()%{'spec_title': ' of {}'.format(node),
        #                           'note'      : note,
        #                           'start'     : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[0])),
        #                           'stop'      : time.strftime(TIME_DISPLAY_FORMAT, time.localtime(msg[1])),
        #                           'lseries'   : cpu_all_n,
        #                           'mseries'   : msg[3],
        #                           'iseries_rw' : msg[4],
        #                           'iseries_w' : msg[5]}
        htmltemp = os.path.join(config.APP_DIR, 'nodeGraph_2.html')
        h = open(htmltemp).read()%{'spec_title': ' of user ' + uname,
                                   'note'      : 'data from influx',
                                   'start'     : time.strftime('%Y-%m-%d', time.localtime(start)),
                                   'stop'      : time.strftime('%Y-%m-%d', time.localtime(stop)),
                                   'lseries'   : cpu_all_nodes,
                                   'mseries'   : mem_all_nodes,
                                   'iseries_rw': io_all_nodes}
        return h

    @cherrypy.expose
    def sunburst(self):
        if not self.monData.hasData():
           return self.getNoDataPage ('Tabular Summary', 'sunburst')
        d_core, d_cpu_util, d_rss_K, d_io_bps, d_state = self.monData.test()

        htmltemp = os.path.join(config.APP_DIR, 'sunburst2.html')
        h = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.monData.updateTS).ctime(), 
                                   'data1':json.dumps(d_core),
                                   'data2':json.dumps(d_cpu_util), 'data3' : json.dumps(d_rss_K), 
                                   'data4' : json.dumps(d_io_bps), 'data5':json.dumps(d_state)}
        return h

    @cherrypy.expose
    def sunburst_old(self):
        if not self.monData.hasData():
           return self.getNoDataPage ('Tabular Summary', 'sunburst')

        d_load, d_vms, d_rss, d_state, list_usernames_flatn = self.monData.getSunburstData()
        json_load = json.dumps(d_load,  sort_keys=False)
        json_vms  = json.dumps(d_vms, sort_keys=False)
        json_rss  = json.dumps(d_rss, sort_keys=False)
        json_state= json.dumps(d_state, sort_keys=False)

        #get all the usernames
        set_usernames = sorted(set(list_usernames_flatn))

        htmltemp = os.path.join(config.APP_DIR, 'sunburst2.html')
        h = open(htmltemp).read()%{'update_time': datetime.datetime.fromtimestamp(self.monData.updateTS).ctime(), 'data1' : json_load, 'data2' : json_state, 'data3' : json_vms, 'data4' : json_rss, 'users':set_usernames}
        return h

    @cherrypy.expose
    def inputSearch(self):
        userLst    = sorted(MyTool.getAllUsers ())
        jobLst     = [str(jid) for jid in sorted(pyslurm.job().get().keys())]
        nodeLst    = sorted(pyslurm.node().get().keys())
        partLst    = sorted(pyslurm.partition().get().keys())
        htmlTemp   = os.path.join(config.APP_DIR, 'search.html')
        htmlStr    = open(htmlTemp).read().format(users=userLst, jobs=jobLst, nodes=nodeLst, partitions=partLst)
        return htmlStr

    @cherrypy.expose
    def bulletinboard(self):
        if self.monData.hasData():
           ts_str     = MyTool.getTsString(self.monData.updateTS)
           low_util   = self.getLongrunLowUtilJobs()
           # node with low resource utlization
           low_nodes  = self.monData.getLowUtilNodes()
        else:
           ts_str     = MyTool.getTsString(int(time.time()))
           low_util   = [{'id':'', 'user':'',  'low_util_msg':self.getWaitMsg()}]
           low_nodes  = [{'name':'', 'low_util_msg':self.getWaitMsg()}]
        other      = self.monData.inMemLog.getAllLogs () #[{'source':'', 'ts':'','msg':''}]

        #SE_ins     = SlurmEntities.SlurmEntities()
        #qos_relax  = SE_ins.relaxQoS()   # {uid:suggestion}
        #qos_relax  = [ {'user':MyTool.getUser(uid), 'msg':s} for uid,s in qos_relax.items()]

        htmlTemp   = os.path.join(config.APP_DIR, 'bulletinboard.html')
        htmlStr    = open(htmlTemp).read().format(update_time =ts_str,
                                                  low_util    =json.dumps(low_util),
                                                  low_node    =json.dumps(low_nodes),
                                                  #qos_relax   =json.dumps(qos_relax),
                                                  #qos_relax_ts=MyTool.getTsString(SE_ins.ts_job_dict),
                                                  other       =other)
        return htmlStr

    @cherrypy.expose
    def settings (self, **settings):
        if settings:
           logger.info("settings{}".format(settings))
           self.chg_settings (settings)
        settings = config.getSettings()
        htmlTemp   = os.path.join(config.APP_DIR, 'settings.html')
        htmlStr    = open(htmlTemp).read().format(settings=json.dumps(settings))
        return htmlStr

    def chg_settings (self, settings):
        chg_flag    = False
        setting_key = settings.pop("setting_key")   # which settings, such as 'summary_column'
        chkbox_str  = settings.pop("checkbox")
        if chkbox_str:
           new_settings  = dict([(key,False) for key in chkbox_str.split(',') if key not in settings])   #false in checkbox is not reported, use "checkbox" combined with true value to decide false
        else:
           new_settings  = {}
        for key, value in settings.items():
            if value.isdigit(): # false value is not returned
               value = int(value)
            elif value == "true":
               value = True
            new_settings[key] = value
        logger.info("---new settings{}".format(new_settings))
        config.setSetting(setting_key, new_settings)

if __name__=="__main__":
   cherrypy.config.update({#'log.access_file':    '/tmp/slurm_util/smcpgraph-html-sun.log',
                           'log.screen':         True,
                           'tools.sessions.on':              True,
                           'server.socket_host': '0.0.0.0',
                           'server.socket_port': 8128})
   conf = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(config.APP_DIR, 'public'),
    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(config.APP_DIR, 'public/images/sf.ico'),
    },
   }
   conf1 = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(config.APP_DIR, 'public'),
    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(config.APP_DIR, 'public/images/sf.ico'),
    },
   }

   sm_data = SLURMMonitorData()
   cherrypy.tree.mount(SLURMMonitorUI(sm_data), '/',     conf)
   cherrypy.tree.mount(sm_data,                 '/data', conf1)

   #cherrypy.engine.signals.subscribe()
   cherrypy.engine.start()
   cherrypy.engine.block()
