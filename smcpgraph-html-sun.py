import cherrypy, _pickle as cPickle, csv, datetime, json, os
import pandas as pd, pwd, pyslurm as SL, re, subprocess as SUB, sys, time, zlib
from collections import defaultdict as DDict

import fs2hc
import scanSMSplitHighcharts

wai = os.path.dirname(os.path.realpath(sys.argv[0]))

WebPort = int(sys.argv[1])

# Directory where processed monitoring data lives.
SMDir = sys.argv[2]

htmlPreamble = '''\
<!DOCTYPE html>
<html>
<head>
    <link href="/static/css/style.css" rel="stylesheet">
</head>
<body>
'''

SacctWindow = 3 # number of days of records to return from sacct.

def loadSwitch(c, l, low, normal, high):
    if c == -1 or c < l-1: return high
    if c > l+1: return low
    return normal

@cherrypy.expose
class SLURMMonitor(object):

    def __init__(self):
        self.data = 'No data received yet. Wait a few minutes and come back.'
        self.jobData = {}
        self.rawNodeData = None
        self.updateTime = 'BOOM'
        self.userJobData = None

    @cherrypy.expose
    def getHostData(self):
        return repr(self.data)

    @cherrypy.expose
    def hello(self):
        return "hello, world"

    @cherrypy.expose
    def getJobData(self):
        return repr(self.jobData)

    @cherrypy.expose
    def getRawNodeData(self):
        return repr(self.rawNodeData)

    @cherrypy.expose
    def getUserJobData(self):
        return repr(self.userJobData)

    def getNode2Jobs (self, jobData):
        node2jobs = DDict(list)
        for jid, jinfo in jobData.items():
            for nd, coreCount in jinfo.get(u'cpus_allocated', {}).items():
                node2jobs[nd].append(jid)

        return node2jobs
        
    def getTableCol(self, hostData, jobData):
        node2jobs = self.getNode2Jobs (jobData)

        result=[]
        for node, v in sorted(hostData.items()):
            status = v[0]
            if len(v) > 1: delay= v[1]
            else:          delay= None
            if ( node2jobs.get(node) ):
               for job in node2jobs.get(node):
                  if len(v) > 3:
                     for uname, uid, coreNum, proNum, load, rss, vms, pp in sorted(v[3:]):
                        result.append([node, status, job, delay, uname, coreNum, proNum, load, rss, vms])
                  else:
                     result.append([node, status, job, delay ])
            else:
               result.append([node, status, ' ', delay])
                
        return result
        
    @cherrypy.expose
    def test(self, **args):
        htmltemp = os.path.join(wai, 'header.html')
        h = open(htmltemp).read()
 
        return open("header.html")

    @cherrypy.expose
    def getHeader(self, page=None):
        pages=["index", "sunburst", "usageGraph", "tymor", "tymor2"]
        titles=["Tabular Summary", "Sunburst Graph", "Usage Graph", "Tymor", "Tymor2"]
 
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

        tableData = self.getTableCol(self.data, self.jobData)
        
        htmltemp = os.path.join(wai, 'index3.html')
        h = open(htmltemp).read()%{'tableData' : tableData}
 
        return h

    @cherrypy.expose
    def updateSlurmData(self, **args):
        d =  cherrypy.request.body.read()
        ts, self.jobData, newdata, self.rawNodeData = cPickle.loads(zlib.decompress(d))

        if type(self.data) != dict: self.data = {}
        for k,v in newdata.items(): self.data[k] = v
        #open("/mnt/xfs1/home/thamamsy/projects/slurmonitor/current/tmp/tymorusd","w+").write(d)
        self.updateTime = time.asctime(time.localtime(ts))
        #print ('Got new data', self.updateTime, len(d), len(self.data), len(newdata), file=sys.stderr)

    def sacctData (self, criteria):
        cmd = ['sacct', '-n', '-P', '-o', 'JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'] + criteria
        try:
            #TODO: capture standard error separately?
            d = SUB.check_output(cmd, stderr=SUB.STDOUT)
        except SUB.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        return d.decode('utf-8')

    def sacctDataInWindow(self, criteria):
        t = datetime.date.today() + datetime.timedelta(days=-SacctWindow)
        startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
        d = self.sacctData (['-S', startDate] + criteria)

        return d

    @cherrypy.expose
    def sacctReport(self, d, skipJobStep=True):
        t = '''\
<table class="slurminfo">
<tr><th>Job ID</th><th>Job Name</th><th>Allocated CPUS</th><th>State</th><th>Exit Code</th><th>User</th><th>Node List</th><th>Start</th><th>End</th></tr>
'''
        jid2info = DDict(list)
        for l in d.splitlines():
            print(l)
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
    def usageGraph(self, yyyymmdd='', fs='home'):
        if not yyyymmdd:
            # only have census date up to yesterday, so we use that as the default.
            yyyymmdd = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        label, usageData = fs2hc.gendata(yyyymmdd, fs)
        htmlTemp = 'fileCensus.html'

        h = open(htmlTemp).read()%{'yyyymmdd': yyyymmdd, 'label': label, 'data': usageData}

        return h

    @cherrypy.expose
    def nodeGraph(self, node, start='', stop=''):
        threeDays = 3*24*3600
        if start:
            start = time.mktime(time.strptime(start, '%Y%m%d'))
            if stop:
                stop = time.mktime(time.strptime(stop, '%Y%m%d'))
            else:
                stop = start + threeDays
        else:
            if stop:
                stop = time.mktime(time.strptime(stop, '%Y%m%d'))
            else:
                stop = time.time()
            start = max(0, stop - threeDays)

        # highcharts
        getSMData = scanSMSplitHighcharts.getSMData
        htmlTemp = os.path.join(wai, 'smGraphHighcharts.html')
        lseries, mseries = getSMData(SMDir, node, start, stop)
        h = open(htmlTemp).read()%{'node': node,
                                          'start': time.strftime('%Y/%m/%d', time.localtime(start)),
                                          'stop': time.strftime('%Y/%m/%d', time.localtime(stop)),
                                          'lseries': lseries,
                                          'mseries': mseries}

        return h


    @cherrypy.expose
    def nodeDetails(self, node):
        if type(self.data) == str: return self.data # error of some sort.

        t = htmlPreamble
        d = self.data.get(node, [])
        try:    status = d[0]
        except: status = 'Unknown'
        try:    skew = d[1]
        except: skew = -9.99
        t += '<h3>Node: %s (<a href="%s/nodeGraph?node=%s">Graph</a>), Status: %s, Delay: %.2f, <a href="#sacctreport">(jump to sacct)</a></h3>\n'%(node, cherrypy.request.base, node, status, skew)
        for user, uid, cores, procs, tc, trm, tvm, cmds in sorted(d[3:]):
            ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
            t += '<hr><em%s>%s</em> %d<pre>'%(ac, user, cores) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'
        t += '<h4 id="sacctreport">sacct report (last %d days for node %s):</h4>\n'%(SacctWindow, node)
        t += self.sacctReport(self.sacctDataInWindow(['-N', node]))
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def userDetails(self, user):
        if type(self.data) == str: return self.data # error of some sort.

        t = htmlPreamble
        t += '<h3>User: %s, <a href="#sacctreport">(jump to sacct)</a></h3>\n'%(user)
        for node, d in sorted(self.data.items()):
            if len(d) < 3: continue
            for nuser, uid, cores, procs, tc, trm, tvm, cmds in sorted(d[3:]):
                if nuser != user: continue
                ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
                t += '<hr><em%s>%s</em> %d<pre>'%(ac, node, cores) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'
        t += '<h4 id="sacctreport">sacct report (last %d days for user %s):</h4>\n'%(SacctWindow, user)
        t += self.sacctReport(self.sacctDataInWindow['-u', user])
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def jobDetails(self, jobid):
        if type(self.data) == str: return self.data # error of some sort.

        jid    = int(jobid)
        jinfo  = self.jobData.get(jid)
        print (jinfo)
        if ( jinfo is None):
            return "Cannot find information of the job"

        t = htmlPreamble
        t += '<h3>Job: %s (<a href="%s/jobGraph?jobid=%s">Graph</a>), State: %s, Num_Nodes: %d, Num_CPUs: %d <a href="#sacctreport">(jump to sacct)</a></h3>\n'%(jobid, cherrypy.request.base, jobid, jinfo.get(u'job_state'), jinfo.get(u'num_nodes'), jinfo.get(u'num_cpus'))

        # get current report
        for node, coreCount in jinfo.get(u'cpus_allocated', {}).items():
            d = self.data.get(node)
            
            if len(d) < 3: 
                t += '<hr><em>%s</em> %d<pre>'%(node, coreCount) + '\n</pre>\n'
            else:
                for user, uid, cores, procs, tc, trm, tvm, cmds in sorted(d[3:]):
                    ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
                    t += '<hr><em%s>%s</em> %d<pre>'%(ac, node, coreCount) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'

        # get sacct report
        t += self.sacctReport(self.sacctData (['-j', jobid]), False)
 
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%cherrypy.request.base
        return t

    @cherrypy.expose
    def tymor(self,**args):
        if type(self.data) == str: return self.data # error of some sort.

        selfdata_o = self.data
        data_o     = self.jobData
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
        more_data      ={i:selfdata[i] for i in selfdata if len(selfdata[i]) > 3}
        more_data_clean={i:more_data[i][0:3]+ more_data[i][j][0:7] for i in more_data for j in range(3,len(more_data[i])) if more_data[i][j][2]>=0 }
        idle_data1     ={i:selfdata[i][0:3] for i in selfdata if len(selfdata[i])<=3}
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

        def most_common(lst):
            if len(lst)>0:
                return max(set(lst), key=lst.count)

        def atoi(text):
            return int(text) if text.isdigit() else text
        
        def natural_keys(text):
            return [ atoi(c) for c in re.split('(\d+)', text) ]
        
        for n, v in sorted(data_dash.items()):
            nodes =list(v['node_info'])
            nodes.sort(key=natural_keys)
            data_dash[n]['list_nodes']    = nodes
            data_dash[n]['usernames_list']=[v['node_info'][i][3] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['username']      =most_common(v['usernames_list'])
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

        def mean(data):
            """Return the sample arithmetic mean of data."""
            n = len(data)
            if n < 1:
                raise ValueError('mean requires at least one data point')
            return sum(data)/n # in Python 2 use sum(data)/float(n)

        def _ss(data):
            """Return sum of square deviations of sequence data."""
            c = mean(data)
            ss = sum((x-c)**2 for x in data)
            return ss

        def pstdev(data):
            """Calculates the population standard deviation."""
            n = len(data)
            if n < 2:
                raise ValueError('variance requires at least two data points')
            ss = _ss(data)
            pvar = ss/n # the population variance
            return pvar**0.5

        load_mean =mean(list_loads_flat)
        load_sd   =pstdev(list_loads_flat)
        RSS_mean  =mean(list_RSS_flat)
        RSS_sd    =pstdev(list_RSS_flat)
        VMS_mean  =mean(list_VMS_flat)
        VMS_sd    =pstdev(list_VMS_flat)
        load_diff_mean=mean(list_core_load_diff_flat)
        load_diff_sd  =pstdev(list_core_load_diff_flat)

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
        jobdata    = {job:v  for jobid,v in self.jobData.items()}

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
        data_o     = self.jobData
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
        
        more_data={i:selfdata[i] for i in selfdata if len(selfdata[i]) > 3}
        more_data_clean={i:more_data[i][0:3]+ more_data[i][j][0:7] for i in more_data for j in range(3,len(more_data[i])) if more_data[i][j][2]>=0 }
        idle_data1={i:selfdata[i][0:3] for i in selfdata if len(selfdata[i])<=3}
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

        def most_common(lst):
            if len(lst)>0:
                return max(set(lst), key=lst.count)

        def atoi(text):
            return int(text) if text.isdigit() else text

        def natural_keys(text):
            return [ atoi(c) for c in re.split('(\d+)', text) ]

        for n, v in sorted(data_dash.items()):
            nodes=list(v['node_info'])
            nodes.sort(key=natural_keys)
            data_dash[n]['list_nodes'] = nodes
            data_dash[n]['usernames_list']=[v['node_info'][i][3] for i in nodes if len(v['node_info'][i])>=7]
            data_dash[n]['username']=most_common(v['usernames_list'])
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

        def mean(data):
            """Return the sample arithmetic mean of data."""
            n = len(data)
            if n < 1:
                raise ValueError('mean requires at least one data point')
            return sum(data)/n # in Python 2 use sum(data)/float(n)                                                                                                                  

        def _ss(data):
            """Return sum of square deviations of sequence data."""
            c = mean(data)
            ss = sum((x-c)**2 for x in data)
            return ss

        def pstdev(data):
            """Calculates the population standard deviation."""
            n = len(data)
            if n < 2:
                raise ValueError('variance requires at least two data points')
            ss = _ss(data)
            pvar = ss/n # the population variance                                                                                                                                    
            return pvar**0.5
        
        load_mean=mean(list_loads_flat)
        load_sd=pstdev(list_loads_flat)
        RSS_mean=mean(list_RSS_flat)
        RSS_sd=pstdev(list_RSS_flat)
        VMS_mean=mean(list_VMS_flat)
        VMS_sd=pstdev(list_VMS_flat)
        load_diff_mean=mean(list_core_load_diff_flat)
        load_diff_sd=pstdev(list_core_load_diff_flat)

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
        data_o = self.jobData
        selfdata = {k:v for k,v in selfdata_o.items()}
        data = {k:v for k,v in data_o.items()}
        
        more_data={i:selfdata[i][0:3] + selfdata[i][3][0:7] for i in selfdata if len(selfdata[i])>3 }
        less_data={i:selfdata[i][0:3] for i in selfdata if len(selfdata[i])<=3}
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
    def jobGraph(self,jobid,start='', stop=''):
        if type(self.data) == str: return self.data # error of some sort.

        jobid    = int(jobid)
        print("--- jobGraph called %d---"%jobid)
        data     = self.jobData

        nodelist = list(data[jobid][u'cpus_allocated'])
        start    = data[jobid][u'start_time']
        stop     = time.time()
        uid      = data[jobid][u'user_id']
        uname    = pwd.getpwuid(uid).pw_name
        print (str(start) + "-" + str(stop))

        # highcharts                                                                                                                 
        getSMData = scanSMSplitHighcharts.getSMData
        lseries_all_nodes=[]
        mseries_all_nodes=[]
        for node in nodelist:
            
            lseries, mseries = getSMData(SMDir, node, start, stop)
            for d in lseries:
                if d['name'] == uname: break
                else:
                    print ('ooops')
            
            for i in lseries:
                if i['name'] == uname:
                    i['name']=node
                    lseries_all_nodes.append(i)
            for i in mseries:
                if i['name'] == uname:
                    i['name']=node 
                    mseries_all_nodes.append(i)
                    
        t='<tr><td><div id="memchart" style= "min-width:1000px; height: 500px; margin: 0 auto"><script>graphSeries(%s,"memchart", "memory usage from %s to %s", "aggregate vms per node");</script></div></td></tr><hr><tr><td><div id="loadchart" style= "min-width:1000px; height: 500px; margin: 0 auto"><script>graphSeries(%s,"loadchart","aggregate load from %s to %s", "aggregate load per node");</script></div></td></tr>'%(mseries_all_nodes,time.strftime('%y/%m/%d', time.localtime(start)),time.strftime('%y/%m/%d',time.localtime(stop)),lseries_all_nodes,time.strftime('%y/%m/%d', time.localtime(start)),time.strftime('%y/%m/%d', time.localtime(stop)))

        htmltemp = os.path.join(wai, 'jobnodes_smGraphHighcharts.html')
        h = open(htmltemp).read()%{'tablenodegraphs' : t}
        return h

    @cherrypy.expose
    def sunburst(self):
        def most_common(lst):
            if len(lst)>0:
                return max(set(lst), key=lst.count)
        
        def find_element(children_list,name):
            """
            Find element in children list
            if exists or return none
            """
            for i in children_list:
                if i["name"] == name:
                    return i
            #If not found return None
            return None

        def add_node(path,value,nest, level=0):
            """
            The path is a list.  Each element is a name that corresponds 
            to a level in the final nested dictionary.  
            """

            #Get first name from path
            this_name = path.pop(0)

            #Does the element exist already?
            element = find_element(nest["children"], this_name)

            #If the element exists, we can use it, otherwise we need to create a new one
            if element:

                if len(path)>0:
                    add_node(path,value, element, level+1)
                    #Else it does not exist so create it and return its children
            else:

                if len(path) == 0:
                    #TODO: Hack, Replace when we've redesigned the data representation.
                    url = ''
                    if level == 3:
                        url = 'nodeDetails?node=' + this_name
                        
                    nest["children"].append({"name": this_name, "value": value, 'url': url})
                else:
                    #TODO: Hack, Replace when we've redesigned the data representation.
                    url = ''
                    if level == 2:
                        url = 'jobDetails?jobid=' + str(this_name)
                    elif level == 1:
                        url = 'userDetails?user=' + this_name
                    #Add new element
                    nest["children"].append({"name": this_name, 'url': url, "children":[]})

                    #Get added element 
                    element = nest["children"][-1]

                    #Still elements of path left so recurse
                    add_node(path,value, element, level+1)

        print("--- sunburst called ---")
        if type(self.data) == str: return self.data # error of some sort.

        selfdata_o = self.data
        data_o     = self.jobData
        selfdata   = {k:v for k,v in selfdata_o.items()}
        data       = {k:v for k,v in data_o.items()}
        more_data  = {i:selfdata[i][0:3] + selfdata[i][3][0:7] for i in selfdata if len(selfdata[i])>3 }
        less_data  = {i:selfdata[i][0:3] for i in selfdata if len(selfdata[i])<=3}

        selfdata_dict=dict(more_data,**less_data)
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in data.items():
            x = [node for node, coreCount in jinfo.get(u'cpus_allocated').items()]
            d1 = {k: selfdata_dict[k] for k in x}
            data[jid]["node_info"] = d1
        
        keys_id=(u'job_id',u'user_id',u'qos', u'nodes','node_info',u'num_nodes', u'num_cpus',u'run_time',u'run_time_str',u'start_time')
        data_dash={i:{k:data[i][k] for k in keys_id} for i in data.keys()}
        
        for n, v in sorted(data_dash.items()):
            data_dash[n]['uid'] = data[n][u'user_id']
            data_dash[n]['username'] = pwd.getpwuid(data_dash[n]['uid']).pw_name
            data_dash[n]['cpu_list']=[v['node_info'][i][5] for i in v['node_info'].keys() if len(v['node_info'][i])>=7]
            if not data_dash[n]['cpu_list']:
                print ('Pruning:', repr(v), file=sys.stderr)
                data_dash.pop(n)
                continue
            nodes = list(v['node_info'])
            data_dash[n]['list_nodes'] = nodes
            data_dash[n]['list_state']=[v['node_info'][i][0] for i in nodes]
            data_dash[n]['list_cores']=[round(v['node_info'][i][5],3) if len(v['node_info'][i])>=7 else -1 for i in nodes]
            data_dash[n]['list_load']=[round(v['node_info'][i][7],3)  if len(v['node_info'][i])>=7 else 0.0 for i in nodes]
            data_dash[n]['list_RSS']=[v['node_info'][i][8] if len(v['node_info'][i])>=7 else 0 for i in nodes]
            data_dash[n]['list_VMS']=[v['node_info'][i][9] if len(v['node_info'][i])>=7 else 0 for i in nodes]
            data_dash[n]['list_core_load_diff']=[round(v['node_info'][i][5] -v['node_info'][i][7],3) if len(v['node_info'][i])>=7 else 0.0 for i in nodes]
        
        #need to filter data_dash so that it no longer contains users that are "None"->this was creating sunburst errors 
        
        open('/tmp/sunburst.tmp', 'w').write(repr(data_dash))
        list_node      =[data_dash[i]['list_nodes'] for i in data_dash]
        list_nodes_flat=[item for sublist in list_node for item in sublist]
        num_nodes      =[data_dash[i][u'num_nodes'] for i in data_dash]
        list_load      =[data_dash[i]['list_load'] for i in data_dash]
        list_loads_flat=[item for sublist in list_load for item in sublist]

        list_part      =[data_dash[i][u'qos'] for i in data_dash]
        list_part_flat =[[list_part[i]]*num_nodes[i] for i in range(len(list_part))] #will not change item
        list_part_flatn=[item for sublist in list_part_flat for item in sublist]

        #jobs=data_dash.keys()
        jobs           =list(data_dash)
        list_job_flat  =[[jobs[i]]*num_nodes[i] for i in range(len(num_nodes))]
        list_job_flatn =[item for sublist in list_job_flat for item in sublist]
        list_usernames =[data_dash[i]['username'] for i in data_dash]
        list_usernames_flat =[[list_usernames[i]]*num_nodes[i] for i in range(len(num_nodes))]
        list_usernames_flatn=[item for sublist in list_usernames_flat for item in sublist]
        list_cpus=[data_dash[i]['cpu_list'] for i in data_dash]
        list_cpus_flat =[item for sublist in list_cpus for item in sublist]
        list_RSS       =[data_dash[i]['list_RSS'] for i in data_dash]
        list_RSS_flat  =[item for sublist in list_RSS for item in sublist]
        list_VMS       =[data_dash[i]['list_VMS'] for i in data_dash]
        list_VMS_flat  =[item for sublist in list_VMS for item in sublist]
        print(repr((list_part_flatn, list_usernames_flatn, list_job_flatn, list_nodes_flat, list_loads_flat)))
        listn=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_loads_flat[i]] for i in range(len(list_nodes_flat))]
        listrss=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_RSS_flat[i]] for i in range(len(list_nodes_flat))]
        listvms=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_VMS_flat[i]] for i in range(len(list_nodes_flat))]
        
        listns      =[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i]] for i in range(len(list_nodes_flat))]
        #node_states =[[j.encode("utf-8"),selfdata[j][0]] for j in selfdata.keys()]
        node_states =[[j,selfdata[j][0]] for j in selfdata.keys()]
        data_df     =pd.DataFrame(listns,columns=['partition','user','job','node'])
        state       =pd.DataFrame(node_states,columns=['node','state'])
        statedata_df=pd.merge(state, data_df, on='node',how='left')
        statedata_df['partition'].fillna('Not_Allocated', inplace=True)
        statedata_df['user'].fillna('Not_Allocated', inplace=True)
        statedata_df['job'].fillna('Not_Allocated', inplace=True)
        statedata_df['load']=28
        order=['partition','user','job','state','node','load']
        statedata_df=statedata_df[order]
        
        data_dfload =pd.DataFrame(listn,   columns=['partition','user','job','node','load'])
        data_dfrss  =pd.DataFrame(listrss, columns=['partition','user','job','node','rss'])
        data_dfvms  =pd.DataFrame(listvms, columns=['partition','user','job','node','vms'])
        
        d_rss  = {"name": "root","sysname":"RSS","children": []}
        levels = ["partition","user","job","node"]
        for row in data_dfrss.iterrows():
            r = row[1]
            path = list(r[levels])
            value = r["rss"]
            add_node(path,value,d_rss)
        json_rss=json.dumps(d_rss, sort_keys=False, indent=2)

        d_vms  = {"name": "root","sysname":"VMS","children": []}
        levels = ["partition","user","job","node"]
        for row in data_dfvms.iterrows():
            r = row[1]
            path = list(r[levels])
            value = r["vms"]
            add_node(path,value,d_vms)
        json_vms=json.dumps(d_vms, sort_keys=False,indent=2)

        d_state = {"name": "root", "sysname": "load","children": []}
        levels  = ["partition","user","job","state","node"]
        for row in statedata_df.iterrows():
            r = row[1]
            path = list(r[levels])
            value = r["load"]
            add_node(path,value,d_state)
        json_state=json.dumps(d_state, sort_keys=False,indent=2)

        d_load = {"name": "root", "sysname": "load","children": []}

        levels = ["partition","user","job","node"]
        for row in data_dfload.iterrows():
            r = row[1]
            path = list(r[levels])
            value = r["load"]
            add_node(path,value,d_load)
        json_load=json.dumps(d_load, sort_keys=False,indent=2)
        
        htmltemp = os.path.join(wai, 'sunburst2.html')
        h = open(htmltemp).read()%{'data1' : json_load, 'data2' : json_state, 'data3' : json_vms, 'data4' : json_rss}
        return h


    sunburst.exposed = True
            
cherrypy.config.update({'server.socket_host': '0.0.0.0', 'server.socket_port': WebPort})
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
