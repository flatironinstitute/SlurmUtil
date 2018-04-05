
import cherrypy as CH
import _pickle as cPickle, datetime as DT, os, pwd, pyslurm as SL, subprocess as SUB, sys, time, zlib
from collections import defaultdict as DD

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

class SLURMMonitor(object):

    def __init__(self):
        self.data = 'No data received.'
        self.jobData = {}
        self.dataTime = 'BOOM'

    def index(self, **args):
        # q&d check of jobData
        node2jobs = DD(list)
        for jid, jinfo in self.jobData.items():
            for node, coreCount in jinfo.get(u'cpus_allocated', {}).items():
                node2jobs[node].append(jid)

        #print >>sys.stderr, repr(node2jobs)
        d = self.data
        if type(d) == str: return d # error of some sort.
        t = htmlPreamble + '''\
<h4 style="font-family:Trebuchet MS, Arial, Helvetica, sans-serif">Status as of %s</h4>
<table class="slurminfo">
<tr><th>Node</th><th>Status</th><th>Jobs</th><th>Delay</th><th>User</th><th>Cores Allocated</th><th>Processes</th><th>Aggregate Load</th><th>RSS</th><th>VMS</th></tr>
''' % self.dataTime

        rclass, nrclass = '', ' class="alt"'
        for n, v in sorted(d.items()):
            state, skew, cskew = v[0], -9.99, ' class="right"'
            if len(v) >= 2: skew = v[1]
            if skew > 3: cskew = ' class="right alarm"'
            rh = '<tr%s><td><a href="%s/nodeDetails?node=%s">%s</a></td><td>%s</td><td>%s</td><td%s>%.2f</td>'%(rclass, CH.request.base, n, n, state, ' '.join(['%d'%j for j in node2jobs.get(n, [])]), cskew, skew)
            rclass, nrclass = nrclass, rclass
            if len(v) > 3:
                for uname, uid, c, p, l, rss, vms, pp in sorted(v[3:]):
                    cuname = loadSwitch(c, l, '', '', ' class="alarm"')
                    cload = loadSwitch(c, l, ' class="right inform"', ' class="right"', ' class="right"')
                    t += rh + '<td%s><a href="%s/userDetails?user=%s">%s</a></td><td class="right">%d</td><td class="right">%d</td><td%s>%.2f</td><td class="right">%.2e</td><td class="right">%.2e</td></tr>\n'%(cuname, CH.request.base, uname, uname, c, p, cload, l, rss, vms)
                    rh = '<tr%s><td></td><td></td><td></td><td></td>'%nrclass
            else:
                t += rh + '<td></td><td></td><td></td><td></td><td></td><td></td></tr>\n'
        t += '</tbody>\n</table>\n<a href="%s/index?refresh=1">&#8635</a>\n</body>\n</html>\n'%CH.request.base
        return t
    index.exposed = True

    def updateSlurmData(self, **args):
        d =  CH.request.body.read()
        ts, self.jobData, self.data = cPickle.loads(zlib.decompress(d))
        self.dataTime = time.asctime(time.localtime(ts))
        print ('Got new data', self.dataTime, len(d), d, file=sys.stderr)
    updateSlurmData.exposed = True

    def details(self, criteria):
        t = DT.date.today() + DT.timedelta(days=-SacctWindow)
        startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
        cmd = ['sacct', '-n', '-P', '-S', startDate, '-o', 'JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'] + criteria
        try:
            #TODO: capture standard error separately?
            d = SUB.check_output(cmd, stderr=SUB.STDOUT)
        except SUB.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))
        t = '''\
<table class="slurminfo">
<tr><th>Job ID</th><th>Job Name</th><th>Allocated CPUS</th><th>State</th><th>Exit Code</th><th>User</th><th>Node List</th><th>Start</th><th>End</th></tr>
'''
        rclass, nrclass = '', ' class="alt"'
        jid2info = DD(list)
        for l in d.split('\n'):
            if not l: continue
            ff = l.split('|')
            if '.' in ff[0]: continue # indicates a job step --- under what circumstances should these be broken out?
            f0p = ff[0].split('_')
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
                t += '<tr%s><td>'%rclass + '</td><td>'.join(ff) + '</tr>\n'
                rclass, nrclass = nrclass, rclass
        t += '\n</tbody>\n</table>\n'
        return t

    def usageGraph(self, yyyymmdd='', fs='home', anon=False):
        if not yyyymmdd:
            # only have census date up to yesterday, so we use that as the default.
            yyyymmdd = (DT.date.today() + DT.timedelta(days=-1)).strftime('%Y%m%d')
        label, usageData = fs2hc.gendata(yyyymmdd, fs, anon)
        htmlTemp = 'fileCensus.html'

        h = open(htmlTemp).read()%{'yyyymmdd': yyyymmdd, 'label': label, 'data': usageData}

        return h

    usageGraph.exposed = True

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

    nodeGraph.exposed = True

    def nodeDetails(self, node):
        t = htmlPreamble
        d = self.data.get(node, [])
        try:    status = d[0]
        except: status = 'Unknown'
        try:    skew = d[1]
        except: skew = -9.99
        t += '<h3>Node: %s (<a href="%s/nodeGraph?node=%s">Graph</a>), Status: %s, Delay: %.2f, <a href="#sacctreport">(jump to sacct)</a></h3>\n'%(node, CH.request.base, node, status, skew)
        for user, uid, cores, procs, tc, trm, tvm, cmds in sorted(d[3:]):
            ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
            t += '<hr><em%s>%s</em> %d<pre>'%(ac, user, cores) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'
        t += '<h4 id="sacctreport">sacct report (last %d days for node %s):</h4>\n'%(SacctWindow, node)
        t += self.details(['-N', node])
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%CH.request.base
        return t

    nodeDetails.exposed = True

    def userDetails(self, user):
        t = htmlPreamble
        t += '<h3>User: %s <a href="#sacctreport">(jump to sacct)</a></h3>\n'%user
        for node, d in sorted(self.data.items()):
            if len(d) < 3: continue
            for nuser, uid, cores, procs, tc, trm, tvm, cmds in sorted(d[3:]):
                if nuser != user: continue
                ac = loadSwitch(cores, tc, ' class="inform"', '', ' class="alarm"')
                t += '<hr><em%s>%s</em> %d<pre>'%(ac, node, cores) + '\n'.join([' '.join(['%6d'%cmd[0], '%6.2f'%cmd[1], '%10.2e'%cmd[5], '%10.2e'%cmd[6]] + cmd[7]) for cmd in cmds]) + '\n</pre>\n'
        t += '<h4 id="sacctreport">sacct report (last %d days for user %s):</h4>\n'%(SacctWindow, user)
        t += self.details(['-u', user])
        t += '<a href="%s/index">&#8617</a>\n</body>\n</html>\n'%CH.request.base
        return t

    userDetails.exposed = True

CH.config.update({'server.socket_host': '0.0.0.0', 'server.socket_port': WebPort})

#wai = os.path.abspath(os.getcwd())

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

CH.quickstart(SLURMMonitor(), '/', conf)
