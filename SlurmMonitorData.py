import cherrypy, copy, _pickle as cPickle, datetime, json, os
import logging, re, sys, time, zlib
from collections import defaultdict
from functools import reduce

import pandas

from EmailSender     import JobNoticeSender
from querySlurm      import PyslurmQuery, SlurmCmdQuery

import config
import MyTool
import inMemCache

# Directory where processed monitoring data lives.
USER_INFO_IDX      = 3  # incoming data and self.data {node: [status, delta, ts, procsByUser, ...]...}
USER_PROC_IDX      = 7  # in procsByUser [uname, uid, user_alloc_cores, proc_cnt, totCPURate, totRSS, totVMS, procs, totIO, totCPU]
PROC_JID_IDX       = 9  # in procs [[pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, intervalIOByteAvg, jid],...]
ONE_HOUR_SECS      = 3600
ONE_DAY_SECS       = 86400
logger             = config.logger
DELAY_SECS         = 60

def logTest (msg, pre_ts):
        with open("tmp.log", "a") as f:
             f.write("Took {}:{}\n".format(time.time()-pre_ts, msg))
        return time.time()

@cherrypy.expose
class SLURMMonitorData(object):
    def __init__(self):
        self.updateTS          = time.time()
        self.data              = {}                   #'No data received yet. Please wait a minute and come back.'
        self.currJobs          = {}                   #re-created in updateMonData
        self.node2jids         = {}                   #{node:[jid...]} {node:{jid: {'proc_cnt','cpu_util','rss','iobps'}}}
        self.uid2jid           = {}                   #
        self.pyslurmJobs       = {}
        self.pyslurmNodes      = {}
        self.jobNode2ProcRecord= defaultdict(lambda: defaultdict(lambda: (0, defaultdict(lambda: defaultdict(int))))) # jid: node: (ts, {pid: cpu_time})
                                                      # one 'ts' kept for each pid
                                                      # modified through updateJobNode2ProcRecord only
        self.inMemCache        = inMemCache.InMemCache()
        self.inMemLog          = inMemCache.InMemLog()

    def hasData (self):
        return self.data!={}

    def getNode2Jobs (self, node):
        return [self.currJobs[jid] for jid in self.node2jids.get(node, []) if jid in self.currJobs]

    # add proesses info of jid, modify self.jobNode2ProcRecord
    # TODO: it is in fact userNodeHistory
    def updateJobNode2ProcRecord (self, ts, jobid, node, job_procs, currJobs):
        ts              = int(ts)
        savTs, savProcs = self.jobNode2ProcRecord[jobid][node]
        if ( ts > savTs ):  #only update when the message is newer
           if job_procs:
             for p in job_procs:   # pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, intervalIOByteAvg, jid
               # 09/09/2019 add jid
               pid = p[0]
               assert (savProcs[pid]['cpu_time'] <= p[3] + p[4])        #increasing
               
               savProcs[pid]['cpu_time']     = p[3] + p[4]
               savProcs[pid]['mem_rss_K']    = int(p[5]/1024)
               savProcs[pid]['io_bps_curr']  = p[8]                    #bytes per sec
               savProcs[pid]['cpu_util_curr']= p[1]                    #bytes per sec
               savProcs[pid]['jid']          = p[PROC_JID_IDX]
           else:
             logger.debug("{}: no proc for job {} on node {}".format(ts, jobid, node))
             savProcs = defaultdict(lambda: defaultdict(int))
           self.jobNode2ProcRecord[jobid][node] = (ts, savProcs)  # modify self.jobNode2ProcRecord
           #remove done job
           done_job = [jid for jid, jinfo in currJobs.items() if jinfo['job_state'] not in ['RUNNING', 'PENDING', 'PREEMPTED']]
           for jid in done_job:
               self.jobNode2ProcRecord.pop(jid, {})

    @cherrypy.expose
    def getNodeData(self):
        return repr(self.data)

    @cherrypy.expose
    def getJobData(self):
        return repr(self.currJobs)

    @cherrypy.expose
    def getAllJobData(self):
        return '({},{})'.format(self.updateTS, self.pyslurmJobs)

    @cherrypy.expose
    def getRawNodeData(self):
        return repr(self.pyslurmNodes)

    @cherrypy.expose
    def getNode2Jobs1 (self):
        return "{}".format(self.node2jids)

    def getUserJobStartTimes(self, uid):
        uid   = int(uid)
        stime = []
        for jid, jinfo in self.currJobs.items():
            if ( jinfo.get('user_id',-1) == uid ): stime.append(jinfo.get('start_time', 0))
        return stime

    def createNode2Jids (jobData):
        node2jobs = defaultdict(list)  #nodename: joblist
        for jid, jinfo in jobData.items():
            for nodename in jinfo.get('cpus_allocated', {}).keys():
                node2jobs[nodename].append(jid)
        return node2jobs

    #TODO: for the case that 1 user - n job
    def uid2jids  (self, uid, node):
        jids = list(filter(lambda x: self.currJobs[x]['user_id']==uid, self.node2jids[node]))
        if len(jids) == 0:
           logger.warning ('uid2jid user {} has no slurm jobs on node {} (jobs {}). Ignore proc data.'.format(uid, node, self.node2jids[node]))
        elif len(jids) > 1:
           logger.warning('uid2jid user {} has multiple slurm jobs {} on node {}'.format(uid, jids, node))

        return jids

    def getJobUsageOnNode (self, jid, job, node):
        job_uid = job['user_id']
        if len(node) > USER_INFO_IDX:
           #09/09/2019 add jid
           #calculate each job's
           user_proc = [userInfo for userInfo in node[USER_INFO_IDX:] if userInfo[1]==job_uid ]
           if len(user_proc) != 1:
              logger.error("User {} has {} record on {}. Ignore".format(job_uid, len(user_proc), node))
              return None, None, None, None, None, None

           job_proc = [proc for proc in user_proc[0][7] if proc[PROC_JID_IDX]==jid]
           # summary data in job_proc
           # [pid, CPURate, 'create_time', 'user_time', 'system_time', 'rss', vms, cmdline, IOBps, jid]
           cpuUtil  = sum([proc[1]         for proc in job_proc])
           rss      = sum([proc[5]         for proc in job_proc])
           vms      = sum([proc[6]         for proc in job_proc])
           iobps    = sum([proc[8]         for proc in job_proc])
           fds      = sum([proc[10]        for proc in job_proc])
           return cpuUtil, rss, vms, iobps, len(job_proc), fds
        else:
           return 0, 0, 0, 0, 0, 0

    def getJobGPUUtil_node (self, job, nodename, gpudata):
        if not gpudata:
           return 0
        #gpudata[gpuname][nodename]
        if nodename in job['gpus_allocated']:
           return sum([gpudata['gpu{}'.format(idx)][nodename] for idx in job['gpus_allocated'][nodename]])
        else:
           return 0

    @cherrypy.expose
    def getNodeUtil (self, **args):
        return self.getNodeUtilData (self.pyslurmNodes, self.data)

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

    #data1=self.pyslurmNodes, data2=self.data
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

    # return list of job label on hostname
    def getNodeJobGPULabelList (self, hostname, alloc_jobs):
       job_gpus  = []
       for jid in alloc_jobs:
           gpus_alloc = self.currJobs[jid]['gpus_allocated'].get(hostname,[])
           if gpus_alloc:
              job_gpus.append (['gpu{}'.format(idx) for idx in gpus_alloc])
           else:
              job_gpus.append ('')
       return job_gpus

    # return #{'gpu0':{'label':,'state':}}
    def getNodeGPULabel (self, gpudata, node_name, node_state, state_str, node_gpus, alloc_jobs):
        if not node_gpus:
           return {}
        gpus = {}
        gpu2jid=dict([(gpu_idx, jid) for jid in alloc_jobs for gpu_idx in self.currJobs[jid]['gpus_allocated'].get(node_name,[])]) #TODO: assume gpu is not shared, gpus_allocated': {'workergpu14': [(0, 2)]
        for i in range(0, node_gpus):
            gpu_name  = 'gpu{}'.format(i)
            jid       = 0
            if (node_state==1) and (i not in gpu2jid):  # node is in use, but gpu is not in use
               gpu_state, state_str = 0, "IDLE"
            else:
               gpu_state            = node_state
            if gpu_state==1:          # gpu in use
               jid       = gpu2jid[i]
               jobInfo   = self.currJobs[jid]
               gpu_alloc = [ 'gpu{}'.format(gpu_idx) for gpu_idx in jobInfo['gpus_allocated'].get(node_name,[])]
               if node_name not in gpudata[gpu_name]:
                  logger.error("gpudata not including {}:{}".format(node_name, gpu_name))
               if node_name not in jobInfo['cpus_allocated']:
                  logger.error("jobInfo not including {} -{}".format(node_name, jobInfo['cpus_allocated']))
               # sometimes, queryBright cannot get some gpu's data, in that case, use 0
               gpu_label = '{}_{}: gpu_util={:.1%}, job=({},{},{} cpu, {})'.format(node_name, gpu_name, gpudata[gpu_name].get(node_name,0), jid, MyTool.getUser(jobInfo['user_id']), jobInfo['cpus_allocated'][node_name], gpu_alloc)
            else:
               gpu_label = '{}_{}: state={}'.format(node_name, gpu_name, state_str)
            gpus[gpu_name] = {'label':gpu_label, 'state': gpu_state, 'job': jid}
        return gpus

    def getHeatmapWorkerData (self, gpudata, weight, avg_minute=0):
        node2job= self.node2jids
        workers = []  #dataset1 in heatmap
        for hostname, hostinfo in sorted(self.data.items()):
            #try:
               pyslurmNodes  = self.pyslurmNodes[hostname]
               node_mem_M   = pyslurmNodes['real_memory']
               alloc_jobs   = node2job.get(hostname, [])
               if avg_minute==0:
                  if len(hostinfo) > USER_INFO_IDX: #has user proc information
                     node_cpu_util  = sum ([hostinfo[idx][4] for idx in range(USER_INFO_IDX, len(hostinfo))])
                  else:
                     node_cpu_util  = 0
               else:
                  node_cpu_util = self.inMemCache.queryNodeAvg(hostname, avg_minute)
               node_mem_util= (node_mem_M-pyslurmNodes['free_mem']) / node_mem_M  if pyslurmNodes['free_mem'] else 0 #ATTN: from slurm, not monitor, if no free_mem, in general, node is DOWN so return 0. TODO: Not saving memory information in cache. The sum of proc's RSS does not reflect the real value.
               node_record  = self.getNodeLabelRecord(hostname, hostinfo, alloc_jobs, node_cpu_util, node_mem_util, gpudata)
               node_record['comb_util'] = (weight['cpu']*node_record['util'] + weight['mem']*node_record['mem_util'])/(weight['cpu'] + weight['mem'])
               workers.append(node_record)
              #{'name':hostname, 'stat':state, 'core':node_cores, 'util':node_cpu_util/node_cores, 'mem_util':node_mem_util, 'jobs':alloc_jobs, 'acct':job_accounts, 'labl':nodeLabel, 'gpus':gpuLabel, 'gpuCount':node_gpus})

            #except Exception as exp:
            #   print("ERROR getHeatmapData: {0}".format(exp))

        return workers

    def nodeAllocated (hostinfo):
        return 'ALLOCATED' in hostinfo[0] or 'MIXED' in hostinfo[0]

    def getNodeLabelRecord (self, hostname, hostinfo, alloc_jobs, node_cpu_util, node_mem_util, gpudata):
        pyslurmNode  = self.pyslurmNodes[hostname]
        node_cores   = pyslurmNode['cpus']
        node_gpus    = MyTool.getNodeGresGPUCount     (pyslurmNode['gres'])
        node_mem_M   = pyslurmNode['real_memory']
        job_accounts = [self.currJobs[jid].get('account', None)        for jid in alloc_jobs]
        job_cores    = [self.currJobs[jid]['cpus_allocated'][hostname] for jid in alloc_jobs]  #
        gpus         = {}   #{'gpu0':{'label':,'state':}}
        if SLURMMonitorData.nodeAllocated(hostinfo):                  #node is in use
           state        = 1
           job_users    = [self.currJobs[jid].get('user',    None)        for jid in alloc_jobs]
           # get node label
           if not node_gpus:  #no GPU node
              lst       = list(zip(alloc_jobs, job_users, ['{} cpu'.format(jc) for jc in job_cores]))
              nodeLabel = '{} ({} cpu, {}GB): cpu_util={:.1%}, mem_util={:.1%}, jobs={}'.format(hostname, node_cores, int(node_mem_M/1024), node_cpu_util/node_cores, node_mem_util, lst)
           else:              # GPU node
              job_gpus  = self.getNodeJobGPULabelList (hostname, alloc_jobs)
              lst       = list(zip(alloc_jobs, job_users, ['{} cpu'.format(jc) for jc in job_cores], job_gpus))
              nodeLabel = '{} ({} cpu, {} gpu, {}GB): cpu_util={:.1%}, used_gpu={}, mem_util={:.1%}, jobs={}'.format(hostname, node_cores, node_gpus, int(node_mem_M/1024), node_cpu_util/node_cores, MyTool.getNodeGresUsedGPUCount (pyslurmNode['gres_used']), node_mem_util, lst)
              gpus = self.getNodeGPULabel (gpudata, hostname, state, hostinfo[0], node_gpus, alloc_jobs)
        else:      # node not in use
           state        = 0 if 'IDLE' in hostinfo[0] else -1
           if not node_gpus:
              nodeLabel = '{} ({} cpu, {}GB): state={}'.format        (hostname, node_cores, int(node_mem_M/1024), hostinfo[0])
           else:
              nodeLabel = '{} ({} cpu, {} gpu, {}GB): state={}'.format(hostname, node_cores, node_gpus, int(node_mem_M/1024), hostinfo[0])
              gpus = self.getNodeGPULabel (gpudata, hostname, state, hostinfo[0], node_gpus, [])
        rlt = {'name':hostname, 'stat':state, 'core':node_cores, 'util':node_cpu_util/node_cores, 'mem_util':node_mem_util, 'jobs':alloc_jobs, 'acct':job_accounts, 'labl':nodeLabel, 'gpus':gpus, 'gpuCount':node_gpus}
        #TODO: add comb_util

        #return nodeLabel, gpus, state
        return rlt

    def getCurrJobGPUNodes (self):
        return PyslurmQuery.getJobGPUNodes(self.currJobs, self.pyslurmNodes)

    def getCurrJobGPUDetail (self):
        return self.getJobGPUDetail(self.currJobs)
    # jobs is self.currJobs
    def getJobGPUDetail (self, jobs):
        rlt       = defaultdict(lambda: defaultdict())   # {'workergpu00':{'gpu0':job,...}
        min_start = int(time.time())                          # earliest start time of jobs
        for job in jobs.values():
            if job['gpus_allocated']:
               for gpuNode, gpuList in job['gpus_allocated'].items():
                   for gpuIdx in gpuList:
                       gpu = 'gpu{}'.format(gpuIdx)
                       rlt[gpuNode][gpu] = job
                       if job['start_time'] < min_start:  min_start = job['start_time']
        return min_start, rlt

    @cherrypy.expose
    def getJobNode2ProcRecord (self, jid):
        return "{}".format(self.jobNode2ProcRecord[int(jid)])

    # add attributes to jobs
    def addJobsAttr (self, ts, jobs, pyslurmNodes, low_util=0.01, long_period=ONE_DAY_SECS, job_width=1, low_mem=0.3):
        #check self.currJobs and locate those jobs in question
        #TODO: 09/09/2019: add jid
        for jid, job in jobs.items():
            period                   = ts - job['start_time']
            total_cpu_time           = 0
            total_rss                = 0
            total_node_mem           = 0           #proportional mem for shared nodes
            total_io_bps             = 0
            total_cpu_util_curr      = 0
            job['node_cpu_util_avg'] = {}
            job['node_rss_util']     = {}
            job['node_io_bps_curr']  = {}
            job['node_cpu_util_curr']= {}
            job['num_proc']          = {}
            job['user']              = MyTool.getUser(job['user_id'])

            for node in job['cpus_allocated']:
                node_cpu_time, node_rss, node_mem, node_io_bps_curr, node_cpu_util_curr = 0,0,0,0,0
                #check self.jobNode2ProcRecord to add up cpu_time and get utilization
                if (jid in self.jobNode2ProcRecord) and (node in self.jobNode2ProcRecord[jid]):
                   savTs, procs        = self.jobNode2ProcRecord[jid][node]
                   node_cpu_time       = sum([ts_proc['cpu_time']      for pid,ts_proc in procs.items()])
                   node_rss            = sum([ts_proc['mem_rss_K']     for pid,ts_proc in procs.items()])
                   node_io_bps_curr    = sum([ts_proc['io_bps_curr']   for pid,ts_proc in procs.items()])
                   node_cpu_util_curr  = sum([ts_proc['cpu_util_curr'] for pid,ts_proc in procs.items()])
                   total_cpu_time     += node_cpu_time
                   total_rss          += node_rss
                   total_io_bps       += node_io_bps_curr
                   total_cpu_util_curr+= node_cpu_util_curr
                   job['node_cpu_util_avg'][node] = node_cpu_time / period / job['cpus_allocated'][node]
                   job['node_rss_util'][node]     = node_rss
                   job['node_io_bps_curr'][node]  = node_io_bps_curr
                   job['node_cpu_util_curr'][node]= node_cpu_util_curr
                   job['num_proc'][node]          = len(procs)

                   node_tres           = MyTool.getTresDict(pyslurmNodes[node]['tres_fmt_str'])
                   if 'mem' in node_tres:   # memory is shared
                      prop             = job['cpus_allocated'][node] / node_tres['cpu'] if 'cpu' in node_tres else 1
                      total_node_mem  += MyTool.convert2K(node_tres['mem']) * prop
                      #total_node_mem  += MyTool.convert2K(node_tres['mem'])
                   else:
                      logger.error('ERROR: Node {} does not have mem {} in tres_fmt_str {}'.format(node, d, s))
                elif ts - job['start_time'] > DELAY_SECS*3 : #allow 60*3 seconds delay
                   if jid not in self.jobNode2ProcRecord:
                      logger.error('ERROR: Job {} (start at {} on {}) is not in self.jobNode2ProcRecord'.format(jid, job['start_time'], job['nodes']))
                   else:
                      logger.error('ERROR: Node {} of Job {} (start at {} on {}) is not in self.jobNode2ProcRecord'.format(node, jid, job['start_time'], job['nodes']))

            job['job_io_bps']   = total_io_bps
            job['job_inst_util']= total_cpu_util_curr
            if total_cpu_time: # has process informatoin
                job['job_avg_util'] = total_cpu_time / period / job['num_cpus']
                job['job_mem_util'] = total_rss / total_node_mem
            else: # no process information
                #print('WARNING: Job {} does not have proc on nodes {}'.format(jid, job['nodes']))
                job['job_avg_util'] = 0
                job['job_mem_util'] = 0
            job['gpus_allocated'] = SLURMMonitorData.getJobAllocGPU(job, pyslurmNodes)

        return jobs

    def getJobAllocGPU (job, node_dict):
        node_list      = [node_dict[node] for node in job['cpus_allocated']]
        gpus_allocated = MyTool.getGPUAlloc_layout(node_list, job['gres_detail'])
        return gpus_allocated

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

    def getCurrLUJobs (self, long_period, job_width, low_mem, exclude_acct=['scc']):
        return self.getLUJobs(self.updateTS, self.currJobs, long_period, job_width, low_mem, exclude_acct)

    def getCurrEmptyAllocNode (self):
        return [nm for nm, ninfo in self.data.items() if SLURMMonitorData.nodeAllocated(ninfo) and (len(ninfo) <= USER_INFO_IDX or      not ninfo[USER_INFO_IDX])]

    def getLowUtilNodes (self):
        # node with low resource utlization
        empty_nodes= self.getCurrEmptyAllocNode()
        low_nodes  = []
        for nm in empty_nodes:
            jobs     = self.getNode2Jobs(nm)
            avg_util = sum([job['job_avg_util'] for job in jobs])
            avg_mem  = sum([job['job_mem_util'] for job in jobs])
            u_set    = set([job['user_id'] for job in jobs])
            u_lst    = [MyTool.getUser(uid) for uid in u_set]
            low_nodes.append({'name':nm, 'msg':'Node is allocated to job {} of user {}. The average cpu utilization is {} and the average memory utiization is {}.'.format([job['job_id'] for job in jobs], u_lst, avg_util, avg_mem)})
        return low_nodes

    def getLUJobs (self, ts, jobs, low_util, long_period, job_width, low_mem, exclude_acct=['scc']):
        #check self.currJobs and locate those jobs in question
        if not jobs:
           jobs   = self.currJobs
        result = {}            # return {jid:job,...}
        for jid, job in jobs.items():
            period = ts - job['start_time']
            if (period > long_period) and (job.get('num_cpus',1)>=job_width) and (job['job_avg_util'] < low_util) and (job['job_mem_util']<low_mem) and (job['job_inst_util'] < low_util) and (job['account'] not in exclude_acct):
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
        logger.debug('getUnbalancedJobs {}'.format(result.keys()))
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
        self.updateTS, self.data, self.pyslurmJobs, self.pyslurmNodes, self.currJobs, self.node2jids = self.extractSlurmData(d)
        self.inMemCache.append(self.data, self.updateTS, self.pyslurmJobs)
        

    def extractSlurmData (self, d):
        updateTS, pyslurmJobs, hn2info, pyslurmNodes = cPickle.loads(zlib.decompress(d))
        #logger.info("hn2info={}".format(hn2info))
        updateTS  = int(updateTS)
        currJobs  = dict([(jid,job) for jid, job in pyslurmJobs.items() if job['job_state'] in ['RUNNING', 'CONFIGURING']])
        node2jids = SLURMMonitorData.createNode2Jids (currJobs)

        nodeData  = {}
        for node,nInfo in hn2info.items():
            if updateTS - int(nInfo[2]) > 600: # Ignore data
               logger.debug("ignore old data of node {} at {}.".format(node, MyTool.getTsString(nInfo[2])))
               continue
            if node not in pyslurmNodes:
               logger.info("ignore no slurm node {}.".format(node))
               continue

            #set the value of self.data
            nodeData[node] = nInfo                      #nInfo: status, delta, ts, procsByUser
            if 'ALLOCATED' in nInfo[0] or 'MIXED' in nInfo[0]: # allocated node 
               jid2proc        = defaultdict(list)          #jid: [proc]
               if len(nInfo) > USER_INFO_IDX and nInfo[USER_INFO_IDX]:
                  for procsByUser in nInfo[USER_INFO_IDX:]: #worker may has multiple users, 
                                                            #[uname, uid, alloc_cores, proc_cnt, totIUA, totRSS, totVMS, procs, totIO, totCPU])
                     if len(procsByUser) > USER_PROC_IDX and procsByUser[USER_PROC_IDX]:
                        #09/09/2019, add jid to proc
                        for proc in procsByUser[USER_PROC_IDX]:
                            jid2proc[proc[PROC_JID_IDX]].append(proc)    #proc[9] is jid
               #else:
               #   logger.info("{}({}), no proc information for allocated node with jobs {}".format(node, nInfo[0], self.node2jids[node]))
               for jid in node2jids[node]:
                  self.updateJobNode2ProcRecord (nInfo[2], jid, node, jid2proc[jid], currJobs)  #nInfo[2] is ts
               #update the latest cpu_time for each proc
            elif len(nInfo) > USER_INFO_IDX and nInfo[USER_INFO_IDX]:  # no-allocated node has proc
               u_lst = [procsByUser[0] for procsByUser in nInfo[USER_INFO_IDX:]]
               logger.warning("{}-{}: User {} has proc running on the node.".format(node, nInfo[0], u_lst))

        self.addJobsAttr      (updateTS, currJobs, pyslurmNodes)          #add attribute job_avg_util, job_mem_util, job_io_bps

        #check for long run low util jobs and send notice
        #low_util = self.getLongrunLowUtilJobs(self.updateTS, self.currJobs)
        #print('low_util={}'.format(low_util.keys()))
        #if (cherrypy.session['settings']['low_util_job']['email'] ):
        #   hour = datetime.datetime.fromtimestamp(self.updateTS).hour
        #   if hour == 8: # only check to send un-duplicate email 8:00am-9:00am
        #      self.jobNoticeSender.sendNotice(self.updateTS, low_util)
        #BulletinBoard
        #self.bulletinBoard.addLowUtilJobNotice (self.updateTS, low_util)
        #TODO: synchorize update to jobNoticeSender and BulletinBoard
        return updateTS, nodeData, pyslurmJobs, pyslurmNodes, currJobs, node2jids

    def getNodeProc (self, node):
        if node not in self.data:
           return None

        #get node data from self.pyslurmNodes
        pyslurmNodes = self.pyslurmNodes[node]  #'name', 'state', 'cpus', 'alloc_cpus',
        newNode      = MyTool.sub_dict(pyslurmNodes, ['name','cpus','alloc_cpus'])
        newNode['gpus'],newNode['alloc_gpus'] = MyTool.getGPUCount(pyslurmNodes['gres'], pyslurmNodes['gres_used'])

        #get data from self.data
        newNode['state']    = self.data[node][0]
        newNode['updateTS'] = self.data[node][2]
        #organize procs by job
        newNode['jobProc']  = defaultdict(lambda: {'job':{}, 'procs':[]})       #jid, {'job': , 'procs': }
        newNode['procCnt']  = 0
        for user in sorted(self.data[node][USER_INFO_IDX:]):
            for proc in user[7]:                          #user, uid, cpuCnt, procCnt, totCPURate, totRSS, totVMS, procs, totIOBps, totCPUTime
                newNode['jobProc'][proc[PROC_JID_IDX]]['procs'].append([proc[i] for i in [0,1,5,6,8,7]])  #[pid(0), CPURate/1, create_time, user_time, system_time, rss/5, 'vms'/6, cmdline/7, IOBps/8, jid, read_bytes, write_bytes]
                newNode['procCnt'] += 1

        if -1 in newNode['jobProc']:  #TODO: deal with -1 jodid
           newNode['jobProc']['undefined'] = newNode['jobProc'][-1]
           del newNode['jobProc'][-1]

        #get data from self.currJobs
        for jid in newNode['jobProc']:
            if jid in self.currJobs:
               newNode['jobProc'][jid]['job'] = dict((k,v) for k, v in self.currJobs[jid].items() if v and v != True)
            else:
               logger.warning("Job {} on node {}({}) is not in self.currJobs={}".format(jid, node, list(newNode['jobProc'].keys()), list(self.currJobs.keys())))
        newNode['jobProc']=dict(newNode['jobProc'])  #convert from defaultdict to dict
        newNode['jobCnt'] =len(newNode['jobProc'])
        newNode['alloc_cpus'] =sum([self.currJobs[jid]['cpus_allocated'][node] for jid in newNode['jobProc'] if jid in self.currJobs])

        jobCPUAlloc = dict([(jid, self.currJobs[jid]['cpus_allocated'][node]) for jid in self.node2jids[node]])  #'cpus_allocated': {'worker1011': 28}

        return newNode

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

    #return dict {node: []}
    #result[node_name]= [alloc_core_cnt, proc_cnt, total_cpu, t_rss, t_vms, rlt_procs, t_io]
    #rlt_procs.append ([pid, intervalCPUtimeAvg, job_avg_cpu, rss, vms, intervalIOByteAvg, cmdline])
    def getJobProc (self, jid):
        if jid not in self.currJobs:
           return None, None
        job_info = self.currJobs[jid]
        job_start= job_info.get('start_time', 0)
        node2job = {}
        nodes    = list(job_info['cpus_allocated'].keys())
        user     = MyTool.getUser(job_info['user_id'])
        for node_name in nodes:
            if node_name not in self.data:
               logger.warning('WARNING: {} is not in self.data'.format(node_name))
               continue
            d  = self.data[node_name]
            ts = d[2]
            if len(d) < USER_INFO_IDX: continue   # no user info
            for user_name, uid, alloc_core_cnt, proc_cnt, t_cpu, t_rss, t_vms, procs, t_io, *etc in d[USER_INFO_IDX:]:
                if user_name == user:
                   #procs[[pid, intervalCPUtimeAvg, create_time, 'user_time', 'system_time, 'rss', 'vms', 'cmdline', intervalIOByteAvg],...]
                   job_procs = []
                   job_cpu   = 0
                   # 09/09/2019 add jid
                   for pid, intervalCPUtimeAvg, create_time, user_time, system_time, rss, vms, cmdline, intervalIOByteAvg, jid, num_fds, *etc in procs:
                       if jid == job_info['job_id']:
                          #proc_avg_cpu = (user_time+system_time) / (ts-job_start) if job_start > 0 else 0  # job_start != proc_start
                          #job_procs.append ([pid, '{:.2f}'.format(intervalCPUtimeAvg), '{:.2f}'.format(proc_avg_cpu), MyTool.getDisplayB(rss), MyTool.getDisplayB(vms), MyTool.getDisplayBps(intervalIOByteAvg), num_fds, ' '.join(cmdline)])
                          job_procs.append ([pid, '{:.2f}'.format(intervalCPUtimeAvg), MyTool.getDisplayB(rss), MyTool.getDisplayB(vms), MyTool.getDisplayBps(intervalIOByteAvg), num_fds, ' '.join(cmdline)])
                          job_cpu     += intervalCPUtimeAvg
                   #TODO: t_rss, t_vms, t_io incorrect, should do similar thing as job_cpu
                   num_procs = len(job_procs) 
                   if num_procs:
                      node2job[node_name]= [int(job_info['cpus_allocated'][node_name]), num_procs, job_cpu/num_procs, t_rss, t_vms, job_procs, t_io]
                   else:
                      node2job[node_name]= [int(job_info['cpus_allocated'][node_name]), 0,         0,                 t_rss, t_vms, job_procs, t_io]
        return ['PID', 'Inst CPU Util', 'RSS', 'VMS', 'IO Rate', 'Num Fds', 'Command'], node2job

    def getUserData(self):
        hostdata   = {host:v for host,v  in self.data.items()}
        jobdata    = {job:v  for jobid,v in self.currJobs.items()}

        hostUser   = [hostdata[h][4][0] for h in list(hostdata)]
        hostStatus = [hostdata[h][0] for h in list(hostdata)]
        hostSCount = {s:hostStatus.count(s) for s in set(hostStatus)}

        jobQos     = [jobdata[j][u'qos']       for j in list(jobdata)]
        jobCpus    = [jobdata[j][u'num_cpus']  for j in list(jobdata)]
        jobNodes   = [jobdata[j][u'num_nodes'] for j in list(jobdata)]

    #req_fld is to make sure the fields are there. default is the nature result
    def getJob (self, jobid, req_fields=[]):
        if self.pyslurmJobs and (jobid in self.pyslurmJobs):
           job = self.pyslurmJobs[jobid]
        else:
           job = PyslurmQuery.getSlurmDBJob (jobid, req_fields=['start_time'])
        return job

    def getSunburstData1 (self):
        more_data    = {k:v[0:USER_INFO_IDX] + v[USER_INFO_IDX][0:7] for k,v in self.data.items() if len(v)>USER_INFO_IDX } #flatten hostdata
        less_data    = {k:v[0:USER_INFO_IDX]                         for k,v in self.data.items() if len(v)<=USER_INFO_IDX }
        hostdata_flat= dict(more_data,**less_data)

        keys_id      =(u'job_id',u'user_id',u'qos', u'num_nodes', u'num_cpus')
        data_dash    ={jid:{k:jinfo[k] for k in keys_id} for jid,jinfo in self.currJobs.items()} #extract set of keys
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in self.currJobs.items():
            data_dash[jid]["node_info"] = {n: hostdata_flat.get(n,[]) for n in jinfo.get(u'cpus_allocated').keys()}

        if len(data_dash) == 0:
            return EMPTYPROCDATA_MSG + '\n\n' + repr(self.data)
        for jid, jinfo in sorted(data_dash.items()):
            username = MyTool.getUser(jinfo[u'user_id'])
            data_dash[jid]['cpu_list'] = [jinfo['node_info'][i][5] for i in jinfo['node_info'].keys() if len(jinfo['node_info'][i])>=7]
            if not data_dash[jid]['cpu_list']:
                #print ('Pruning:', repr(jinfo), file=sys.stderr)
                data_dash.pop(jid)
                continue
        
    def sunburst_node_func (df, val_col):  # return [{'name':worker0000,'value':33}]
        return df[['node',val_col]].rename(columns={'node':'name',val_col:'value'}).to_dict(orient='record')

    def sunburst_job_func(df, val_col):  # return [{'name': 932005, 'children': [{'name': 'worke...
        return df[['job','node',val_col]].groupby(['job']).apply(lambda x: {'name':str(x.name), 'children':SLURMMonitorData.sunburst_node_func(x, val_col)}).to_list()

    def df2nested(df, value_col):
        d = []
        for p, d1 in df.groupby(['partition']):
            children=d1[['user','job','node',value_col]].groupby(['user']).apply(lambda x: {'name':x.name, 'children':SLURMMonitorData.sunburst_job_func(x, value_col)}).to_list()
            d.append({'name':p, 'children':children})
        return d

    # return a df with columns [jid, node, cpu, rss, io]
    def getJobNodeValueDF (self):
        cols = ['node', 'user', 'jid', 'cpu', 'rss', 'io']
        lst  = []
        for node, nodeInfo in self.data.items():
            for userInfo in nodeInfo[USER_INFO_IDX:]:    # multiple users
                for procs in userInfo[PROC_JID_IDX]:     
                    item = [node, jid]

    def getJobNodeProcUtil (self, jid, nodes):
        rlt = []
        cpu_lst, mem_lst, io_lst=[], [], []
        if jid in self.jobNode2ProcRecord:
           for node in nodes:
               ts, procs = self.jobNode2ProcRecord[jid].get(node, (0, []))
               if procs:
                  j_cpu_util  = sum([p['cpu_util_curr'] for p in procs.values()])
                  j_mem_rss_K = sum([p['mem_rss_K']     for p in procs.values()])
                  j_io_bps    = sum([p['io_bps_curr']   for p in procs.values()])
                  cpu_lst.append (j_cpu_util)
                  mem_lst.append (j_mem_rss_K)
                  io_lst.append  (j_io_bps)
               else:     # no record of proces
                  cpu_lst.append (0)
                  mem_lst.append (0)
                  io_lst.append  (0)
           return cpu_lst, mem_lst, io_lst
        else:
           logger.info("Job {} not in self.jobNode2ProcRecord".format(jid))
           lst = [0] * len(nodes)
           return lst, lst, lst 
                 
    def test1(self):  #took 2.1 sec
        #TODO: synchronize the mod/get of core data structure
        ts           = time.time()
        user2acct    = SlurmCmdQuery.getAllUserAssoc()

        jobs_df      = pandas.DataFrame()
        job_keys     = ['user', 'user_id', 'job_id', 'partition', 'qos']
        for jid, jinfo in self.currJobs.items():
            job_nodes     = list(jinfo['cpus_allocated'])
            job_node_cpu  = list(jinfo['cpus_allocated'].values())
            l1,l2,l3      = self.getJobNodeProcUtil (jid, job_nodes)
            #job_df        = pandas.DataFrame(zip(job_nodes, job_node_cpu,l1,l2,l3), columns=['node','num_cpus','cpu_util','mem_rss_K','io_bps'])
            #for key in job_keys:
            #    job_df[key] = jinfo[key]
            #job_df['account'] = user2acct.get(jinfo['user'],{}).get('Def Acct','undefined')
            #jobs_df       = jobs_df.append(job_df)
        ts = logTest("jobs_df={}".format(jobs_df), ts)
        
    def test(self):
        #TODO: synchronize the mod/get of core data structure
        #self.getSunburstData ()
        ts           = time.time()

        user2acct    = SlurmCmdQuery.getAllUserAssoc()

        job_keys     = ['user', 'user_id', 'job_id', 'partition', 'qos']
        jobs_lst     = []           # nested list
        for jid, jinfo in self.currJobs.items():
            job_flds      = [jinfo[key] for key in job_keys]
            acct          = user2acct.get(jinfo['user'],{}).get('Def Acct','undefined')
            l1,l2,l3      = self.getJobNodeProcUtil (jid, list(jinfo['cpus_allocated']))
            idx           = 0
            for node, cpu_count in jinfo['cpus_allocated'].items():
                n_state   = self.data.get(node, ['unknown'])[0]
                if 'ALLOCATED' in n_state:
                   n_state = 'ALLOCATED'
                elif 'MIXED' in n_state:
                   n_state = 'MIXED'
                curr_lst  = job_flds + [acct, node, n_state, cpu_count, l1[idx], l2[idx], l3[idx]]
                jobs_lst.append (curr_lst)
                idx      += 1

        mapping       = dict(zip(['user', 'user_id', 'job', 'partition', 'qos', 'acct', 'node', 'node_state', 'num_cpus','cpu_util','mem_rss_K','io_bps'],range(12)))
        # acct, user, job, node, num_cpus
        idx_lst         = [mapping[key] for key in ['acct', 'user', 'job', 'node']]
        num_cpus_dict   = MyTool.list2nestedDict ("num_cpus",   jobs_lst, idx_lst,  mapping["num_cpus"])
        cpu_util_dict   = MyTool.list2nestedDict ("cpu_util",   jobs_lst, idx_lst,  mapping["cpu_util"])
        rss_K_dict      = MyTool.list2nestedDict ("mem_rss",    jobs_lst, idx_lst,  mapping["mem_rss_K"])
        io_bps_dict     = MyTool.list2nestedDict ("io_bps",     jobs_lst, idx_lst,  mapping["io_bps"])

        # state
        idx_lst1        = [mapping[key] for key in ['node_state', 'acct', 'user', 'job', 'node']]
        for n_name, node in self.pyslurmNodes.items():
            if ('ALLOCATED' not in node['state']) and ('MIXED' not in node['state']):
               n_state = node['state']
               if 'IDLE' in node['state']:
                  n_state = 'IDLE'
               elif ('DOWN' in n_state) or ('MAINT' in n_state) or ('REBOOT' in n_state):
                  n_state = 'DOWN/MAINT...'
               jobs_lst.append([None, 'Not_allocated', None, 'Not_allocated', 'Not_allocated', 'Not_allocated', n_name, n_state, node['cpus'], 0, 0, 0])
        state_ncpu_dict = MyTool.list2nestedDict ("state",      jobs_lst, idx_lst1, mapping["num_cpus"])
        ts = logTest("Done", ts)

        # acct, user, job, node, alloc_mem
        # acct, user, job, node, cpu_util
        # acct, user, job, node, mem_rss_K
        # acct, user, job, node, io_bps
        return num_cpus_dict, cpu_util_dict, rss_K_dict, io_bps_dict, state_ncpu_dict
        

    def getSunburstData(self):
        #prepare required information in data_dash
        ts = time.time()
        more_data    = {k:v[0:USER_INFO_IDX] + v[USER_INFO_IDX][0:7] for k,v in self.data.items() if len(v)>USER_INFO_IDX } #flatten hostdata
        less_data    = {k:v[0:USER_INFO_IDX]                         for k,v in self.data.items() if len(v)<=USER_INFO_IDX }
        hostdata_flat= dict(more_data,**less_data)

        keys_id      =(u'job_id',u'user_id',u'qos', u'num_nodes', u'num_cpus')
        data_dash    ={jid:{k:jinfo[k] for k in keys_id} for jid,jinfo in self.currJobs.items()} #extract set of keys
        #this appends a dictionary for all of the node information to the job dataset
        for jid, jinfo in self.currJobs.items():
            data_dash[jid]["node_info"] = {n: hostdata_flat.get(n,[]) for n in jinfo.get(u'cpus_allocated').keys()}

        if len(data_dash) == 0:
            return EMPTYPROCDATA_MSG + '\n\n' + repr(self.data)
        for jid, jinfo in sorted(data_dash.items()):
            username = MyTool.getUser(jinfo[u'user_id'])
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
            data_dash[jid]['list_jobid']   =[jid]                             * num_nodes
            data_dash[jid]['list_username']=[username]                        * num_nodes
            data_dash[jid]['list_qos']     =[jinfo[u'qos']]                   * num_nodes
            data_dash[jid]['list_group']   =[MyTool.getUserOrgGroup(username)]* num_nodes

        #need to filter data_dash so that it no longer contains users that are "None"->this was creating sunburst errors
        #open('/tmp/sunburst.tmp', 'w').write(repr(data_dash))

        # get flat list corresponding to each node
        list_nodes_flat     =reduce((lambda x,y: x+y), [v['list_nodes']    for v in data_dash.values()])
        list_loads_flat     =reduce((lambda x,y: x+y), [v['list_load']     for v in data_dash.values()])
        list_cpus_flat      =reduce((lambda x,y: x+y), [v['cpu_list']      for v in data_dash.values()])
        list_RSS_flat       =reduce((lambda x,y: x+y), [v['list_RSS']      for v in data_dash.values()])
        list_VMS_flat       =reduce((lambda x,y: x+y), [v['list_VMS']      for v in data_dash.values()])
        list_job_flatn      =reduce((lambda x,y: x+y), [v['list_jobid']    for v in data_dash.values()])
        list_part_flatn     =reduce((lambda x,y: x+y), [v['list_qos']      for v in data_dash.values()])
        list_usernames_flatn=reduce((lambda x,y: x+y), [v['list_username'] for v in data_dash.values()])
        list_group_flatn    =reduce((lambda x,y: x+y), [v['list_group']    for v in data_dash.values()])

        # merge above list into nested list
        listn  =[[list_group_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_loads_flat[i]] for i in range(len(list_nodes_flat))]
        listrss=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_RSS_flat[i]]    for i in range(len(list_nodes_flat))]
        listvms=[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i],list_VMS_flat[i]]    for i in range(len(list_nodes_flat))]
        listns =[[list_part_flatn[i],list_usernames_flatn[i],list_job_flatn[i],list_nodes_flat[i]]                     for i in range(len(list_nodes_flat))]
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
        d_vms     = MyTool.createNestedDict("VMS",   ["partition","user","job","node"],          data_dfvms,   "vms")
        d_rss     = MyTool.createNestedDict("RSS",   ["partition","user","job","node"],          data_dfrss,   "rss")
        d_state   = MyTool.createNestedDict("state", ["partition","user","job","state", "node"], data_dfstate, "load")

        d_load  = {'name':'root', 'sysname':'load',  'children':SLURMMonitorData.df2nested(data_dfload, 'load')}
        d_vms   = {'name':'root', 'sysname':'VMS',   'children':SLURMMonitorData.df2nested(data_dfvms,  'vms')}
        d_rss   = {'name':'root', 'sysname':'RSS',   'children':SLURMMonitorData.df2nested(data_dfrss,  'rss')}
        
        ts = logTest("Done", ts)
        return d_load, d_vms, d_rss, d_state, list_usernames_flatn

    @cherrypy.expose
    def getLowResourceJobs (self, job_length_secs=ONE_DAY_SECS, job_width_cpus=1, job_cpu_avg_util=0.1, job_mem_util=0.3):
        job_dict = self.getLUJobs(self.updateTS, self.currJobs, float(job_cpu_avg_util), int(job_length_secs), int(job_width_cpus), float(job_mem_util))
        return json.dumps([self.updateTS, job_dict])

    #get the total cpu time of uid on node
    def getJobNodeTotalCPUTime(self, jid, node):
        time_lst = [ d['cpu_time'] for d in self.jobNode2ProcRecord[jid][node][1].values() ]

        return sum(time_lst)

    # return cpuUtil, rss, vms, iobps, len(job_proc), fds
    def getJobUsageOnNode (self, jid, job, n_name, node):
        uid = job['user_id']
        if len(node) > USER_INFO_IDX:
           #09/09/2019 add jid
           #calculate each job's
           user_proc = [userInfo for userInfo in node[USER_INFO_IDX:] if userInfo[1]==uid ]
           if len(user_proc) != 1:
              logger.warning("User {} has {} record on {}. Ignore".format(uid, len(user_proc), n_name))
              return None, None, None, None, None, None

           job_proc = [proc for proc in user_proc[0][7] if proc[PROC_JID_IDX]==jid]
           # summary data in job_proc
           # [pid, CPURate, 'create_time', 'user_time', 'system_time', 'rss', vms, cmdline, IOBps, jid]
           cpuUtil  = sum([proc[1]         for proc in job_proc])
           rss      = sum([proc[5]         for proc in job_proc])
           vms      = sum([proc[6]         for proc in job_proc])
           iobps    = sum([proc[8]         for proc in job_proc])
           fds      = sum([proc[10]        for proc in job_proc])
           return cpuUtil, rss, vms, iobps, len(job_proc), fds
        else:
           logger.warning("Job {} has no proc record on node {}.".format(jid, n_name))
           return 0, 0, 0, 0, 0, 0

    @cherrypy.expose
    def index(self):
        return "{}: data={}, currJobs={}".format(MyTool.getTsString(self.updateTS), self.data, self.currJobs)

    @cherrypy.expose
    def log(self, **args):
       self.inMemLog.append(args)

if __name__=="__main__":
   cherrypy.config.update({'log.screen':         True,
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

   cherrypy.quickstart(SLURMMonitorData(), '/', conf)
