import cherrypy, copy, _pickle as cPickle, datetime, json, os
import logging, re, sys, threading, time, traceback, zlib
from collections import defaultdict
from functools import reduce

import pandas

from EmailSender     import JobNoticeSender
from querySlurm      import SlurmCmdQuery
from queryPyslurm    import PyslurmQuery
from queryBright     import BrightRelayClient

import config, sessionConfig
import MyTool
import inMemCache

# Directory where processed monitoring data lives.
TS_IDX             = 2
USER_INFO_IDX      = 3  # incoming data and self.data {node: [status, delta, ts, users_procs, ...]...}
USER_PROC_IDX      = 7  # in users_procs [uname, uid, user_alloc_cores, proc_cnt, totCPURate, totRSS, totVMS, procs, totIO, totCPU]
STATE_IDX,DELTA_IDX,TS_IDX,USERS_IDX = range(4)
PID_IDX,CPU_IDX,CREATE_TIME_IDX,USER_TIME_IDX,SYS_TIME_IDX,RSS_IDX,VMS_IDX,CMD_IDX,IO_IDX,JID_IDX,FDS_IDX,READ_IDX,WRITE_IDX,UID_IDX,THREADS_IDX   = range(15)   
                        # in procs [[pid, intervalCPUtimeAvg, create_time, user_time, system_time, mem_rss, mem_vms, cmdline, in    tervalIOByteAvg, jid],...]
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
    def __init__(self, name):
        self.cluster           = name
        self.updateTS          = time.time()
        self.data              = {}                   #'No data received yet. Please wait a minute and come back.'
        self.noslurm_data      = {}
        self.currJobs          = {}                   #re-created in updateMonData
        self.node2jids         = {}                   #{node:[jid...]} {node:{jid: {'proc_cnt','cpu_util','rss','iobps'}}}
        self.uid2jid           = {}                   #
        self.pyslurmJobs       = {}
        self.pyslurmNodes      = {}
        self.pyslurmData       = {}
        self.jobNode2Proc      = defaultdict(lambda: defaultdict(lambda: (0, defaultdict(list))))
        self.inMemCache        = inMemCache.InMemCache()
        self.inMemLog          = inMemCache.InMemLog()

        self.checkTS           = 0
        self.checkLUJ_TS       = 0
        self.checkResult       = {}                   # ts: {}
        self.jobNoticeSender   = JobNoticeSender()
        self.lock              = threading.Lock()
        self.bright1           = BrightRelayClient()
        logger.info("Create SLURMMonitorData {}".format(self.cluster))

    def hasData (self):
        return self.data!={}

    def getNode2Jobs (self, node):
        return [self.currJobs[jid] for jid in self.node2jids.get(node, []) if jid in self.currJobs]

    @cherrypy.expose
    def getNodesData(self):
        return repr(self.data)

    @cherrypy.expose
    def getNodeData(self, node):
        return repr(self.allData.get(node,'No data'))

    @cherrypy.expose
    def getJobData(self):
        return repr(self.currJobs)

    @cherrypy.expose
    def getAllJobData(self):
        return '({},{})'.format(self.updateTS, self.pyslurmJobs)

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
    def getNodeGPURecord (self, gpudata, node_name, node_state, state_str, node_gpus, alloc_jobs):
        if not node_gpus:
           return {}
        if not gpudata:
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
               if node_name not in gpudata.get(gpu_name,{}):
                  logger.warning("gpudata not including {}:{}".format(node_name, gpu_name))
               if node_name not in jobInfo['cpus_allocated']:
                  logger.error("jobInfo not including {} -{}".format(node_name, jobInfo['cpus_allocated']))
               # sometimes, queryBright cannot get some gpu's data, in that case, use 0
               gpu_util  = gpudata.get(node_name, {}).get(gpu_name,0)
               gpu_label = '{}_{}: gpu_util={:.1%}, job=({},{},{} cpu, {})'.format(node_name, gpu_name, gpu_util, jid, MyTool.getUser(jobInfo['user_id']), jobInfo['cpus_allocated'][node_name], gpu_alloc)
            else:
               gpu_util  = 0
               gpu_label = '{}_{}: state={}'.format(node_name, gpu_name, state_str)
            gpus[gpu_name] = {'label':gpu_label, 'state': gpu_state, 'job': jid, 'util':gpu_util}
        return gpus

    
    def getHeatmapWorkerData (self, gpudata, weight, avg_minute=0):
        node2job = self.node2jids
        workers  = []  #dataset1 in heatmap
        if self.cluster == 'Popeye':
            sorted_d = sorted(self.data, key=lambda x: x[4:].zfill(5))
        else:
            sorted_d = sorted(self.data)
        for hostname in sorted_d:
            hostinfo     = self.data[hostname]
            pyslurmNode  = self.pyslurmNodes[hostname]
            node_mem_M   = pyslurmNode['real_memory']
            alloc_jobs   = node2job.get(hostname, [])
            if avg_minute==0:
               if len(hostinfo) > USER_INFO_IDX: #has user proc information
                  node_cpu_util  = sum ([hostinfo[idx][4] for idx in range(USER_INFO_IDX, len(hostinfo))])
               else:
                  node_cpu_util  = 0
            else:
               node_cpu_util = self.inMemCache.queryNodeAvg(hostname, avg_minute)
            node_mem_util= (node_mem_M-pyslurmNode['free_mem']) / node_mem_M  if pyslurmNode['free_mem'] else 0 #ATTN: from slurm, not monitor, if no free_mem, in general, node is DOWN so return 0. TODO: Not saving memory information in cache. The sum of proc's RSS does not reflect the real value.
            node_record  = self.getHeatmapNodeRecord(hostname, hostinfo, alloc_jobs, node_cpu_util, node_mem_util, gpudata)
            node_record['comb_util'] = (weight['cpu']*node_record['util'] + weight['mem']*node_record['mem_util'])/(weight['cpu'] + weight['mem'])
            workers.append(node_record)
              #{'name':hostname, 'stat':state, 'core':node_cores, 'util':node_cpu_util/node_cores, 'mem_util':node_mem_util, 'jobs':alloc_jobs, 'acct':job_accounts, 'labl':nodeLabel, 'gpus':gpuLabel, 'gpuCount':node_gpus})

        return workers

    def nodeAllocated (hostinfo):
        return 'ALLOCATED' in hostinfo[0] or 'MIXED' in hostinfo[0]

    def getHeatmapNodeRecord (self, hostname, hostinfo, alloc_jobs, node_cpu_util, node_mem_util, gpudata):
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
              gpus = self.getNodeGPURecord (gpudata, hostname, state, hostinfo[0], node_gpus, alloc_jobs)
        else:      # node not in use
           state        = 0 if 'IDLE' in hostinfo[0] else -1
           if not node_gpus:
              nodeLabel = '{} ({} cpu, {}GB): state={}'.format        (hostname, node_cores, int(node_mem_M/1024), hostinfo[0])
           else:
              nodeLabel = '{} ({} cpu, {} gpu, {}GB): state={}'.format(hostname, node_cores, node_gpus, int(node_mem_M/1024), hostinfo[0])
              gpus = self.getNodeGPURecord (gpudata, hostname, state, hostinfo[0], node_gpus, [])
        rlt = {'name':hostname, 'stat':state, 'core':node_cores, 'util':node_cpu_util/node_cores, 'mem_util':node_mem_util, 'jobs':alloc_jobs, 'acct':job_accounts, 'labl':nodeLabel, 'gpus':gpus, 'gpuCount':node_gpus}
        #TODO: add comb_util

        #return nodeLabel, gpus, state
        return rlt

    # return (gpu_nodes, max_gpu_cnt)
    def getCurrJobGPUNodes (self):
        return PyslurmQuery.getJobGPUNodes(self.currJobs, self.pyslurmNodes)

    def getCurrJobGPUDetail (self):
        return PyslurmQuery.getJobGPUDetail(self.currJobs)

    # add attributes to jobs
    def updateJobsAttr (cluster, ts, jobs, jobNode2Procs, pyslurmNodes):
        #check self.currJobs and locate those jobs in question
        for jid, job in jobs.items():
            job_runtime     = ts - job['start_time']
            if job_runtime < 0.01:
               logger.warning("Job {} has an invaild period {}-{}={}. Ignore the update.".format(jid, ts, job['start_time'], job_runtime))
               continue

            total_cpu_time           = 0
            total_rss                = 0
            total_node_mem           = MyTool.getTresDict(job['tres_alloc_str']).get('mem',0)
            total_node_mem           = MyTool.convert2M(total_node_mem)
            total_io_bps             = 0
            total_cpu_util           = 0
            #add attributes to job
            job['user']              = MyTool.getUser(job['user_id'], cluster)
            job['node_cpu_time']     = {}         # cpu time on each node
            job['node_cpu_util_avg'] = {}
            job['node_rss']     = {}
            job['node_io_bps_curr']  = {}
            job['node_cpu_util_curr']= {}
            job['node_num_proc']     = {}
            for node in job['cpus_allocated']:
                node_cpu_time, node_rss, node_mem, node_io_bps, node_cpu_util= 0,0,0,0,0
                savTs, procs         = jobNode2Procs[jid][node]
                if savTs:
                   #add up proc cpu_time to get node and job cpu_time
                   node_cpu_time     = sum([p[USER_TIME_IDX]+p[SYS_TIME_IDX] for p in procs.values()])
                   node_rss          = sum([p[RSS_IDX]>>10                   for p in procs.values()])    #convert to K
                   node_io_bps       = sum([p[IO_IDX]                        for p in procs.values()]) 
                   node_cpu_util     = sum([p[CPU_IDX]                       for p in procs.values()]) 
                   total_cpu_time   += node_cpu_time
                   total_rss        += node_rss
                   total_io_bps     += node_io_bps
                   total_cpu_util   += node_cpu_util
                   #deal with mem, unnessary as job['tres_alloc_str'] has it
                   #node_tres           = MyTool.getTresDict(pyslurmNodes[node]['tres_fmt_str'])
                   #if 'mem' in node_tres:   # memory is shared
                   #   ratio            = job['cpus_allocated'][node] / node_tres['cpu'] if 'cpu' in node_tres else 1
                   #   total_node_mem  += MyTool.convert2K(node_tres['mem']) * ratio
                   #else:
                   #   logger.error('ERROR: Node {} does not have mem {} in tres_fmt_str {}'.format(node, d, s))

                   #calculate and assign cpu avg util
                   job['node_cpu_time'][node]     = node_cpu_time 
                   if job['cpus_allocated'][node]:
                      job['node_cpu_util_avg'][node] = node_cpu_time / job_runtime / job['cpus_allocated'][node]
                   else:
                      logger.warning ("Job {}'s allocated CPUs on node{} is 0".format(jid, node))
                      job['node_cpu_util_avg'][node] = 0
                   job['node_rss'][node]     = node_rss
                   job['node_io_bps_curr'][node]  = node_io_bps
                   job['node_cpu_util_curr'][node]= node_cpu_util
                   job['node_num_proc'][node]     = len(procs)
                #no job information
                elif ts - job['start_time'] > DELAY_SECS*3 : #allow 60*3 seconds delay, 
                   if jid not in jobNode2Procs:
                      logger.warning('Job {} (start at {} on {}) is not in jobNode2Procs'.format(jid, MyTool.getTsString(job['start_time']), job['nodes']))
                   else:    # node not in jobNode2Procs[jid]
                      logger.warning('Node {} of Job {} (start at {} on {}) is not in jobNode2Procs'.format(node, jid, MyTool.getTsString(job['start_time']), job['nodes']))

            #calculate and assign job level util
            job['job_cpu_time']   = total_cpu_time
            job['job_inst_util']  = total_cpu_util / job['num_cpus']
            job['job_avg_util']   = total_cpu_time / job_runtime / job['num_cpus']
            job['job_io_bps']     = total_io_bps
            job['job_mem_util']   = total_rss / total_node_mem / 1024
            job['gpus_allocated'] = SLURMMonitorData.getJobAllocGPU(job, pyslurmNodes)
            job['cpu_eff']        = {'core-wallclock':job['num_cpus']*job_runtime, 'cpu_time':total_cpu_time}   #ATTENTION: currently, cpu_eff only appear for finished job, if it will be included in the running jobs, need to make modification
            #tres_alloc_str
            job['mem_eff']        = {'alloc_mem_MB':total_node_mem, 'alloc_nodes':job['num_nodes'], 'mem_KB':total_rss}

    def getJobAllocGPU (job, node_dict):
        node_list      = [node_dict[node] for node in job['cpus_allocated']]
        gpus_allocated = MyTool.getGPUAlloc_layout(node_list, job['gres_detail']) if job['gres_detail'] else {}
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

    def getCurrEmptyAllocNode (self):
        return [nm for nm, ninfo in self.data.items() if SLURMMonitorData.nodeAllocated(ninfo) and (len(ninfo) <= USER_INFO_IDX or      not ninfo[USER_INFO_IDX])]

    def getLowUtilNodes (self):
        # node with low resource utlization
        empty_nodes= self.getCurrEmptyAllocNode()
        low_nodes  = []
        for nm in empty_nodes:
            jobs     = self.getNode2Jobs(nm)
            avg_util = sum([job.get('job_avg_util',0) for job in jobs])
            avg_mem  = sum([job.get('job_mem_util',0) for job in jobs])
            u_set    = set([job['user_id'] for job in jobs])
            u_lst    = [MyTool.getUser(uid) for uid in u_set]
            low_nodes.append({'name':nm, 'msg':'Node is allocated to job {} of user {}. The average cpu utilization is {} and the average memory utiization is {}.'.format([job['job_id'] for job in jobs], u_lst, avg_util, avg_mem)})
        return low_nodes

    # return the low util jobs with settings
    def getCurrLUJobs (self, gpu_data, luj_settings):
        if self.updateTS == self.checkLUJ_TS:   # check already
           return self.checkResult
        else:
           self.checkLUJ_TS = self.updateTS
           result = SLURMMonitorData.getLowUtilJobs(self.updateTS, self.currJobs, gpu_data, luj_settings)
           self.checkResult = result
           return result

    def getLowUtilJobs (ts, jobs, gpu_data, lmt_settings, exclude_acct=['scc']):
        #check and locate those jobs in question
        result = {}            # return {jid:job,...}
        for jid, job in jobs.items():
            if job['account'] in exclude_acct: continue  # 
            period = ts - job['start_time']
            if period                < lmt_settings['run_time_hour']*3600: continue    # short job
            if job.get('num_cpus',1) < lmt_settings['alloc_cpus']:         continue    # small job 
            if (job['job_avg_util']  > lmt_settings['cpu']/100) or (job['job_mem_util']  > lmt_settings['mem']/100) or (job['job_inst_util'] > lmt_settings['cpu']/100): 
               continue
            if not job['gpus_allocated']: 
               job['gpu_avg_util'] = -1
               job['remained_time_str']= MyTool.time_sec2str(job['time_limit']*60 - job['run_time'])
               result[job['job_id']] = job
            elif gpu_data:     # if allocate GPU and have gpu_data
               #if job is allocated gpu, check gpu util low
               for node, gpu_lst in job['gpus_allocated'].items():
                   gpu_keys = ["gpu{}".format(g) for g in gpu_lst]
                   gpu_sum  = sum([gpu_data[node][k] for k in gpu_keys])
                   gpu_avg  = gpu_sum / len(gpu_lst)
                   job['gpu_avg_util'] = gpu_avg
                   if gpu_avg < lmt_settings['gpu']/100:
                       job['remained_time_str']= MyTool.time_sec2str(job['time_limit']*60 - job['run_time'])
                       result[job['job_id']] = job

        return result

    @cherrypy.expose
    def getUnbalancedJobs (self, cpu_stdev, rss_stdev, io_stdev):
        cpu_stdev, rss_stdev, io_stdev = int(cpu_stdev), int(rss_stdev), int(io_stdev)
        self.calculateStat (self.currJobs, self.data)
        sel_jobs = [job for jid, job in self.currJobs.items()
                         if (job['node_cpu_stdev']>cpu_stdev) or (job['node_rss_stdev']>rss_stdev) or (job['node_io_stdev']>io_stdev)]
        return sel_jobs

    #for a job, caclulate the deviaton of the cpu, mem, rss on different allocated nodes
    def calculateStat (self, jobs, nodes):
        for jid, job in jobs.items():
            if 'node_cpu_stdev' in job:     # already calculated
               return

            if job['num_nodes'] == 1:
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = 0,0,0     #cpu util, rss in KB, io bps
               continue
            #[u_name, uid, allocated_cpus, len(pp), totIUA_util, totRSS, totVMS, pp, totIO, totCPU_rate]
            job_nodes= MyTool.nl2flat(job['nodes'])
            node_cpu = [job['node_cpu_util_avg'].get(node,0)  for node in job_nodes]
            node_rss = [job['node_rss'].get(node,0)      for node in job_nodes]
            node_io  = [job['node_io_bps_curr'].get(node,0)   for node in job_nodes]
            if len(node_cpu) > 1:
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = MyTool.stddev(node_cpu), MyTool.stddev(node_rss), MyTool.stddev(node_io)     
               #job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = MyTool.pstdev(node_cpu), MyTool.pstdev(node_rss), MyTool.pstdev(node_io)     
            else:
               logger.error('Job {} has not enough process running on allocated nodes {} ({}) to calculate standard deviation.'.format(jid, job['nodes'], proc_cpu))
               job['node_cpu_stdev'],job['node_rss_stdev'], job['node_io_stdev'] = 0,0,0

    @cherrypy.expose
    def test(self, **args):
        return "hello"

    def updateNodeData (sav_data, new_data):
        for node, n_data in new_data.items():
            sav_data[node] = n_data
            
    #update self.jobNode2Proc with new data in jobNode2Proc
    def updateJobNode2Proc (jobNode2ProcSav, jobNode2Procs, currJobs, update_ts):
        # copy proc information
        for jid, node_info in jobNode2Procs.items():
            for node, ts_procs in node_info.items():
                savTs, savProcs = jobNode2ProcSav[jid][node]
                if not savTs: # set to new information
                   jobNode2ProcSav[jid][node] = ts_procs
                else:                                  # replace old information
                   if ( update_ts > savTs):
                      jobNode2ProcSav[jid][node] = ts_procs
        # remove finished job
        done_job = [jid for jid, jinfo in currJobs.items() if jinfo['job_state'] not in ['RUNNING', 'PENDING', 'PREEMPTED']]
        for jid in done_job:
            jobNode2ProcSav.pop(jid, {})
        # remove -1 (not belong to a slurm job)'s proc if it is too old
        noslurm = jobNode2ProcSav[-1]
        for node in list(noslurm.keys()):
            ts, *other = noslurm[node]
            if ts < (update_ts - ONE_DAY_SECS):
               noslurm.pop(node, ())
        
    #get avg GPU util of all running jobs' GPU nodes for last n hours
    @cherrypy.expose
    def getJobGPUData (self, hours=1):
        if self.cluster == "Rusty":
           gpu_nodes, max_gpu_cnt = self.getCurrJobGPUNodes()
           return self.bright1.getNodesGPUAvg(list(gpu_nodes), start=int(time.time()-hours*3600))
        else:
           return {}

    def checkJobs (self):
        #check hourly for long run low util jobs and send notice
        luj_settings = sessionConfig.getSetting('low_util_job')
        if luj_settings['email']:
           now        = datetime.datetime.now()
           send_hours = luj_settings['email_hour']
           if now.hour in send_hours and now.minute < 5:
              if self.updateTS - self.checkTS > 600:     # at least 10 minute from last time to avoid send it twice 
                  logger.info('{}:({})'.format(self.updateTS, self.checkTS))   
                  gpu_data    = self.getJobGPUData ()
                  low_util    = self.getCurrLUJobs (gpu_data, luj_settings)
                  logger.info('{}:({}) low_util={}'.format(self.updateTS, self.checkTS, low_util.keys()))   
                  if low_util:
                     self.jobNoticeSender.sendLUSummary(self.cluster, low_util, luj_settings)
                  #self.jobNoticeSender.sendNotice(self.updateTS, low_util)
                  self.checkTS = self.updateTS

    @cherrypy.expose
    def updateSlurmData(self, **args):
      try:
        #receive data
        d =  cherrypy.request.body.read()
        self.updateTS, slurm_data, self.currJobs, self.node2jids, self.pyslurmData, noSlurm_data, jobNode2Proc = SLURMMonitorData.extractData(d)
        #update internal data
        SLURMMonitorData.updateNodeData     (self.data,             slurm_data)
        SLURMMonitorData.updateNodeData     (self.noslurm_data,     noSlurm_data)
        SLURMMonitorData.updateJobNode2Proc (self.jobNode2Proc, jobNode2Proc, self.currJobs, self.updateTS)
        SLURMMonitorData.updateJobsAttr (self.cluster, self.updateTS, self.currJobs, self.jobNode2Proc, self.pyslurmData['nodes'])
        #other
        self.inMemCache.append(self.data,         self.updateTS, self.pyslurmData['jobs'])
        self.inMemCache.append(self.noslurm_data, self.updateTS, self.pyslurmData['jobs'])
        self.pyslurmJobs  = self.pyslurmData['jobs']
        self.pyslurmNodes = self.pyslurmData['nodes']

        #check hourly for long run low util jobs and send notice
        self.checkJobs ()
      except Exception as inst:
        logger.exception("{}".format(inst))

    def extractPyslurmJobData (pyslurmJobData):
        jobData   = dict([(jid,job) for jid, job in pyslurmJobData.items() if job['job_state'] in ['RUNNING', 'CONFIGURING']])
        node2jobs = defaultdict(list)  #nodename: joblist
        for jid, jinfo in jobData.items():
            for nodename in jinfo.get('cpus_allocated', {}).keys():
                node2jobs[nodename].append(jid)
        return jobData, node2jobs

    def extractJid2Proc (nInfo):
        #extract jid2proc, jid=-1 means no job 
        jid2proc = defaultdict(list)          #jid: [proc]
        if len(nInfo) > USER_INFO_IDX and nInfo[USER_INFO_IDX]:     # have user data
           for user_procs in nInfo[USER_INFO_IDX:]: #worker may has multiple users, 
                                            #[uname, uid, alloc_cores, proc_cnt, totIUA, totRSS, totVMS, procs, totIO, totCPU])
               if len(user_procs) > USER_PROC_IDX and user_procs[USER_PROC_IDX]:    # have proc data
                  for proc in user_procs[USER_PROC_IDX]:
                      jid2proc[proc[JID_IDX]].append(proc)    #proc[9] is jid, -1 means no jid, 09/09/2019, add jid
        return jid2proc

    # use self.data and self.allData
    def extractData (d):
        updateTS, hn2info, pyslurmData = cPickle.loads(zlib.decompress(d))
        pyslurmData['updateTS']        = updateTS

        currJobs, node2jids            = SLURMMonitorData.extractPyslurmJobData(pyslurmData['jobs'])

        #extract slurmData,noSlurmData and jobNode2Proc from new data
        slurmData    = {}                         #assign to self.data later
        noSlurmData  = {}
        jobNode2Proc = defaultdict(lambda: defaultdict(dict))   # {jobid: {node: (ts, {pid: [procs]})}}
        for node,nInfo in hn2info.items():
            #save new data to slurmData or noSlurmData
            if updateTS - int(nInfo[TS_IDX]) > 600: # Ignore data
               logger.debug("{}: ignore old data of node {} at {}.".format(updateTS, node, MyTool.getTsString(nInfo[TS_IDX])))
               continue
            if node in pyslurmData['nodes']:
               slurmData[node] = nInfo                      #nInfo: status, delta, ts, users_procs
            else:
               noSlurmData[node] = nInfo

            #extract jid2proc, jid=-1 means no job, and use the information to update history
            jid2proc = SLURMMonitorData.extractJid2Proc (nInfo)
            for jid, procs in jid2proc.items():
                jobNode2Proc[jid][node]=(nInfo[TS_IDX], dict([(p[PID_IDX], p) for p in procs]))

        return updateTS, slurmData, currJobs, node2jids, pyslurmData, noSlurmData, jobNode2Proc

    def updateNodeMonAttr_noslurm (node, node_data, currJobs, jobNode2Procs):
        if 'mon_state' in node: #already added 
           return

        nodeName          = node['name']
        #get data from self.data
        node['mon_state'] = node_data[nodeName][STATE_IDX]                                #state already there
        node['updateTS']  = node_data[nodeName][TS_IDX]

        #slurm jobs
        node['jobCnt']    = 0
        node['alloc_cpus']= 0
        node['running_jobs']=[]
        node['jobProc']   = {-1: {'job':None, 'procs':jobNode2Procs[-1][nodeName][1]}}
        node['procCnt']   = sum([1 for d in node['jobProc'].values() for p in d['procs']]) 

    def updateNodeMonAttr (node, node_data, slurm_jids, currJobs, jobNode2Procs):
        if 'mon_state' in node: #already added 
           return

        nodeName          = node['name']
        #get data from self.data
        node['mon_state'] = node_data[nodeName][STATE_IDX]                                #state already there
        node['updateTS']  = node_data[nodeName][TS_IDX]

        #slurm jobs
        node['jobCnt']    = len(slurm_jids)
        node['alloc_cpus']= sum([currJobs[jid]['cpus_allocated'][nodeName] for jid in slurm_jids])
        node['running_jobs']= [jid for jid in slurm_jids if currJobs[jid]['job_state']=='RUNNING']
        #organize procs by job: {jid: {'job': job, 'procs': [proc]}}
        node['jobProc']   = dict([(jid, {'job':currJobs.get(jid,None), 'procs':jobNode2Procs[jid][nodeName][1]}) for jid in slurm_jids])
        node['procCnt']   = sum([1 for d in node['jobProc'].values() for p in d['procs']])
  
    #add new proc data to self.pyslurmNodes[nodeName]
    def getNodeProc (self, nodeName):
        if nodeName in self.pyslurmNodes:
           #get nodeName data from self.pyslurmNodes
           node      = self.pyslurmData['nodes'][nodeName]  #'name', 'state', 'cpus', 'alloc_cpus',
           node['gpus'],node['alloc_gpus'] = MyTool.getGPUCount(node['gres'], node['gres_used'])
        else:     # not a slurm node
           node      = {'name':nodeName, 'updateTS':0}

        slurmMon     = False       # if it is a slurm node and has monitoring data
        if nodeName in self.data:
           slurmMon  = True
           SLURMMonitorData.updateNodeMonAttr (node, self.data, self.node2jids.get(nodeName,[]), self.currJobs, self.jobNode2Proc)
        elif nodeName in self.noslurm_data:
           SLURMMonitorData.updateNodeMonAttr_noslurm (node, self.noslurm_data, self.currJobs, self.jobNode2Proc)
        else:
           node['note']= "No monitoring data"
        
        return slurmMon, node
       
    #return dict (workername, workerinfo)
    def getUserNodeProc (self, user):
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
                for procs in userInfo[JID_IDX]:     
                    item = [node, jid]

    def getJobNodeProcUtil (self, jid, nodes):
        rlt = []
        cpu_lst, mem_lst, io_lst=[], [], []
        if jid in self.jobNode2Proc:
           for node in nodes:
               ts, procs = self.jobNode2Proc[jid][node]
               #{1779064: [1779064, 0.0, 1679113036.25, 0.0, 0.01, 3039232, 13180928, ['/bin/bash', '/cm/local/apps/slurm/var/spool/job2204874/slurm_script'], 0, 2204874, 4, 520192, 21864448, 1359, 1],
               if procs:
                  j_cpu_util  = sum([p[CPU_IDX] for p in procs.values()])
                  j_mem_rss   = sum([p[RSS_IDX] for p in procs.values()])
                  j_io_bps    = sum([p[IO_IDX]  for p in procs.values()])
                  cpu_lst.append (j_cpu_util)
                  mem_lst.append (j_mem_rss)
                  io_lst.append  (j_io_bps)
               else:     # no record of proces
                  cpu_lst.append (0)
                  mem_lst.append (0)
                  io_lst.append  (0)
           return cpu_lst, mem_lst, io_lst
        else:
           logger.info("Job {} not in self.jobNode2Proc".format(jid))
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
        
    def getSunburstData(self):
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

    @cherrypy.expose
    def getLowResourceJobs (self, job_length_secs=ONE_DAY_SECS, job_width_cpus=1, job_cpu_avg_util=0.1, job_mem_util=0.3):
        # called from jupyter
        job_dict = SLURMMonitorData.getLowUtilJobs(self.updateTS, self.currJobs, float(job_cpu_avg_util), int(job_length_secs), int(job_width_cpus), float(job_mem_util))
        return json.dumps([self.updateTS, job_dict])

    #get the total cpu time of uid on node
    def getJobNodeTotalCPUTime(self, jid, node):
        time_lst = [ p[USER_TIME_IDX]+p[SYS_TIME_IDX] for p in self.jobNode2Proc[jid][node][1].values() ]   

        return sum(time_lst)

    # return cpuUtil, rss, vms, iobps, len(job_proc), fds, not related to history
    def getJobUsageOnNode (self, jid, job, n_name, node):
        uid = job['user_id']
        if len(node) > USER_INFO_IDX:
           #09/09/2019 add jid
           #calculate each job's
           user_proc = [userInfo for userInfo in node[USER_INFO_IDX:] if userInfo[1]==uid ]
           if len(user_proc) != 1:
              logger.warning("User {} has {} record on {}. Ignore".format(uid, len(user_proc), n_name))
              return None, None, None, None, None, None

           job_proc = [proc for proc in user_proc[0][7] if proc[JID_IDX]==jid]
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

    def getUserJobsByState (self, uid):
        result    = defaultdict(list)  #{state:[job...]}
        jobs      = [job for job in self.pyslurmJobs.values() if job['user_id']==uid]
        for job in jobs:
            result[job['job_state']].append (job)
        return dict(result)

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
