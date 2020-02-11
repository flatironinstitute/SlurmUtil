#!/usr/bin/env python

import time
t1=time.time()
import json, operator, pwd, sys, pickle
from datetime import datetime, timezone, timedelta

import influxdb
from collections import defaultdict
import MyTool
import pdb

with open('./config.json') as config_file:
     APP_CONF = json.load(config_file)

class InfluxQueryClient:
    Interval = 61
    LOCAL_TZ = timezone(timedelta(hours=-4))
    CLIENT_INS = None

    @classmethod
    def getClientInstance (cls):
        if not cls.CLIENT_INS:
           cls.CLIENT_INS = InfluxQueryClient()

        return cls.CLIENT_INS

    def __init__(self, influxServer=None, dbname=None):
        if not influxServer:
           influxServer = APP_CONF['influxdb']['host']
        if not dbname:
           dbname       = APP_CONF['influxdb']['db']
        self.influx_client = self.connectInflux (influxServer, 8086, dbname)
        print("INFO: influx_client= " + repr(self.influx_client._baseurl))

    def connectInflux (self, host, port, dbname):
        return influxdb.InfluxDBClient(host, port, "yliu", "", dbname)

        
    # given a jid, return the timed history on its assigned nodes
    def getSlurmJobHistory (self, jid):
        #select nodes from slurm_jobs where job_id = jid
        history = {}
        nodes = self.getSlurmJobInfo (jid)
        for node in nodes:
            #mem,cpu = getSlurmNodeResourceHistory (node)
            history[node]=[mem, cpu]
            
        return history

    def getSlurmJobInfo (self, jid):
        #jid is string
        t1=time.time()
        query= "select nodes,eligible_time,start_time,end_time,user_id from autogen.slurm_jobs where job_id='" + jid + "'"
        #returned time is local timestamp
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        point['submit_time']=point['time']
        print("INFO: getSlurmJobInfo take time " + str(time.time()-t1))
        return point
        
    #return nodelist's time series of the a uid, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getNodeProcData (self, node, jid, start_time='', stop_time=''):
        t1=time.time()

        #prepare query
        g =('('+n+')' for n in nodelist)
        hostnames = '|'.join(g)
        query= "select * from autogen.node_proc_mon where hostname=~/"+ hostnames + "/ and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.query(query)
        if not results:
           return None

        points   = results.get_points()
        node2seq = { n:{} for n in nodelist}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            if 'mem_rss_K' in point:
               mem_rss_K = MyTool.getDictNumValue(point, 'mem_rss_K')
            else:
               mem_rss_K = int(MyTool.getDictNumValue(point, 'mem_rss') / 1024)
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_rate'),
                                   MyTool.getDictNumValue(point, 'io_write_rate'), 
                                   mem_rss_K]

        print("INFO: getNodeProcData take time " + str(time.time()-t1))
        return node2seq


    #return nodelist's time series of the a uid, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmUidMonData(self, uid, nodelist, start_time, stop_time):
        t1=time.time()

        #prepare query
        g =('('+n+')' for n in nodelist)
        hostnames = '|'.join(g)
        query= "select * from autogen.cpu_uid_mon where uid = '" + str(uid) + "' and hostname=~/"+ hostnames + "/ and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.query(query)
        if not results:
           return None

        points   = results.get_points()
        node2seq = { n:{} for n in nodelist}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            if 'mem_rss_K' in point:
               mem_rss_K = MyTool.getDictNumValue(point, 'mem_rss_K')
            else:
               mem_rss_K = int(MyTool.getDictNumValue(point, 'mem_rss') / 1024)
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_rate'),
                                   MyTool.getDictNumValue(point, 'io_write_rate'), 
                                   mem_rss_K]

        #print(repr(node2seq))
        print("INFO: getSlurmUidMonData take time " + str(time.time()-t1))
        return node2seq

    #return the query result list of dict {pid: (ts, cpu, mem, io_r, io_w) ... }, ...}, NOT USED YET
    def getNodeJobProcData (self, node, jid, start_time='', stop_time=''):
        query   = "select * from autogen.node_proc_mon where hostname='" + node + "' and jid=" + str(jid)   #jid is int type in node_proc_mon
        if start_time:
           query += " and time >= " + str(int(start_time)) + "000000000"
        if stop_time:
           query += " and time <= " + str(int(stop_time)+1) + "000000000"

        results = self.query(query)
        if not results:
           return None, None, None

        points     = list(results.get_points()) # lists of dictionaries
        first_time = points[0]['time']
        last_time  = points[len(points)-1]['time']
        pids       = set([p['pid'] for p in points])
        rlt        = {}
        for pid in pids:
            rlt[pid]= [ (point['time'], MyTool.getDictNumValue(point, 'cpu_system_time') + MyTool.getDictNumValue(point, 'cpu_user_time'),
                               MyTool.getDictNumValue(point, 'mem_rss'),
                               MyTool.getDictNumValue(point, 'io_read_bytes'),
                               MyTool.getDictNumValue(point, 'io_write_bytes')) for point in points if point['pid']==pid] 

        return first_time, last_time, rlt

    #return the query result list of dict {hostname: {ts: [cpu, mem, io_r, io_w] ... }, ...}, NOT USED YET
    def getSlurmJobData (self, jid, start_time='', stop_time=''):
        query   = "select * from autogen.cpu_jid_mon where jid = '" + str(jid) + "'"   #jid is str type
        if start_time:
           query += " and time >= " + str(int(start_time)) + "000000000"
        if stop_time:
           query += " and time <= " + str(int(stop_time)+1) + "000000000"

        results = self.query(query)
        if not results:
           return None, None, None

        points  = list(results.get_points()) # lists of dictionaries
        rlt     = defaultdict(dict)
        for point in points:
            ts       = point['time']
            rlt[point['hostname']][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'),
                              MyTool.getDictNumValue(point, 'mem_rss_K'),
                              MyTool.getDictNumValue(point, 'io_read_rate'),
                              MyTool.getDictNumValue(point, 'io_write_rate')]
        if len(points)>0:
           start_time = points[0]['time']
           stop_time  = points[len(points)-1]['time']

        return start_time, stop_time, rlt

    #return the query result list of dictionaries
    def queryUidMonData (self, uid, start_time='', stop_time='', nodelist=[]):
        t1=time.time()

        query = "select * from autogen.cpu_uid_mon where uid = '" + str(uid) + "'"
        if nodelist:
           g =('('+n+')' for n in nodelist)
           hostnames = '|'.join(g)
           query += " and hostname=~/"+ hostnames + "/"
        if start_time:
           query += " and time >= " + str(int(start_time)) + "000000000"
        if stop_time:
           query += " and time <= " + str(int(stop_time)+1) + "000000000"

        print ("INFO: queryUidMonData " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='s')
        points  = list(results.get_points()) # lists of dictionaries
        for point in points:
            if 'mem_rss_K' not in point:
               point['mem_rss_K']=int (point['mem_rss'] / 1024)

        print("INFO: getSlurmUidMonData take time {} and return {} points".format((time.time()-t1), len(points)))
        return points

    #return time series of the a uid on all nodes, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmUidMonData_All(self, uid, start_time, stop_time):
        t1=time.time()

        #prepare query
        query= "select * from one_month.cpu_uid_mon where uid = '" + str(uid) + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("INFO: getSlurmUidMonData_All " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points() # lists of dictionaries

        node2seq = {}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            if node not in node2seq: node2seq[node]={}
            if 'mem_rss_K' in point:
               mem_rss_K = MyTool.getDictNumValue(point, 'mem_rss_K')
            else:
               mem_rss_K = int(MyTool.getDictNumValue(point, 'mem_rss') / 1024)
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_rate')   + MyTool.getDictNumValue(point, 'io_write_rate'), 
                                   mem_rss_K]
        #print(repr(node2seq))
        print("INFO: getSlurmUidMonData_All take time " + str(time.time()-t1))
        return node2seq

    #return all uid sequence of a node, {uid: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmNodeMonData(self, node, start_time, stop_time):
        #prepare query
        query= "select * from autogen.cpu_uid_mon where hostname = '" + node + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.query(query)
        if not results:
           return None, start_time, stop_time

        points  = list(results.get_points())
        uid2seq = {}
        for point in points: #points are sorted by point['time']
            ts      = point['time']
            uid     = point['uid']
            if uid not in uid2seq: uid2seq[uid] = {}
            if 'mem_rss_K' in point:
               mem_rss_K = MyTool.getDictNumValue(point, 'mem_rss_K')
            else:
               mem_rss_K = int(MyTool.getDictNumValue(point, 'mem_rss') / 1024)
            uid2seq[uid][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                 MyTool.getDictNumValue(point, 'io_read_rate'),
                                 MyTool.getDictNumValue(point, 'io_write_rate'), 
                                 mem_rss_K]

        if len(points)>0:
           start_time = points[0]['time']
           stop_time  = points[len(points)-1]['time']
          
        #print(repr(uid2seq))
        return uid2seq, start_time, stop_time

    # return list [[ts, run_time] ... ]
    def getSlurmJobRuntimeHistory (self, jobid, st, et):
        t1=time.time()
        query= "select * from autogen.slurm_job_mon where job_id = '" + str(jobid) + "' and time >= " + str(st) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("INFO: getSlurmJobRuntimeHistory " + query)

        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()
        nodeDataDict    = {}
        for point in points:
            #print (repr(point))
            nodeDataDict[point['time']] = point['run_time']

        print("INFO: getSMon take time " + str(time.time()-t1))
     
        return nodeDataDict
    
    def getSavedNodeHistory (self, filename='nodeHistory', days=3):
        with open('./{}_{}.pickle'.format(filename, days), 'rb') as f:
             rltSet = pickle.load(f)
        return rltSet
        
    def savNodeHistory      (self, filename='nodeHistory', days=3):
        st,et  = MyTool.getStartStopTS (days=days)
        rltSet = self.getNodeHistory(st, et) 
        with open('./{}_{}.pickle'.format(filename, days), 'wb') as f:
             pickle.dump(rltSet, f)
  
    # query autogen.slurm_pending and return {ts_in_sec:{state_reason:count}], ...}  
    def getNodeHistory(self, st, et):
        t1=time.time()
        
        query= "select * from autogen.slurm_node_mon where time >= " + str(int(st)) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("INFO: getNodeHistory query {}".format(query))

        results    = self.influx_client.query(query, epoch='ms')
        points     = list(results.get_points())
        print("INFO: getNodeHistory {} points between {} and {} take time {}".format(len(points), st, et, time.time()-t1))
        #point['tags']   = MyTool.sub_dict_exist_remove (item, ['name', 'boot_time', 'slurmd_start_time'])
        #point['fields'] = MyTool.sub_dict_exist_remove (item, ['cpus', 'cpu_load', 'alloc_cpus', 'state', 'free_mem', 'gres', 'gres_used', 'partitions', 'reason', 'reason_time', 'reason_uid', 'err_cpus', 'alloc_mem'])

        ts2AllocNodeCnt    = defaultdict(int)   # Alloc
        ts2IdleNodeCnt     = defaultdict(int)   # Idle
        ts2MixNodeCnt      = defaultdict(int)   # Mix 
        ts2DownNodeCnt     = defaultdict(int)   # not usable

        ts2AllocCPUCnt     = defaultdict(int)   # Alloc alloc CPU
        ts2MixCPUCnt       = defaultdict(int)   # Mix
        #ts2IdleAllocCPUCnt = defaultdict(int)   # Alloc idle CPU, once ALLOC, alloc_cpus=cpus
        ts2IdleCPUCnt      = defaultdict(int)   # Idle + Mix idle
        ts2DownCPUCnt      = defaultdict(int)   # not usable

        for point in points:
            ts           = point['time']
            node_state   = point['state']

            if 'ALLOCATED' in node_state: # running state
               ts2AllocNodeCnt[ts] += 1
               ts2AllocCPUCnt[ts]  += point['alloc_cpus']
            elif 'MIXED' in node_state:
               ts2MixNodeCnt[ts]   += 1

               alloc_cpus           = int(point['alloc_cpus']) if point.get('alloc_cpus', None) else 0  #point['alloc_cpus'] may return None
               ts2IdleCPUCnt[ts]   += point['cpus'] - alloc_cpus
               ts2MixCPUCnt[ts]    += alloc_cpus
            elif 'IDLE'  in node_state:
               ts2IdleNodeCnt[ts]  += 1
               ts2IdleCPUCnt[ts]   += point['cpus']
            else:
               ts2DownNodeCnt[ts]  += 1
               ts2DownCPUCnt[ts]   += point['cpus']

        print("INFO: getNodeHistory {} points between {} and {} take time {}".format(len(points), st, et, time.time()-t1))
        return ts2AllocNodeCnt, ts2MixNodeCnt, ts2IdleNodeCnt, ts2DownNodeCnt, ts2AllocCPUCnt, ts2MixCPUCnt, ts2IdleCPUCnt, ts2DownCPUCnt 

    def getSavedJobRequestHistory (self, filename='jobRequestHistory', days=3):
        with open('./{}_{}.pickle'.format(filename, days), 'rb') as f:
             rltSet = pickle.load(f)
       
        return rltSet 

    def savJobRequestHistory      (self, filename='jobRequestHistory', days=3):
        st,et  = MyTool.getStartStopTS (days=days)
        rltSet = self.getJobRequestHistory(st, et) 
        with open('./{}_{}.pickle'.format(filename, days), 'wb') as f:
             pickle.dump(rltSet, f)
  
        
    def getJobRequestHistory(self, st, et):
        t1=time.time()
        
        query= "select * from autogen.slurm_job_mon where time >= " + str(int(st)) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("INFO: getJobRequestHistory query {}".format(query))

        results    = self.influx_client.query(query, epoch='ms')
        points     = list(results.get_points())
        #point['tags']   = MyTool.sub_dict_remove       (item, ['job_id', 'user_id'])
        #point['fields'] = MyTool.sub_dict_exist_remove (item, ['job_state', 'state_reason', 'run_time', 'suspend_time', 'num_cpus', 'num_nodes', 'tres_req_str']) #
 
        runJidSet      = set()   #use set to remove duplicate ids
        pendJidSet     = set()   #use set to remove duplicate ids
        ts2ReqNodeCnt  = defaultdict(int)
        ts2ReqCPUCnt   = defaultdict(int)
        ts2PendReqNodeCnt = defaultdict(int)
        ts2PendReqCPUCnt  = defaultdict(int)
        for point in points:
            ts  = point['time']
            jid = point['job_id']
            tres_dict = MyTool.str2dict(point.get('tres_req_str', None))
               
            if point['job_state'] in ['RUNNING']: # running state
               runJidSet.add(jid)
               if tres_dict:
                  ts2ReqNodeCnt[ts] += int(tres_dict.get('node', 1))
                  ts2ReqCPUCnt[ts]  += int(tres_dict.get('cpu', point.get('num_cpus',28)))
            elif point['job_state'] in ['PENDING']: # pending state
               pendJidSet.add(jid)
               if tres_dict:
                  ts2PendReqNodeCnt[ts] += int(tres_dict.get('node', 1))
                  ts2PendReqCPUCnt[ts]  += int(tres_dict.get('cpu', point.get('num_cpus',28)))

        print("INFO: getJobRequestHistory {} points for {} running and {} pending jobs between {} and {} take time {}".format(len(points), len(runJidSet), len(pendJidSet), st, et, time.time()-t1))
        return runJidSet, ts2ReqNodeCnt, ts2ReqCPUCnt, pendJidSet, ts2PendReqNodeCnt, ts2PendReqCPUCnt
       
    # query autogen.slurm_pending and return {ts_in_sec:{state_reason:count}], ...}  
    def getPendingCount (self, st, et):
        # data before is not reliable, add tres_per_node after 06/04/3019 11:59AM
        st = max (int(st), 1550868105)

        query= "select * from autogen.slurm_pending where time >= " + str(int(st)) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("INFO: getPendingCount {}".format(query))
        #pendpoint['tags']  = MyTool.sub_dict_exist (item, ['job_id', 'state_reason'])
        #pendpoint['fields']= MyTool.sub_dict_exist (item, ['submit_time', 'user_id', 'account', 'qos', 'partition', 'tres_req_str', 'last_sched_eval', 'time_lim

        results      = self.influx_client.query(query, epoch='ms')
        points       = list(results.get_points())
        jidSet       = set([point['job_id'] for point in points])   #use set to remove duplicate ids
        #print("jidSet={}".format(jidSet))

        tsReason2jobCnt = defaultdict(lambda:defaultdict(int))
        tsState2cpuCnt  = defaultdict(lambda:defaultdict(int))
        tsPart2noPri    = defaultdict(lambda:defaultdict(list))   # sav partition-points
        tsPart2pri      = defaultdict(lambda:defaultdict(list))                       # sav Priority points
       
        for point in points:
            ts           = point['time']
            state_reason = point['state_reason']

            if state_reason and ('Resources' in state_reason):
              tres = point.get('tres_per_node','')
              if (tres and 'gpu' in tres):
                 tsReason2jobCnt[ts]['{}_GPU'.format(state_reason)] += 1
              else:
                 tsReason2jobCnt[ts][state_reason] += 1
            else:
              tsReason2jobCnt[ts][state_reason] += 1

            # Priority reduce to resources most of the time
            if state_reason and ('Priority' in state_reason):
              tsPart2pri[ts][point['partition']].append (point)
            else:
              tsPart2noPri[ts][point['partition']].append(point)  #non-priority is saved by parition
             
        for ts, part2pri in tsPart2pri.items():
            for part_name, pri_lst in part2pri.items():
                pri_min_jid     = min([point['job_id'] for point in pri_lst])
                noPri_lst       = tsPart2noPri[ts][part_name]
                pri_count       = min(10, len(pri_lst))
                if noPri_lst:
                   #the job with a max jid smaller than pri_min_jid
                   #noPri_pre_jid   = max([point['job_id'] for point in noPri_lst if point['job_id'] < pri_min_jid])
                   #noPri_pre_point = [point for point in noPri_lst if point['job_id']==noPri_pre_jid][0]
                   noPri_pre_point = max([point for point in noPri_lst if point['job_id'] < pri_min_jid], default={'state_reason':None}, key=operator.itemgetter('job_id'))
                   tsReason2jobCnt[ts][noPri_pre_point['state_reason']] += pri_count
                else:
                   tsReason2jobCnt[ts][None] += pri_count
                
        return tsReason2jobCnt, jidSet
        
    #return information of hostname
    #return all uid sequence of a node, {uid: {ts: [cpu, io, mem] ... }, ...}
    def getNodeMonData_1(self, node, start_time, stop_time):
        t1=time.time()

        #prepare query
        query= "select * from autogen.cpu_load where hostname = '" + node + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getNodeMonData_1 " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        ts2data = defaultdict(dict)
        preP    = None
        for point in points: #points are sorted by point['time']
            if not preP:
               preP   = point
            else:
               ts     = point['time']
               period = (ts - preP['time'])/1000
               if period < 1:  continue
               for key in ['cpu_times_iowait','cpu_times_system','cpu_times_user', 'disk_io_read_time','disk_io_write_time', 'disk_io_read_bytes', 'disk_io_write_bytes', 'net_io_rx_bytes', 'net_io_tx_bytes']:
                   valueDiff = point[key]-preP[key]
                   if valueDiff >= 0:
                      if key in ['disk_io_read_time','disk_io_write_time']:
                         valueDiff /= 1000
                      ts2data[ts][key] = (valueDiff/ period)
                   else: 
                      print("getNodeMonData_1 ERROR {}: negative diff value {} of {} from {} to {}".format(ts, valueDiff, key, preP[key], point[key]))
               for key in ['mem_buffers', 'mem_cached', 'mem_used']:
                   ts2data[ts][key] = point[key]
   
               preP   = point

        print("getNodeMonData_1 take time " + str(time.time()-t1))
        return ts2data

    def getUserProc(self, user, nodelist, start_time, stop_time):
        t1=time.time()

        uid       = MyTool.getUid(user)
        g =['({})'.format(n) for n in nodelist]
        hostnames = '|'.join(g)
        query= "select * from autogen.cpu_proc_info where uid = '" + str(uid) + "' and hostname=~/"+ hostnames + "/ and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getUserProc " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        query_rlt = self.influx_client.query(query, epoch='s') 
        rlt       = {}
        for node in nodelist:
            points = query_rlt.get_points(tags={'hostname':node})
            procs  = list(points)
            #time cmdline cpu_affinity cpu_system_time cpu_user_time end_time hostname io_read_bytes io_read_count io_write_bytes io_write_count mem_data   mem_lib mem_rss   mem_shared mem_text mem_vms    name    num_fds pid    ppid   status   uid 
            #{'time': 1564604764, 'cmdline': "['/mnt/home/ageorgescu/qe/qe_release_6.4/bin/pw.x', '-nk', '270', '-ndiag', '1']", 'hostname': 'worker0031', 'name': 'pw.x', 'pid': '1003095', 'ppid': 1002972, 'uid': '1144'},
            for proc in procs:
                if 'end_time' not in proc or not proc['end_time']:
                   print('WARNING: getUserProc do not have end_time {}'.format(proc)) 
                   proc['avg_util'] = -1
                   proc['avg_io']   = -1
                else:
                   period = proc['end_time']-proc['time']
                   proc['avg_util'] = (proc.get('cpu_system_time',0)+proc.get('cpu_user_time',0))/period            
                   proc['avg_io']   = (proc.get('io_write_bytes',0)+proc.get('io_write_bytes',0))/1024/period

                proc['cmdline']  = ' '.join(eval(proc['cmdline']))
                if 'mem_rss_K' not in proc:
                   if proc['mem_rss']:
                      proc['mem_rss_K'] = int(proc['mem_rss'] / 1024)
                   else:
                      proc['mem_rss_K'] = -1
                if 'mem_vms_K' not in proc:
                   if proc['mem_vms']:
                      proc['mem_vms_K'] = int(proc['mem_vms'] / 1024)
                   else:
                      proc['mem_vms_K'] = -1

            rlt[node] = procs
            
        return repr(query), rlt

    def query(self, query):
        t1=time.time()

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        try:
           query_rlt = self.influx_client.query(query, epoch='s') 
        except Exception as e:
           print("ERROR: influxdb query {} exception {}".format(query, e))
           return None

        print("INFO: influxdb query {} take time {}".format(query, time.time()-t1))
        return query_rlt

    def queryJobNodeProcess ():
        return None

def test():
    pass
 
def main():
    t1=time.time()
    start, stop = MyTool.getStartStopTS(days=3) 
    app   = InfluxQueryClient()
    start, stop, rlt   = app.getNodeJobProcData('worker1006',469406)
    #s1, e1, d1=app.getSlurmJobData(465261)
    #s2, e2, d2=app.getSlurmJobData(465262)
    #d3=app.getSlurmUidMonData(1012, ['workergpu30'], s1, e1)
    #app.getUserProc ('dhofmann', ['worker1031'], 1568554418, 1568908041)
    #rlt   = app.getSlurmJobRuntimeHistory(346864, 1566727264, 1567159264)
    print('{}'.format(rlt))
    #app.getPendingCount(start, stop)
    #app.savNodeHistory(days=7)
    #app.savJobRequestHistory(days=7)
    #app.getJobRequestHistory(start, stop)
    #app.getNodeHistory(start, stop)
    #point = app.getSlurmJobInfo('70900')
    #point = app.getSlurmJobInfo('105179')
    #print("getSlurmJobInfo result " + repr(point))
    #if point:
    #   nodelist = MyTool.convert2list(point['nodes'])
    #   start    = point['start_time']
    #   user_id  = point['user_id']
    #   if 'end_time' in point:
    #       stop     = point['end_time']
    #   print("user_id=" + user_id + ",nodelist="+repr(nodelist)+", " + repr(start) + "," + repr(stop))
    #   for ndname in nodelist:
    #       print(ndname)    
    #       app.getSlurmNodeMon(ndname, point['user_id'], point['start_time'], point['end_time'])
    #app.getSlurmUidMonData (point['user_id'], point['nodes'], point['start_time'], point['end_time'])

    #endtime = datetime.now()
    #starttime = endtime - timedelta(seconds=259200)          # 3 days=259200 seconds
 
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
   print("Total take time " + str(time.time()-t1))
