#!/usr/bin/env python

import time
t1=time.time()
import json, pwd, sys
from datetime import datetime, timezone, timedelta

import influxdb
from collections import defaultdict
import MyTool


class InfluxQueryClient:
    Interval = 61
    LOCAL_TZ = timezone(timedelta(hours=-4))
    CLIENT_INS = None

    @classmethod
    def getClientInstance (cls):
        if not cls.CLIENT_INS:
           cls.CLIENT_INS = InfluxQueryClient('scclin011','slurmdb')

        return cls.CLIENT_INS

    def __init__(self, influxServer='scclin011', dbname='slurmdb'):
        t1 = time.time()
        self.influx_client = self.connectInflux (influxServer)
        print("influx_client= " + repr(self.influx_client._baseurl))
        print("take time " + str(time.time()-t1))

    def connectInflux (self, host, port=8086, dbname='slurmdb'):
        return influxdb.InfluxDBClient(host, 8086, "yliu", "", dbname)

        
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
        print("getSlurmJobInfo take time " + str(time.time()-t1))
        return point
        
    #return nodelist's time series of the a uid, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmUidMonData(self, uid, nodelist, start_time, stop_time):
        t1=time.time()

        #prepare query
        g =('('+n+')' for n in nodelist)
        hostnames = '|'.join(g)
        query= "select * from autogen.cpu_uid_mon where uid = '" + str(uid) + "' and hostname=~/"+ hostnames + "/ and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getSlurmUidNodeMon " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        node2seq = { n:{} for n in nodelist}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_rate'),
                                   MyTool.getDictNumValue(point, 'io_write_rate'), 
                                   MyTool.getDictNumValue(point, 'mem_rss')]

        #print(repr(node2seq))
        print("getSlurmUidMonData take time " + str(time.time()-t1))
        return node2seq

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

        print ("queryUidMonData " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points() # lists of dictionaries

        print("getSlurmUidMonData_All take time " + str(time.time()-t1))
        return points

    #return time series of the a uid on all nodes, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmUidMonData_All(self, uid, start_time, stop_time):
        t1=time.time()

        #prepare query
        query= "select * from one_month.cpu_uid_mon where uid = '" + str(uid) + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getSlurmUidMonData_All " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points() # lists of dictionaries

        node2seq = {}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            if node not in node2seq: node2seq[node]={}
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_bytes')   + MyTool.getDictNumValue(point, 'io_write_bytes'), 
                                   MyTool.getDictNumValue(point, 'mem_rss')]
        #print(repr(node2seq))
        print("getSlurmUidMonData_All take time " + str(time.time()-t1))
        return node2seq

    #return all uid sequence of a node, {uid: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmNodeMonData(self, node, start_time, stop_time):
        t1=time.time()

        #prepare query
        query= "select * from autogen.cpu_uid_mon where hostname = '" + node + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getSlurmNodeMonData " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = list(results.get_points())

        uid2seq = {}
        for point in points: #points are sorted by point['time']
            ts      = point['time']
            uid     = point['uid']
            if uid not in uid2seq: uid2seq[uid] = {}
            uid2seq[uid][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                 MyTool.getDictNumValue(point, 'io_read_rate'),
                                 MyTool.getDictNumValue(point, 'io_write_rate'), 
                                 MyTool.getDictNumValue(point, 'mem_rss')]

        if len(points)>0:
           start_time = points[0]['time']/1000
           stop_time  = points[len(points)-1]['time']/1000
          
        #print(repr(uid2seq))
        print("getSlurmUidMonData take time " + str(time.time()-t1))
        return uid2seq, start_time, stop_time

    # return list [[ts, run_time] ... ]
    def getSlurmJobRuntimeHistory (self, jobid, st, et):
        t1=time.time()
        query= "select * from one_week.slurm_jobs_mon where job_id = '" + str(jobid) + "' and time >= " + str(st) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("getSlurmJobRuntimeHistory " + query)

        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()
        nodeDataDict    = {}
        for point in points:
            #print (repr(point))
            nodeDataDict[point['time']] = point['run_time']

        print("getSMon take time " + str(time.time()-t1))
     
        return nodeDataDict
    
    # query autogen.slurm_pending and return {ts_in_sec:{state_reason:count}], ...}  
    def getRunningJob_NodeCPUHistory(self, st, et):
        t1=time.time()
        
        query= "select * from autogen.slurm_job_mon where time >= " + str(int(st)) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("getJobMonHistory query {}".format(query))

        results    = self.influx_client.query(query, epoch='ms')
        points     = list(results.get_points())
        #point['tags']   = MyTool.sub_dict_remove       (item, ['job_id', 'user_id'])
        #point['fields'] = MyTool.sub_dict_exist_remove (item, ['job_state', 'num_cpus', 'num_nodes', 'state_reason', 'run_time', 'suspend_time'])
 
        jidSet     = set()   #use set to remove duplicate ids
        ts2NodeCnt = defaultdict(int)
        ts2CPUCnt  = defaultdict(int)
        for point in points:
            ts = point['time']
            if point['job_state'] in ['RUNNING']: # running state
               jidSet.add(point['job_id'])
               ts2NodeCnt[ts] += point['num_nodes']
               ts2CPUCnt[ts]  += point['num_cpus']

        print("getRunningJobHistory between {} and {} take time {}".format(st, et, time.time()-t1))
        return ts2NodeCnt, ts2CPUCnt, jidSet
       

    def getPendingCount (self, st, et):
        t1=time.time()
        
        # data before is not reliable, add tres_per_node after 06/04/3019 11:59AM
        st = max (int(st), 1550868105)

        query= "select * from autogen.slurm_pending where time >= " + str(int(st)) + "000000000 and time <= " + str(int(et)) + "000000000"
        print ("getPendingCount {}".format(query))

        results      = self.influx_client.query(query, epoch='ms')
        points       = list(results.get_points())
        jidSet       = set([point['job_id'] for point in points])   #use set to remove duplicate ids
        #print("jidSet={}".format(jidSet))

        tsReason2jobCnt = defaultdict(lambda:defaultdict(int))
        tsState2cpuCnt = defaultdict(lambda:defaultdict(int))
        for point in points:
            ts = point['time']
            state_reason = point['state_reason']
            if (state_reason == 'Resources'):
              tres = point.get('tres_per_node','')
              if (tres and 'gpu' in tres):
                 tsReason2jobCnt[ts]['Resources_GPU'] += 1
              else:
                 tsReason2jobCnt[ts][state_reason] += 1
            else:
              tsReason2jobCnt[ts][state_reason] += 1
            
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

def main():
    t1=time.time()
    app   = InfluxQueryClient()
    #point = app.getSlurmJobInfo('70900')
    point = app.getSlurmJobInfo('105179')
    print("getSlurmJobInfo result " + repr(point))
    if point:
       nodelist = MyTool.convert2list(point['nodes'])
       start    = point['start_time']
       user_id  = point['user_id']
       if 'end_time' in point:
           stop     = point['end_time']
       print("user_id=" + user_id + ",nodelist="+repr(nodelist)+", " + repr(start) + "," + repr(stop))
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
