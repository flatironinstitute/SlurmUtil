#!/usr/bin/env python

import time
t1=time.time()
import json, pwd, sys
from datetime import datetime, timezone, timedelta

import influxdb
import collections
import MyTool

class InfluxQueryClient:
    Interval = 61
    LOCAL_TZ = timezone(timedelta(hours=-4))

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
        
    def getSlurmNodeMon (self, hostname, uid, start_time, end_time):
        t1=time.time()
        st=int(start_time)
        et=int(end_time)

        query= "select * from one_week.cpu_proc_mon where hostname='" + hostname + "' and uid = '" + uid + "' and time >= " + str(st) + "000000000 and time <= " + str(et) + "000000000"
        print ("getSlurmNodeMon " + query)
        #returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='s')
        points  = results.get_points()

        #combine the result
        #for each ts, add up the cpu, io and mem {ts: [cpu, .., io, .. mem] ... } over pids
        nodeDataDict = {}
        pids = {}  #pid: pidcount
        for point in points:
            #print (repr(point))
            #MyTool.sub_dict(point, ['cpu_system_time', 'cpu_user_time', 'io_read_bytes', 'io_write_bytes', 'mem_data', 'mem_rss', 'mem_shared', 'mem_text', 'mem_vms', 'num_fds'])
            if point['time'] not in nodeDataDict: nodeDataDict[point['time']] = {'cpu_time':0, 'io_bytes':0, 'mem_rss':0}
            nodeDataDict[point['time']]['cpu_time'] += (MyTool.getDictNumValue(point, 'cpu_system_time') + MyTool.getDictNumValue(point, 'cpu_user_time'))

            nodeDataDict[point['time']]['io_bytes'] += (MyTool.getDictNumValue(point, 'io_read_bytes')   + MyTool.getDictNumValue(point, 'io_write_bytes'))
            nodeDataDict[point['time']]['mem_rss']  += MyTool.getDictNumValue(point, 'mem_rss') 
            if point['pid'] not in pids: pids[point['pid']]=0
            pids[point['pid']] += 1
         
        #print ("nodeDataDict size" + str(len(nodeDataDict)))
        #for pid, count in pids.items():
        #    print (str(pid) + " size " + str(count))
        preTs = None
        for ts, v in nodeDataDict.items():
            if preTs:
               if v['cpu_time'] < preV['cpu_time']:
                  #two possiblities 1. there is a lower value in between, 2. missing/incorrect measurements 
                  #v['cpu_perc'] = (v['cpu_time']) / (ts - preTs)
                  v['cpu_perc'] = 0
               else:
                  v['cpu_perc'] = (v['cpu_time'] - preV['cpu_time']) / (ts - preTs)
               #if v['cpu_perc'] > 1: v['cpu_perc']=1
               
            preTs = ts
            preV  = v
            
        #print(hostname + ":" + repr(nodeDataDict))
        print("getSlurmNodeMon take time " + str(time.time()-t1))

    #return [[...],[...],[...]], each of which
    #[{'node':nodename, 'data':[[timestamp, value]...]} ...]
    def getSlurmJobMonData(self, jid, uid, nodelist, start_time, stop_time):
        t1=time.time()
        st=int(start_time)
        et=int(stop_time)
        g =('('+n+')' for n in nodelist)
        hostnames = '|'.join(g)

        query= "select * from one_week.cpu_proc_mon where uid = '" + str(uid) + "' and hostname=~/"+ hostnames + "/ and time >= " + str(st) + "000000000 and time <= " + str(et) + "000000000"
        print ("getSlurmJobNodeMon " + query)
        #returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        #for each nodename and ts, add up the cpu, io and mem {hostname: {ts: [cpu, io, mem] ... }}: over pids
        nodeDataDict = dict([(node, {}) for node in nodelist])
        for point in points:
            #print (repr(point))
            ts       = point['time']
            nodeData = nodeDataDict [point['hostname']]
               
            if ts not in nodeData: nodeData[ts] = [0,0,0]
            nodeData[ts][0] += (MyTool.getDictNumValue(point, 'cpu_system_time') + MyTool.getDictNumValue(point, 'cpu_user_time'))
            nodeData[ts][1] += (MyTool.getDictNumValue(point, 'io_read_bytes')   + MyTool.getDictNumValue(point, 'io_write_bytes'))
            nodeData[ts][2] +=  MyTool.getDictNumValue(point, 'mem_rss') 
         
        #print(nodeDataDict)
        print("getSMon take time " + str(time.time()-t1))
     
        return nodeDataDict

    #return nodelist's time series of the a uid, {hostname: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmUidMonData(self, uid, nodelist, start_time, stop_time):
        t1=time.time()

        #prepare query
        g =('('+n+')' for n in nodelist)
        hostnames = '|'.join(g)
        query= "select * from one_month.cpu_uid_mon where uid = '" + str(uid) + "' and hostname=~/"+ hostnames + "/ and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getSlurmUidNodeMon " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        node2seq = { n:{} for n in nodelist}
        for point in points: #points are sorted by point['time']
            ts       = point['time']
            node     = point['hostname']
            node2seq[node][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                   MyTool.getDictNumValue(point, 'io_read_bytes')   + MyTool.getDictNumValue(point, 'io_write_bytes'), 
                                   MyTool.getDictNumValue(point, 'mem_rss')]

        #print(repr(node2seq))
        print("getSlurmUidMonData take time " + str(time.time()-t1))
        return node2seq

    #return all uid sequence of a node, {uid: {ts: [cpu, io, mem] ... }, ...}
    def getSlurmNodeMonData(self, node, start_time, stop_time):
        t1=time.time()

        #prepare query
        query= "select * from one_month.cpu_uid_mon where hostname = '" + node + "' and time >= " + str(int(start_time)) + "000000000 and time <= " + str(int(stop_time)+1) + "000000000"
        print ("getSlurmNodeMonData " + query)

        #execute query, returned time is local timestamp, epoch is for returned result, not for query
        results = self.influx_client.query(query, epoch='ms')
        points  = results.get_points()

        uid2seq = {}
        for point in points: #points are sorted by point['time']
            ts      = point['time']
            uid     = point['uid']
            if uid not in uid2seq: uid2seq[uid] = {}
            uid2seq[uid][ts] = [ MyTool.getDictNumValue(point, 'cpu_system_util') + MyTool.getDictNumValue(point, 'cpu_user_util'), 
                                 MyTool.getDictNumValue(point, 'io_read_bytes')   + MyTool.getDictNumValue(point, 'io_write_bytes'), 
                                 MyTool.getDictNumValue(point, 'mem_rss')]

        #print(repr(uid2seq))
        print("getSlurmUidMonData take time " + str(time.time()-t1))
        return uid2seq

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
        
#uidDict = {uid: {'cpu_system_util':0, ...}, ...}
def createUidMonPoint (utcTS, node, uidDict):
    points = []
    for uid, fields in uidDict.items():
        point           = {'measurement':'cpu_uid_mon', 'time':utcTS, 'tags':{'uid':uid, 'hostname':node}}
        point['fields'] = fields
     
        points.append (point)

    return points

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
