#!/usr/bin/env python
import argparse
import datetime
import operator
import time
import MyTool

from collections import defaultdict
from functools   import reduce
from queryBright import BrightRestClient
from querySlurm  import PyslurmQuery
from queryInflux import InfluxQueryClient

def display_user_CPU(user_name):
    uid  = MyTool.getUid (user)
    if not uid:
       print ("{} User {} does not exist.".format(MyTool.getTsString(ts), user_name))
    user_job_lst = PyslurmQuery.getUserCurrJobs(uid)
    if not user_job_lst:
       print("{} User {} does not have running jobs.".format(MyTool.getTsString(ts), user_name))
       return
    node_dict    = PyslurmQuery.getAllNodes ()

    start        = min([job['start_time'] for job in user_job_lst])
    stop         = int(time.time())
    influxClient = InfluxQueryClient()
    node2seq     = influxClient.getSlurmUidMonData_All(uid, start)
  
    print("User {} has running jobs {} with earliest start time at {}".format(user_name, [job['job_id'] for job in user_job_lst], MyTool.getTsString(start)))
    print("\t{:>10} {:>10} {:>10} {:>10} {:>10}".format('Node', 'AvgCPU', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    for node, seqDict in node2seq.items():  #TODO: seq avg need to calculate considering period
        cpuSeq = [info[0] for ts,info in seqDict.items()]
        cpuAvg = sum(cpuSeq) / (stop-start)
        memSeq = [info[1] for ts,info in seqDict.items()]
        memAvg = sum(memSeq) / (stop-start)
        io_rSeq = [info[2] for ts,info in seqDict.items()]
        io_rAvg = sum(io_rSeq) / (stop-start)
        io_wSeq = [info[3] for ts,info in seqDict.items()]
        io_wAvg = sum(io_wSeq) / (stop-start)
        print("\t{:>10} {:>10.2f} {:>10.2f} {:>10.2f} {:>10.2f}".format(node, cpuAvg, memAvg, io_rAvg, io_wAvg))
    return

        
def display_job_CPU(jid):
    jid  = int(jid)
    ts   = int(time.time())
    job  = PyslurmQuery.getCurrJob (jid)
    if not job:
       print ("\t{} Job {} does not exist or already stops running.".format(MyTool.getTsString(ts), jid))

    influxClient          = InfluxQueryClient()
    start, stop, node2seq = influxClient.getSlurmJobData(jid)  #{hostname: {ts: [cpu, mem, io_r, io_w] ... }}
    if not node2seq:
       print ("Cannot find Job {} in Influx Server".format(jid))
       return
    print ("Job {} run from {} to {}".format(jid, MyTool.getTsString(start), MyTool.getTsString(stop)))
   
    print("\t{:>10} {:>10} {:>10} {:>10} {:>10}".format('Node', 'AvgCPU', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    for node, seqDict in node2seq.items():
        cpuSeq = [info[0] for ts,info in seqDict.items()]
        cpuAvg = sum(cpuSeq) / (stop-start)
        memSeq = [info[1] for ts,info in seqDict.items()]
        memAvg = sum(memSeq) / (stop-start)
        io_rSeq = [info[2] for ts,info in seqDict.items()]
        io_rAvg = sum(io_rSeq) / (stop-start)
        io_wSeq = [info[3] for ts,info in seqDict.items()]
        io_wAvg = sum(io_wSeq) / (stop-start)
        print("\t{:>10} {:>10.2f} {:>10.2f} {:>10.2f} {:>10.2f}".format(node, cpuAvg, memAvg, io_rAvg, io_wAvg))
    return

def display_node_CPU(node_name):
    ts   = int(time.time())
    node = PyslurmQuery.getNode (node_name)
    if not node:
       print ("{} Node {} does not exist.".format(MyTool.getTsString(ts), node_name))
       return

    jobs               = PyslurmQuery.getNodeAllocJobs (node_name, node)
    t1                 = min([job['start_time'] for job in jobs])
    influxClient       = InfluxQueryClient()
    uid2seq,start,stop = influxClient.getSlurmNodeMonData(node_name,t1)

    if not uid2seq:
       print("Node {} does not have running jobs.")
       return
    print("Node {} has running jobs {} with earliest start time at {}".format(node_name, [job['job_id'] for job in jobs], MyTool.getTsString(t1)))
    print("\t{:>10} {:>20} {:>20} {:>10} {:>10} {:>10} {:>10}".format('User', 'Start', 'Stop', 'AvgCPU', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    for uid, seqDict in uid2seq.items():  #TODO: seq avg need to calculate considering period
        ts_seq = [ts for ts in seqDict.keys()]
        cpuSeq = [info[0] for ts,info in seqDict.items()]
        cpuAvg = sum(cpuSeq) / (stop-start)
        memSeq = [info[1] for ts,info in seqDict.items()]
        memAvg = sum(memSeq) / (stop-start)
        io_rSeq = [info[2] for ts,info in seqDict.items()]
        io_rAvg = sum(io_rSeq) / (stop-start)
        io_wSeq = [info[3] for ts,info in seqDict.items()]
        io_wAvg = sum(io_wSeq) / (stop-start)
        print("\t{:>10} {:>20} {:>20} {:>10.2f} {:>10.2f} {:>10.2f} {:>10.2f}".format(MyTool.getUser(uid), MyTool.getTsString(min(ts_seq)), MyTool.getTsString(max(ts_seq)), cpuAvg, memAvg, io_rAvg, io_wAvg))
    return


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Collect GPU utilization data')
    parser.add_argument('-u', '--user', metavar='USER',        nargs="+", help='user name')
    parser.add_argument('-j', '--jid',  metavar='JOBID', type=int, nargs="+", help='job id')
    parser.add_argument('-n', '--node', metavar='workerNode',     nargs="+", help='node')
    #parser.add_argument('-t', '--hour', metavar='N', type=int, default=24, help='hours (default=24), used with node option')
    args = parser.parse_args()
    #print(args)
 
    if args.user:
       for user in args.user:
          display_user_CPU(user)
    if args.jid:
       for jid in args.jid:
          display_job_CPU(jid)
    if args.node:
       for node in args.node:
          display_node_CPU(node)
