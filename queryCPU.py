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
       return

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
       #TODO: get job from sacct 

    influxClient          = InfluxQueryClient()
    start, stop, node2seq = influxClient.getSlurmJobData(jid)  #{hostname: {ts: [cpu, mem, io_r, io_w] ... }}
    if not node2seq:
       print ("Cannot find Job {} in Influx Server".format(jid))
       return
    if job:
       print ("{} Job {} of {} running {}, allocate {} nodes {}.".format(MyTool.getTsString(ts), jid, MyTool.getUser(job['user_id']), datetime.timedelta(seconds=ts - job['start_time']), len(job['cpus_allocated']), list(job['cpus_allocated'])))
    else:
       print ("{} Job {} ({} to {}). ".format(MyTool.getTsString(ts), jid, MyTool.getTsString(start), MyTool.getTsString(stop)))
   
    fmt_str  ="    {:>11} {:>8} {:>24} {:>10} {:>10} {:>10}"
    dfmt_str ="    {:>11} {:>8} {:>5.2f}({:>5.2f},{:>5.2f},{:>5.2f}) {:>10} {:>10} {:>10}"
    print(fmt_str.format('Node', 'AllocCPU', 'AvgCPU(10,30,60min)', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    print(fmt_str.format('-'*10, '-'*8, '-'*24, '-'*10, '-'*10, '-'*10))
    for node, seqDict in node2seq.items():
        cpuSeq   = [[ts,info[0]] for ts,info in seqDict.items()]
        cpuAvg   = MyTool.getTimeSeqAvg(cpuSeq, start, stop)
        cpuAvg1  = MyTool.getTimeSeqAvg(cpuSeq, stop-60*10, stop)
        cpuAvg2  = MyTool.getTimeSeqAvg(cpuSeq, stop-60*30, stop)
        cpuAvg3  = MyTool.getTimeSeqAvg(cpuSeq, stop-60*60, stop)

        memSeq   = [[ts,info[1]] for ts,info in seqDict.items()]
        memAvg   = MyTool.getTimeSeqAvg(memSeq, start, stop)
        io_rSeq  = [[ts,info[2]] for ts,info in seqDict.items()]
        io_rAvg  = MyTool.getTimeSeqAvg(io_rSeq, start, stop)
        io_wSeq  = [[ts,info[3]] for ts,info in seqDict.items()]
        io_wAvg  = MyTool.getTimeSeqAvg(io_wSeq, start, stop)
        if job:
           alloc_cpu = job['cpus_allocated'].get(node, '')
        else:
           alloc_cpu = ''
        print(dfmt_str.format(node, alloc_cpu, cpuAvg,cpuAvg1,cpuAvg2,cpuAvg3, MyTool.getDisplayKB(memAvg), MyTool.getDisplayBps(io_rAvg), MyTool.getDisplayBps(io_wAvg)))
    return

#display from the latest job start point or last hours, which one is earlier
def display_node_CPU(node_name, hours=2):
    ts   = int(time.time())
    node = PyslurmQuery.getNode (node_name)
    if not node:
       print ("{} Node {} does not exist.".format(MyTool.getTsString(ts), node_name))
       return

    jobs               = PyslurmQuery.getNodeAllocJobs (node_name, node)
    if not jobs:
       print("{}: Node {} up {},\t{} cores, no running jobs.".format(
              MyTool.getTsString(ts), 
              node_name, 
              datetime.timedelta(seconds=ts - node['boot_time']), 
              node['cores']))
       return
    users              = [MyTool.getUser(uid) for uid in set([job['user_id'] for job in jobs])]
    t1                 = max([job['start_time'] for job in jobs])
    t1                 = min(t1, ts-hours*60*60)
    influxClient       = InfluxQueryClient()
    uid2seq,start,stop = influxClient.getSlurmNodeMonData(node_name,t1)

    if not uid2seq:
       print("Node {} does not have running jobs's data.")
       return
    print("{}: Node {} up {}, {} cores, {} users {} running {} jobs {} currently.".format(
              MyTool.getTsString(ts), 
              node_name, 
              datetime.timedelta(seconds=ts - node['boot_time']), 
              node['cores'],
              len(users), users,
              len(jobs), [job['job_id'] for job in jobs]))
    fmt_str  = "    {:>12} {:>17} {:>24} {:>10} {:>10} {:>10}"
    dfmt_str = "    {:>12} {:>17} {:>5.2f}({:>5.2f},{:>5.2f},{:>5.2f}) {:>10} {:>10} {:>10}"
    print(fmt_str.format('User', 'TimePeriod', 'AvgCPU(10,30,60min)', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    print(fmt_str.format('-'*12, '-'*17, '-'*24, '-'*10, '-'*10, '-'*10))
    for uid, seqDict in uid2seq.items():  #TODO: seq avg need to calculate considering period
        start_ts = min([ts for ts in seqDict.keys()])
        cpuSeq   = [[ts,info[0]] for ts,info in seqDict.items()]
        cpuAvg   = MyTool.getTimeSeqAvg(cpuSeq, start_ts, ts)
        cpuAvg1  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*10, ts)
        cpuAvg2  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*30, ts)
        cpuAvg3  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*60, ts)

        memSeq   = [[ts,info[1]] for ts,info in seqDict.items()]
        memAvg   = MyTool.getTimeSeqAvg(memSeq, start_ts, ts)
        io_rSeq  = [[ts,info[2]] for ts,info in seqDict.items()]
        io_rAvg  = MyTool.getTimeSeqAvg(io_rSeq, start_ts, ts)
        io_wSeq  = [[ts,info[3]] for ts,info in seqDict.items()]
        io_wAvg  = MyTool.getTimeSeqAvg(io_wSeq, start_ts, ts)
        print(dfmt_str.format(MyTool.getUser(uid), str(datetime.timedelta(seconds=ts - start_ts)), cpuAvg, cpuAvg1,cpuAvg2,cpuAvg3,MyTool.getDisplayKB(memAvg), MyTool.getDisplayBps(io_rAvg), MyTool.getDisplayBps(io_wAvg)))
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
