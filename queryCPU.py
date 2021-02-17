#!/usr/bin/env python
import argparse
import datetime
import operator, numpy, pandas
import time
import MyTool

from collections import defaultdict
from functools   import reduce
from queryBright import BrightRestClient
from querySlurm  import PyslurmQuery
from queryInflux import InfluxQueryClient

def display_user_CPU(user_name, hours=2, detail=False):
    ts    = int(time.time())
    uid   = MyTool.getUid (user)
    if not uid:
       print ("{}: User {} does not exist.".format(MyTool.getTsString(ts), user_name))
       return

    user_job_lst = PyslurmQuery.getUserRunningJobs(uid)
    if not user_job_lst:
       print("{}: User {} does not have running jobs.".format(MyTool.getTsString(ts), user_name))
       return

    t1           = min([job['start_time'] for job in user_job_lst])
    print("{}: User {} has {} running jobs {} whose earliest start time is {}".format(MyTool.getTsString(ts), user_name, len(user_job_lst), [job['job_id'] for job in user_job_lst], MyTool.getTsString(t1)))
    if hours:
       t1        = ts - hours*60*60
    influxClient = InfluxQueryClient()
    node2seq     = influxClient.getSlurmUidMonData_All(uid, t1)
    if not node2seq:
       print(" "*21 + "Server does not have data.")
       return

    fmt_str  = "    {:>12} {:>17} {:>17} {:>24} {:>10} {:>10} {:>10}"
    dfmt_str = "    {:>12} {:>17} {:>17} {:>5.2f}({:>5.2f},{:>5.2f},{:>5.2f}) {:>10} {:>10} {:>10}"
    print(fmt_str.format('Node', 'StartTime', 'TimePeriod', 'AvgCPU(10,30,60min)', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    print(fmt_str.format('-'*12, '-'*17,      '-'*17, '-'*24, '-'*10, '-'*10, '-'*10))
    for node, seqDict in sorted(node2seq.items()):  
        tsSeq    = [ts for ts in seqDict.keys()]
        start_ts = min(tsSeq)
        stop_ts  = max(tsSeq)
        cpuSeq   = [[ts,info[0]] for ts,info in seqDict.items()]
        cpuAvg   = MyTool.getTimeSeqAvg(cpuSeq, start_ts, stop_ts)
        cpuAvg1  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*10, ts)
        cpuAvg2  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*30, ts)
        cpuAvg3  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*60, ts)

        memSeq   = [[ts,info[1]] for ts,info in seqDict.items()]
        io_rSeq  = [[ts,info[2]] for ts,info in seqDict.items()]
        io_wSeq  = [[ts,info[3]] for ts,info in seqDict.items()]
        memAvg   = MyTool.getTimeSeqAvg(memSeq,  start_ts, stop_ts)
        io_rAvg  = MyTool.getTimeSeqAvg(io_rSeq, start_ts, stop_ts)
        io_wAvg  = MyTool.getTimeSeqAvg(io_wSeq, start_ts, stop_ts)
        
        print(dfmt_str.format(node, MyTool.getTS_strftime(start_ts,'%b-%d %H:%M:%S'), str(datetime.timedelta(seconds=stop_ts-start_ts)), cpuAvg, cpuAvg1,cpuAvg2,cpuAvg3,MyTool.getDisplayKB(memAvg), MyTool.getDisplayBps(io_rAvg), MyTool.getDisplayBps(io_wAvg)))

    if detail:
        display_CPU_detail(node2seq)
    return

def display_job_CPU(jid, detail=False):
    jid  = int(jid)
    ts   = int(time.time())
    job  = PyslurmQuery.getCurrJob (jid)
    if not job:
       print ("\t{}: Job {} does not exist or already stops running.".format(MyTool.getTsString(ts), jid))
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

    if detail:
       display_CPU_detail(node2seq)
    return

def display_CPU_detail(id2seq, uid=False):
    d_str = "CPU Details:"
    print ("\n{}\n{}".format(d_str, '='*len(d_str)))
    all_df = pandas.DataFrame()
    for myid, seqDict in id2seq.items():
        if uid:
           myid = MyTool.getUser(myid)
        cpu_str = "{}".format(myid)
        df = pandas.DataFrame(seqDict, index=[cpu_str, 'Mem', 'IOR', 'IOW']).transpose()[[cpu_str]]
        all_df = all_df.join(df, how='outer')
    all_df.index = pandas.to_datetime(all_df.index, unit='s')
    all_df.index = all_df.index.tz_localize('UTC')
    all_df.index = all_df.index.tz_convert('EST')
    all_df       = all_df.replace(numpy.nan, '', regex=True)
    pandas.set_option("display.max_rows", None, "display.max_columns", None)
    print(all_df)
    return

#display from the latest job start point or last hours, which one is earlier
def display_node_CPU(node_name, hours=2, detail=False):
    ts    = int(time.time())
    node  = PyslurmQuery.getNode (node_name)
    if not node:
       print ("{}: Node {} does not exist.".format(MyTool.getTsString(ts), node_name))
       return

    n_str = "{}: Node {} up {}, {} cores".format(
              MyTool.getTsString(ts),
              node_name,
              datetime.timedelta(seconds=ts - node['boot_time']),
              node['cores'])
    jobs  = PyslurmQuery.getNodeAllocJobs (node_name, node)
    users = [MyTool.getUser(uid) for uid in set([job['user_id'] for job in jobs])]
    if jobs:
       j_str = "{} users {} running {} jobs {} currently".format(len(users), users, len(jobs), [job['job_id'] for job in jobs])
    else:
       j_str = "no running jobs currently"
    print("{}, {}.".format(n_str, j_str))
    if not jobs and not hours:
       return
    if hours:
       t1          = ts - hours*60*60
    else:
       t1          = min([job['start_time'] for job in jobs]) 
    influxClient   = InfluxQueryClient()
    uid2seq,s1,s2  = influxClient.getSlurmNodeMonData(node_name,t1)

    if not uid2seq:
       print(" "*21 + "Server does not have data of the node for last {} hours.".format(hours))
       return
    fmt_str  = "    {:>12} {:>17} {:>17} {:>24} {:>10} {:>10} {:>10}"
    dfmt_str = "    {:>12} {:>17} {:>17} {:>5.2f}({:>5.2f},{:>5.2f},{:>5.2f}) {:>10} {:>10} {:>10}"
    print(fmt_str.format('User', 'StartTime', 'TimePeriod', 'AvgCPU(10,30,60min)', 'AvgMem', 'AvgIORead', 'AvgIOWrite'))
    print(fmt_str.format('-'*12, '-'*17,      '-'*17, '-'*24, '-'*10, '-'*10, '-'*10))
    for uid, seqDict in uid2seq.items():  #TODO: seq avg need to calculate considering period
        tsSeq    = [ts for ts in seqDict.keys()]
        start_ts = min(tsSeq)
        stop_ts  = max(tsSeq)
        cpuSeq   = [[ts,info[0]] for ts,info in seqDict.items()]
        cpuAvg   = MyTool.getTimeSeqAvg(cpuSeq, start_ts, stop_ts)
        cpuAvg1  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*10, ts)
        cpuAvg2  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*30, ts)
        cpuAvg3  = MyTool.getTimeSeqAvg(cpuSeq, ts-60*60, ts)

        memSeq   = [[ts,info[1]] for ts,info in seqDict.items()]
        io_rSeq  = [[ts,info[2]] for ts,info in seqDict.items()]
        io_wSeq  = [[ts,info[3]] for ts,info in seqDict.items()]
        memAvg   = MyTool.getTimeSeqAvg(memSeq,  start_ts, stop_ts)
        io_rAvg  = MyTool.getTimeSeqAvg(io_rSeq, start_ts, stop_ts)
        io_wAvg  = MyTool.getTimeSeqAvg(io_wSeq, start_ts, stop_ts)
        print(dfmt_str.format(MyTool.getUser(uid), MyTool.getTS_strftime(start_ts,'%b-%d %H:%M:%S'), str(datetime.timedelta(seconds=stop_ts-start_ts)), cpuAvg, cpuAvg1,cpuAvg2,cpuAvg3,MyTool.getDisplayKB(memAvg), MyTool.getDisplayBps(io_rAvg), MyTool.getDisplayBps(io_wAvg)))
    if detail:
        display_CPU_detail(uid2seq, uid=True)
    return

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Collect CPU and other resources (mem, io) utilization data')
    parser.add_argument('-j', '--jid',  metavar='JOBID', type=int, nargs="+", help='job id')
    parser.add_argument('-n', '--node', metavar='workerNode',     nargs="+", help='node')
    parser.add_argument('-u', '--user', metavar='USER',        nargs="+", help='user name')
    parser.add_argument('-t', '--hours',  metavar='N', type=int, default=0, help='retrieve last N hours of node and user history data. N=0 means to retrieve data from the earliest start time of current running jobs.')
    parser.add_argument('-d', '--detail', default=False, action='store_true', help='list detailed data')
    #parser.add_argument('-t', '--hour', metavar='N', type=int, default=24, help='hours (default=24), used with node option')
    args = parser.parse_args()
    #print(args)
 
    if args.user:
       for user in args.user:
          display_user_CPU(user, args.hours, args.detail)
    if args.jid:
       for jid in args.jid:
          display_job_CPU(jid, args.detail)
    if args.node:
       for node in args.node:
          display_node_CPU(node, args.hours, args.detail)
