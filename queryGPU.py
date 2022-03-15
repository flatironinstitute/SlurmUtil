#!/usr/bin/env python
import argparse
import datetime
import operator
import time
import MyTool

from collections import defaultdict
from functools   import reduce
from queryBright import BrightRestClient
from queryPyslurm  import PyslurmQuery

BRIGHT_URL = "https://rusty-mgr1:8081/rest/v1/monitoring/"
def display_user_GPU(user_name):
    ts   = int(time.time())
    uid  = MyTool.getUid (user)
    if not uid:
       print ("{} User {} does not exist.".format(MyTool.getTsString(ts), user_name))
    user_job_lst = PyslurmQuery.getUserCurrJobs(uid)
    if not user_job_lst:
       print("{} User {} does not have running jobs.".format(MyTool.getTsString(ts), user_name))
       return
    node_dict    = PyslurmQuery.getAllNodes ()
    job_gpu_d    = dict([(job['job_id'], PyslurmQuery.getJobAllocGPU(job, node_dict)) for job in user_job_lst])

    u_node       = [node_name for g_alloc_d in job_gpu_d.values() for node_name in g_alloc_d]
    u_gpu_cnt    = sum([len(g_lst) for g_alloc_d in job_gpu_d.values() for g_lst in g_alloc_d.values()])
    g_union      = reduce(lambda rlt, curr: rlt.union(set(curr)), [g_lst for g_alloc_d in job_gpu_d.values() for g_lst in g_alloc_d.values()], set())
    print("{} User {} has {} running jobs,\talloc {} GPUs on {} GPU nodes.".format(MyTool.getTsString(ts), user_name, len(user_job_lst), u_gpu_cnt, len(u_node)))
    #get gpu data
    if u_node:   #GPU nodes allocated
       gpu_data  = BrightRestClient(BRIGHT_URL).getGPU (u_node, min([job['start_time'] for job in user_job_lst if job_gpu_d[job['job_id']]]), list(g_union),msec=False)
    else:
       gpu_data  = {}
    print("\t{:10}{:20}{:>16}{:>20}{:>25}".format("Jid", "Job run time", "Node.GPU", "Job avg util", "Avg util (5,10,30min)"))
    for job in user_job_lst:
       jid        = job['job_id']
       j_run_time = str(datetime.timedelta(seconds=ts - job['start_time']))
       j_first_ln = True
       if not job_gpu_d[jid]:  # job not using GPU
           print("\t{:<10}{:20}{:>16}".format(job['job_id'], j_run_time, 'No GPU'))
           continue
       for node, g_lst in job_gpu_d[jid].items():
           for g in g_lst:
               g_name = '{}.gpu{}'.format(node,g)
               g_data = gpu_data[g_name]
               g_avg  = MyTool.getTimeSeqAvg(g_data, job['start_time'], ts)
               g_avg1 = MyTool.getTimeSeqAvg(g_data, ts-5*60,           ts)
               g_avg2 = MyTool.getTimeSeqAvg(g_data, ts-10*60,          ts)
               g_avg3 = MyTool.getTimeSeqAvg(g_data, ts-30*60,          ts)
               if j_first_ln:
                  print("\t{:<10}{:20}{:>16}{:>20.2f}{:>10.2f},{:>6.2f},{:>6.2f}".format(jid, j_run_time, g_name, g_avg*100, g_avg1*100, g_avg2*100, g_avg3*100))
                  j_first_ln = False
               else:
                  print("\t{:<10}{:20}{:>16}{:>20.2f}{:>10.2f},{:>6.2f},{:>6.2f}".format('', '', g_name, g_avg*100, g_avg1*100, g_avg2*100, g_avg3*100))
        
def display_job_GPU(jid):
    ts   = int(time.time())
    job  = PyslurmQuery.getCurrJob (jid)
    if not job:
       print ("{} Job {} does not exist or already stops running.".format(MyTool.getTsString(ts), jid))
       return
    j_gpu = PyslurmQuery.getJobAllocGPU(job)
    #print(j_gpu)
    if not j_gpu:
       print ("{} Job {} does not allocate any GPU.".format(MyTool.getTsString(ts), jid))
       return

    print("{} Job {} of {} run for {},\talloc {} GPUs on {} GPU nodes.".format(MyTool.getTsString(ts), jid, MyTool.getUser(job['user_id']), datetime.timedelta(seconds=ts - job['start_time']), sum([len(g_lst) for g_lst in j_gpu.values()]), sum([1 for g_lst in j_gpu.values() if g_lst]))) 
    gpu_union     = reduce(lambda rlt, curr: rlt.union(set(curr)), j_gpu.values(), set())
    #print(gpu_union)
    gpu_data      = BrightRestClient(BRIGHT_URL).getGPU (list(j_gpu.keys()), job['start_time'], list(gpu_union),msec=False)
    #print(gpu_data.keys()) 
    print("\t{:12}{:>6}{:>20}{:>25}".format("Node", "GPU", "Job avg util", "Avg util (5,10,30min)"))
    for node_name,gpu_list in j_gpu.items():
        for gid in gpu_list:
            g_data   = gpu_data['{}.gpu{}'.format(node_name,gid)]
            g_avg    = MyTool.getTimeSeqAvg(g_data, job['start_time'], ts)
            g_avg1   = MyTool.getTimeSeqAvg(g_data, ts-5*60,           ts)
            g_avg2   = MyTool.getTimeSeqAvg(g_data, ts-10*60,          ts)
            g_avg3   = MyTool.getTimeSeqAvg(g_data, ts-30*60,          ts)
            print("\t{:12}{:6}{:>20.2f}{:>10.2f},{:>6.2f},{:>6.2f}".format(node_name, gid, g_avg*100, g_avg1*100, g_avg2*100, g_avg3*100))
    return

def display_node_GPU(node_name):
    ts   = int(time.time())
    node = PyslurmQuery.getNode (node_name)
    if not node:
       print ("{} Node {} does not exist.".format(MyTool.getTsString(ts), node_name))
       return
    if 'gpu' not in node['features']:
       print ("{} Node {} does not have GPUs.".format(MyTool.getTsString(ts), node_name))
       return

    jobs = PyslurmQuery.getNodeAllocJobs (node_name, node)
    gpu_total, gpu_used = MyTool.getGPUCount(node['gres'], node['gres_used'])
    print("{}: Node {} up {},\t{} GPUs ({} used), {} allocated jobs.".format(MyTool.getTsString(ts), node_name, datetime.timedelta(seconds=ts - node['boot_time']), gpu_total, gpu_used, len(jobs) if jobs else 0)) 
    
    jid2gpu = dict(map(lambda job: (job['job_id'], PyslurmQuery.getJobAllocGPUonNode(job, node)), jobs))
    if jid2gpu:
       job_gpu     = reduce(operator.add, jid2gpu.values())
       start_ts    = min([job['start_time'] for job in jobs if job['gres_detail']])
       gpu_data    = BrightRestClient(BRIGHT_URL).getNodeGPU (node_name, start_ts, job_gpu, msec=False)
    else:
       gpu_data    = {}

    jid2job = dict(map(lambda job: (job['job_id'], job), jobs))
    gid2jid = defaultdict(list)
    for jid, gpu_list in jid2gpu.items():
        for gid in gpu_list:
            gid2jid[gid].append(jid)
    print("\t{:6}{:10}{:>20}{:>20}{:>25}".format("GPU", "Jid", "Job run time", "Job avg util", "Avg util (5,10,30min)"))
    for gid in range(0, gpu_total):
        jid_list = gid2jid[gid]
        if jid_list:
           start_ts = min([jid2job[jid]['start_time'] for jid in jid_list])
           g_data   = gpu_data['{}.gpu{}'.format(node_name,gid)]
           g_avg    = MyTool.getTimeSeqAvg(g_data, start_ts, ts)
           g_avg1   = MyTool.getTimeSeqAvg(g_data, ts-5*60,  ts)
           g_avg2   = MyTool.getTimeSeqAvg(g_data, ts-10*60,  ts)
           g_avg3   = MyTool.getTimeSeqAvg(g_data, ts-30*60,  ts)
           print("\t{:<6}{:10}{:>20}{:>20.2f}{:>10.2f},{:>6.2f},{:>6.2f}".format(gid, str(jid_list), str(datetime.timedelta(seconds=ts - start_ts)), g_avg*100, g_avg1*100, g_avg2*100, g_avg3*100))
        else:
           print("\t{:<6}{:10}".format(gid, "IDLE"))
        
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
          display_user_GPU(user)
    if args.jid:
       for jid in args.jid:
          display_job_GPU(jid)
    if args.node:
       for node in args.node:
          display_node_GPU(node)
