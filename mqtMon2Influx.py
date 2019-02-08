#!/usr/bin/env python

import _pickle as cPickle
import urllib.request as urllib2
import json, pwd, sys, time, zlib
import logging
import os.path
import paho.mqtt.client as mqtt
import pyslurm
import threading
from datetime import datetime, timezone, timedelta

import influxdb

import collections
from collections import defaultdict as DDict
import pyslurm
import MyTool
import querySlurm
import queryInflux
import SlurmEntities


# Maps a host name to a tuple (time, pid2info), where time is the time
# stamp of the last update, and pid2info maps a pid to a dictionary of
# info about the process identified by the pid.

class InfluxWriter (threading.Thread):
    INTERVAL = 61

    def __init__(self, influxServer='scclin011'):
        threading.Thread.__init__(self)

        self.influx_client = self.connectInflux (influxServer)
        self.source        = []
        logging.info("Start InfluxWriter with influx_client={}, interval={}".format(self.influx_client._baseurl, self.INTERVAL))

    def connectInflux (self, host, port=8086, user="yliu", db="slurmdb"):
        return influxdb.InfluxDBClient(host, port, user, "", db)

    def writeInflux (self, points, ret_policy="autogen", t_precision="s"):
        if ( len(points) == 0 ):
           return

        try:
           logging.info  ("writeInflux {}".format(len(points)))
           #logging.debug ("writeInflux {}".format(points))
           self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision=t_precision)
        except influxdb.exceptions.InfluxDBClientError as err:
           logging.error ("writeInflux " + ret_policy + " ERROR:" + repr(err) + repr(points))

    def addSource(self, source):
        self.source.append(source)

    def run(self):
        while True:
          time.sleep (InfluxWriter.INTERVAL)
          
          points = []
          for s in self.source:
              points.extend(s.retrievePoints ())

          self.writeInflux (points)

class MQTTReader (threading.Thread):
    TS_FNAME = 'host_up_ts.txt'

    #mqtt client to receive data and save it in influx
    def __init__(self, mqttServer='mon5.flatironinstitute.org'):
        threading.Thread.__init__(self)

        self.mqtt_client   = self.connectMqtt   (mqttServer)

        self.lock          = threading.Lock()
        self.cpuinfo       = {} 		#static information
        self.node2ts2uid2proc = {}                 #used by hostproc2point
        self.node2uid2ts2done = {}                 #used by hostproc2point
        self.hostperf_msgs = []
        self.hostinfo_msgs = []
        self.hostproc_msgs = []
        self.startTime     = datetime.now().timestamp()

        # read host_up_ts, TODO: from event_table slurmdb
        if os.path.isfile(self.TS_FNAME):
           with open(self.TS_FNAME, 'r') as f:
                self.cpu_up_ts = json.load(f)
        else:
           self.cpu_up_ts      = {}
        self.cpu_up_ts_count   = 0

        logging.info("Start MQTTReader with mqtt_client={}".format(self.mqtt_client))

    def run(self):
        # Asynchronously receive messages
        self.mqtt_client.loop_forever()

    def connectMqtt (self, host):
        mqtt_client            = mqtt.Client()
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_message = self.on_message
        mqtt_client.connect(host)

        return mqtt_client

    def on_connect(self, client, userdata, flags, rc):
        #self.mqtt_client.subscribe("cluster/hostprocesses/worker1000")
        #print ("on_connect with code %d." %(rc) )
        self.mqtt_client.subscribe("cluster/hostinfo/#")
        self.mqtt_client.subscribe("cluster/hostperf/#")
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    # put into message queue with incoming message
    def on_message(self, client, userdata, msg):
        data     = json.loads(msg.payload)
        if ( self.startTime - data['hdr']['msg_ts'] > 25200 ): # 7 days
           logging.info("Skip old message=" + repr(data['hdr']))
           return

        with self.lock:
          if   ( data['hdr']['msg_type'] == 'cluster/hostperf' ):
           self.hostperf_msgs.append(data)
          elif ( data['hdr']['msg_type'] == 'cluster/hostinfo' ):
           self.hostinfo_msgs.append(data)
          elif ( data['hdr']['msg_type'] == 'cluster/hostprocesses' ):
           self.hostproc_msgs.append(data)

    # retrieve points
    def retrievePoints (self):
        self.slurmTime= datetime.now().timestamp()
        self.jobData  = pyslurm.job().get()
        self.nodeData = pyslurm.node().get()

        points        = []
        #autogen.cpu_info, time, hostname, total_socket, total_thread, total_cores, cpu_model, is_vm
        points.extend(self.process_list(self.hostinfo_msgs, self.hostinfo2point))
        #autogen.cpu_load, time, hostname, proc_*, load_*, cpu_*, mem_*, net_*, disk_*
        points.extend(self.process_list(self.hostperf_msgs, self.hostperf2point))
        #autogen.cpu_proc_info, cpu_proc_mon
        points.extend(self.process_list(self.hostproc_msgs, self.hostproc2point))

        if self.cpu_up_ts_count > 0:
           with open (self.TS_FNAME, 'w') as f:
                json.dump(self.cpu_up_ts, f)
           self.cpu_up_ts_count = 0

        return points

    def process_list (self, msg_list, item_func):
        points=[]
        while (len(msg_list) > 0 ):
            with self.lock: msg = msg_list.pop()
            pts = item_func(msg)
            if pts:         points.extend(pts)
        return points

    # return point only if the information is new
    def hostinfo2point (self, msg):
        #{'tcp_wmem': [16384, 1048576, 56623104], 'tcp_rmem': [16384, 1048576, 56623104], 'hostname_full': 'worker1000', 
        # 'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostinfo', 'msg_ts': 1528902257.666469}, 
        # 'mem': {'swap_total': 1048572, 'hw_mem_installed': 536870912, 'swappiness': 0, 'hw_mem_max': 536870912, 'mem_total': 528279508}, 'net': [{'ip': '10.128.145.0', 'ifname': 'eno1', 'mac': '7c:d3:0a:c6:0d:da'}, {'ip': '169.254.0.2', 'ifname': 'idrac', 'mac': '7c:d3:0a:c6:0d:df'}], 'udp_mem': [12380898, 16507864, 24761796], 
        # 'os': {'kernel_version': '3.10.0-693.5.2.el7.x86_64', 'os_version': 'CentOS Linux release 7.4.1708 (Core)', 'kernel_boot_ts': 1528671180.279176}, 
        # 'cpu': {'total_sockets': 2, 'total_threads': 28, 'total_cores': 28, 'cpu_model': 'Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz', 'is_vm': 0}, 'system': {'system_model': 'PowerEdge C6320', 'bios_vendor': 'Dell Inc.', 'system_vendor': 'Dell Inc.', 'bios_date': '01/09/2017', 'system_serial': 'C5DQMD2', 'bios_version': '2.4.2'}}
        host = msg['hdr']['hostname'] 
        ts   = msg['hdr']['msg_ts']

        # record uptime
        if (host not in self.cpu_up_ts) :
           self.cpu_up_ts[host] = []
        if 'os' in msg:
           up_ts = int(msg['os']['kernel_boot_ts']/10) * 10
           if up_ts not in self.cpu_up_ts[host]:
              self.cpu_up_ts[host].append(up_ts)
              self.cpu_up_ts_count += 1

        if ((host not in self.cpuinfo) or (self.cpuinfo[host] != repr(msg['cpu'])) ):
           self.cpuinfo[host]    = repr(msg['cpu'])

           point = {'measurement':'cpu_info'}
           point['time']   = (int)(ts)
           point['tags']   = {'hostname':host}
           point['fields'] = msg['cpu']

           return [point]
        else:
           return []

    def hostperf2point(self, msg):
        #{'load': [0.29, 0.29, 0.44], 'cpu_times': {'iowait': 553.4, 'idle': 6050244.96, 'user': 12374.76, 'system': 2944.12}, 'proc_total': 798, 'hdr': {'hostname': 'ccalin007', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostperf', 'msg_ts': 1541096161.66126}, 'mem': {'available': 196645462016, 'used': 8477605888, 'cached': 3937718272, 'free': 192701874176, 'total': 201179480064, 'buffers': 5869568}, 'net_io': {'rx_err': 0, 'rx_packets': 6529000, 'rx_bytes': 5984570284, 'tx_err': 0, 'tx_drop': 0, 'tx_bytes': 6859935273, 'tx_packets': 6987776, 'rx_drop': 0}, 'proc_run': 1, 'disk_io': {'write_bytes': 7890793472, 'read_count': 130647, 'write_count': 221481, 'read_time': 19938, 'read_bytes': 2975410176, 'write_time': 6047344}}
        host  = msg['hdr']['hostname']
        ts    = msg['hdr']['msg_ts']

        point                        = {'measurement':'cpu_load', 'time': (int)(ts)}
        point['tags']                = {'hostname'   : host}
        point['fields']              = MyTool.flatten(MyTool.sub_dict(msg, ['cpu_times', 'mem', 'net_io', 'disk_io']))
        point['fields']['load_1min'] = msg['load'][0]
        point['fields']['load_5min'] = msg['load'][1]
        point['fields']['load_15min']= msg['load'][2]
        point['fields']['proc_total']= msg['proc_total']
        point['fields']['proc_run']  = msg['proc_run']

        return [point]

    #ts2uid2procs have 2 ts (preTs, ts) 
    def getUidAggLoad (self, node, ts2uid2procs):

        result      = {}
        preTs,ts    = sorted(ts2uid2procs.keys())
        uid2proc    = ts2uid2procs[ts]
        preUid2proc = ts2uid2procs[preTs]
        for uid, procs in uid2proc.items():
            # self.node2uid2ts2done save the done proc's sum cpu time to avoid drop in accumulated cpu time of a job/uid
            if node not in self.node2uid2ts2done:       self.node2uid2ts2done[node]={}
            if uid  not in self.node2uid2ts2done[node]: self.node2uid2ts2done[node][uid]={}
            
            if uid in preUid2proc:
               donePids    = [pid for pid in preUid2proc[uid].keys() if pid not in procs.keys()]
               if donePids:
                  logging.debug ("getUidAggLoad donePids = " + repr(donePids))
                  doneSum1    = sum(preUid2proc[uid][pid].get('cpu_system_time',0) for pid in donePids)
                  doneSum2    = sum(preUid2proc[uid][pid].get('cpu_user_time',  0) for pid in donePids)
                  if (preTs in self.node2uid2ts2done[node][uid]):
                     doneSum     = [self.node2uid2ts2done[node][uid][preTs][0] + doneSum1, self.node2uid2ts2done[node][uid][preTs][1] + doneSum2]
                  else:
                     doneSum     = [doneSum1, doneSum2]
                  self.node2uid2ts2done[node][uid] = {ts: doneSum}
                  logging.debug ("getUidAggLoad node2uid2ts2done = " + repr(self.node2uid2ts2done[node][uid]))
                  
            # calculate util
            procFldList = list(procs.values())                  #key is pid, [{'cpu_sys_time'}:0,...},...]
            # sum over field keys such as cpu_sys_time
            if (len(procFldList) == 0): 
               logging.warn ("getUidAggLoad WARNING: no process running on the node for user " + str(uid))
               continue

            #TODO: change to field list
            sumDict     = {fKey: sum(fields.get(fKey,0) for fields in procFldList) for fKey in procFldList[0].keys()}
               
            #cpu utilization is different
            pids           = procs.keys()
            #print ("uid=" + repr(uid) + ",ts=" + repr(ts) + ",preUid2proc.keys()=" + repr(preUid2proc.keys()))
            if uid in preUid2proc:
               preProcs       = preUid2proc[uid]
               sum1  = sum(preProcs[pid].get('cpu_system_time',0) for pid in pids if pid in preProcs)
               sum2  = sum(preProcs[pid].get('cpu_user_time',  0) for pid in pids if pid in preProcs)
               preSysTimeSum = 0
               preUsrTimeSum = 0
               for pid in pids:
                   if pid in preProcs: 
                      preSysTimeSum += preProcs[pid].get('cpu_system_time',0)
                      preUsrTimeSum += preProcs[pid].get('cpu_user_time',  0)
               if ( sum1 != preSysTimeSum or sum2 != preUsrTimeSum):
                   logging.error("getUidAggLoad ERROR: sum1 wrong")

               sumDict['cpu_system_util'] = ( sumDict.get('cpu_system_time') - preSysTimeSum ) / max(0.01, ts - preTs)
               sumDict['cpu_user_util']   = ( sumDict.get('cpu_user_time')   - preUsrTimeSum ) / max(0.01, ts - preTs)
               if (sumDict['cpu_system_util'] < 0) or (sumDict['cpu_user_util'] < 0):
                  logging.error("getUidAggLoad ERROR: negative value for utilization " + repr(procs) + " - " + repr(preProcs))
               
               
               if (ts in self.node2uid2ts2done[node][uid]):
                  sumDict['cpu_system_time'] += self.node2uid2ts2done[node][uid][ts][0]
                  sumDict['cpu_user_time']   += self.node2uid2ts2done[node][uid][ts][1]

               result[uid] = sumDict

        return ts,preTs,result
        
    #ingore non-slurm node
    def hostproc2point (self, msg):
        #{'processes': [{'status': 'sleeping', 'uid': 1083, 'mem': {'lib': 0, 'text': 905216, 'shared': 1343488, 'data': 487424, 'vms': 115986432, 'rss': 1695744}, 'pid': 23825, 'cmdline': ['/bin/bash', '/cm/local/apps/slurm/var/spool/job65834/slurm_script'], 'create_time': 1528790822.57, 'io': {'write_bytes': 40570880, 'read_count': 9712133, 'read_bytes': 642359296, 'write_count': 1067292080}, 'num_fds': 4, 'num_threads': 1, 'name': 'slurm_script', 'ppid': 23821, 'cpu': {'system_time': 0.21, 'affinity': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27], 'user_time': 0.17}}, 
        #...
        #'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostprocesses', 'msg_ts': 1528901819.82538}} 
        points   = []
        ts       = msg['hdr']['msg_ts']
        node     = msg['hdr']['hostname']

        if ( len(msg['processes']) == 0 ):   return []
        if ( not self.isSlurmNode(node) ):   return []

        if ( node not in self.node2ts2uid2proc ):  self.node2ts2uid2proc[node] = {}

        ts2uid2proc = self.node2ts2uid2proc[node]                 #{ts: {uid: {pid:cpu_time} } }
        if ( ts not in ts2uid2proc and len(ts2uid2proc) == 2 ):     # ts should be non-decreasing
           # a newer ts, summerize and write the old data to influx, only 2 ts in ts2uid2proc
           preTs,prepreTs,uidSum = self.getUidAggLoad (node, ts2uid2proc)
           
           # create cpu_uid_mon point
           uidPoint     = queryInflux.createUidMonPoint (preTs, node, uidSum)
           #print ("hostproc2point uidPoint=" + repr(uidPoint))
           points.extend (uidPoint)
           #remove the old record
           del ts2uid2proc[prepreTs]
        
        # record the data
        if ts not in ts2uid2proc:  self.node2ts2uid2proc[node][ts] = {}
        uid2proc  = self.node2ts2uid2proc[node][ts]
        for proc in msg['processes']:
            if (not 'user_time' in proc['cpu'] or not 'system_time' in proc['cpu']): logging.error ("CPU info missing ERROR: " + repr(proc))

            pid                           = proc['pid']
            uid                           = proc['uid']
            infopoint                     = {'measurement':'cpu_proc_info'}
            infopoint['time']             = (int)(proc['create_time'])
            infopoint['tags']             = MyTool.sub_dict(proc, ['pid', 'uid'])
            infopoint['tags']['hostname'] = node
            infopoint['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['cmdline', 'name', 'ppid']))
            points.append(infopoint)

            #measurement cpu_proc_mon
            point                     = {'measurement':'cpu_proc_mon'}
            point['time']             = (int)(ts)
            point['tags']             = MyTool.sub_dict(proc, ['pid', 'uid', 'create_time'])
            point['tags']['hostname'] = node
            del proc['cpu']['affinity']
            point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['mem', 'io', 'num_fds', 'cpu'], default=0))
            point['fields']['status'] = querySlurm.SlurmStatus.getStatusID(proc['status'])
            points.append (point)

            #save data from cpu_uid_mon
            if uid not in uid2proc: uid2proc[uid]={}
            uid2proc[uid][pid] = MyTool.flatten(MyTool.sub_dict(proc, ['cpu', 'mem', 'io', 'num_fds'], default=0))

            #print ("node2ts2uid2proc=" + repr(self.node2ts2uid2proc))
            

        return points

    #pyslurm.slurm_pid2jobid only on slurm node with slurmd running

    def isSlurmNode (self, hostname):
        return (hostname in self.nodeData )

class SlurmDataReader (threading.Thread):
    INTERVAL = 30            #10s
    def __init__(self, influxServer='scclin011'):
        threading.Thread.__init__(self)

        self.slurm  = SlurmEntities.SlurmEntities()
        self.points = []
        self.lock   = threading.Lock()
        logging.info("Start SlurmDataReader with interval={}".format(self.INTERVAL))

    def run(self):
        while True:
          ts, jobList = self.slurm.getCurrentPendingJobs()
          #print ("{}:{}".format(ts, jobList))
          self.cvt2points (ts, jobList)
          time.sleep (SlurmDataReader.INTERVAL)

    #return influxdb points
    def cvt2points (self, ts_sec, jobList):
        points = []
        for job in jobList:
            point           = {'measurement':'slurm_pending', 'time': int(ts_sec)}  # time in ms
            point['tags']   = {'state_reason':job.pop('state_reason')}
            point['fields'] = job

            #print ("point {}".format(point))
            points.append(point)

        with self.lock:
            self.points.extend(points)
            logging.debug ("SlurmDataReader data size {}".format(len(self.points)))

    #return the points 
    def retrievePoints (self):
        with self.lock:
            sav         = self.points
            self.points = []

        return sav

    def slurmjob2point (self, item):
    #{'account': 'cca', 'admin_comment': None, 'alloc_node': 'rusty2', 'alloc_sid': 18879, 'array_job_id': None, 'array_task_id': None, 'array_task_str': None, 'array_max_tasks': None, 'assoc_id': 14, 'batch_flag': 1, 'batch_host': 'worker1200', 'batch_script': None, 'billable_tres': None, 'bitflags': 0, 'boards_per_node': 0, 'burst_buffer': None, 'burst_buffer_state': None, 'command': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/iron_start', 'comment': None, 'contiguous': False, 'core_spec': None, 'cores_per_socket': None, 'cpus_per_task': 1, 'cpu_freq_gov': None, 'cpu_freq_max': None, 'cpu_freq_min': None, 'dependency': None, 'derived_ec': '0:0', 'eligible_time': 1529184154, 'end_time': 1529788955, 'exc_nodes': [], 'exit_code': '0:0', 'features': [], 'fed_origin': None, 'fed_siblings': None, 'gres': [], 'group_id': 1119, 'job_id': 69021, 'job_state': 'RUNNING', 'licenses': {}, 'max_cpus': 0, 'max_nodes': 0, 'name': 'bh7_ref2_m000097656_SS01_r01', 'network': None, 'nodes': 'worker[1200-1203]', 'nice': 0, 'ntasks_per_core': None, 'ntasks_per_core_str': 'UNLIMITED', 'ntasks_per_node': 28, 'ntasks_per_socket': None, 'ntasks_per_socket_str': 'UNLIMITED', 'ntasks_per_board': 0, 'num_cpus': 112, 'num_nodes': 4, 'partition': 'cca', 'mem_per_cpu': False, 'min_memory_cpu': None, 'mem_per_node': True, 'min_memory_node': 512000, 'pn_min_memory': 512000, 'pn_min_cpus': 28, 'pn_min_tmp_disk': 0, 'power_flags': 0, 'preempt_time': None, 'priority': 4294901702, 'profile': 0, 'qos': 'cca', 'reboot': 0, 'req_nodes': [], 'req_switch': 0, 'requeue': False, 'resize_time': 0, 'restart_cnt': 0, 'resv_name': None, 'run_time': 232295, 'run_time_str': '2-16:31:35', 'sched_nodes': None, 'shared': '0', 'show_flags': 7, 'sockets_per_board': 0, 'sockets_per_node': None, 'start_time': 1529184155, 'state_reason': 'None', 'std_err': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'std_in': '/dev/null', 'std_out': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'submit_time': 1529184154, 'suspend_time': 0, 'time_limit': 10080, 'time_limit_str': '7-00:00:00', 'time_min': 0, 'threads_per_core': None, 'tres_req_str': 'cpu=112,node=4', 'tres_alloc_str': 'cpu=112,mem=2000G,node=4', 'user_id': 1119, 'wait4switch': 0, 'wckey': None, 'work_dir': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01', 'altered': None, 'block_id': None, 'blrts_image': None, 'cnode_cnt': None, 'ionodes': None, 'linux_image': None, 'mloader_image': None, 'ramdisk_image': None, 'resv_id': None, 'rotate': False, 'conn_type': 'n/a', 'cpus_allocated': {'worker1200': 28, 'worker1201': 28, 'worker1202': 28, 'worker1203': 28}, 'cpus_alloc_layout': {}}
        points   = []

        # slurm_jobs_mon: self.slurmTime, job_id
        point =  {'measurement':'slurm_jobs_mon', 'time': (int)(self.slurmTime)}
        point['tags']   =MyTool.sub_dict(item, ['job_id'])
        point['fields'] =MyTool.flatten(MyTool.sub_dict(item, ['run_time', 'job_state']))
        #ATTENTION: not saving slurm_jobs_mon as it is only elpased time and not useful
        #points.append(point)

        # slurm_jobs: submit_time, job_id, user_id
        infopoint = {'measurement':'slurm_jobs', 'time': (int)(item.pop('submit_time'))}
        infopoint['tags']   =MyTool.sub_dict_remove(item, ['user_id', 'job_id'])

        cpu_allocated       = item.pop('cpus_allocated')
        for v in ['job_state', 'run_time_str', 'time_limit_str', 'std_err', 'std_out', 'work_dir', 'cpus_alloc_layout', 'std_in']: del item[v]
        infopoint['fields'] =MyTool.flatten(item)
        if cpu_allocated: infopoint['fields']['cpus_allocated']=repr(cpu_allocated)  #otherwise, two many fields
        if infopoint['fields']['time_limit'] == 'UNLIMITED':
           infopoint['fields']['time_limit'] = -1
        
        #ATTENTION: not saving slurm_jobs as it can be retrived from slurm db
        #points.append(infopoint)

        return points

def main(influxServer):
    r1   = MQTTReader()
    r1.start()
    r2   = SlurmDataReader ()
    r2.start()

    ifdb = InfluxWriter    (influxServer)
    ifdb.addSource (r1)
    ifdb.addSource (r2)
    ifdb.start()

if __name__=="__main__":
   #Usage: python mqtMon2Influx.py [influx_server]
   logging.basicConfig(filename='/tmp/slurm_util/mqtMon2Influx.log',level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s') # must before InfluxDB

   influxServer = 'scclin011'
   if len(sys.argv) > 1:
      influxServer = sys.argv[1]

   main(influxServer)

