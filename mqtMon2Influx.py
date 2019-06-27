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
import MyTool
import querySlurm

import pdb

# Maps a host name to a tuple (time, pid2info), where time is the time
# stamp of the last update, and pid2info maps a pid to a dictionary of
# info about the process identified by the pid.

class InfluxWriter (threading.Thread):
    INTERVAL   = 61
    BATCH_SIZE = 8000

    def __init__(self, influxServer='scclin011', influxDB='slurmdb'):
        threading.Thread.__init__(self)

        self.influx_client = self.connectInflux (influxServer, influxDB)
        self.source        = []
        logging.info("Start InfluxWriter with influx_client={}, interval={}".format(self.influx_client._baseurl, self.INTERVAL))

    def connectInflux (self, host, db, port=8086, user="yliu"):
        return influxdb.InfluxDBClient(host, port, user, "", db)

    def writeInflux (self, points, ret_policy="autogen", t_precision="s"):
        try:
           logging.info  ("writeInflux {}".format(len(points)))
           if debug:
              print("writeInflux {}\n".format(len(points)))
              for idx in range(min(len(points),10)):
                  print("{}".format(points[idx]))
              return True
           else:
              #ret = self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision=t_precision, batch_size=BATCH_SIZE)
              ret = self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision=t_precision)
              return ret
        except influxdb.exceptions.InfluxDBClientError as err:
           logging.error ("writeInflux " + ret_policy + " ERROR:" + repr(err) + repr(points))
           return False

    def addSource(self, source):
        self.source.append(source)

    def run(self):
        points = []
        while True:
          time.sleep (InfluxWriter.INTERVAL)
          
          for s in self.source:
              points.extend(s.retrievePoints ())
              logging.info ("InfluxWriter have {} points after checking source {}".format(len(points), s))

          if len(points) == 0: continue
          ret = self.writeInflux (points)
          logging.debug("writeInflux return {}".format(ret))
          if ret:
             points = []
          else: 
             # show the users list, still failed, reconnect with it
             logging.error("write_points return False")

class MQTTReader (threading.Thread):
    TS_FNAME = 'host_up_ts.txt'

    #mqtt client to receive data and save it in influx
    #two threads: one for mqtt client, one for the reader
    def __init__(self, mqttServer='mon5.flatironinstitute.org'):
        threading.Thread.__init__(self)

        self.mqtt_client   = self.connectMqtt   (mqttServer)

        self.cpuinfo       = {} 		#static information
        self.nodeUidTs2points = DDict(lambda: DDict(lambda: DDict(dict))) #{node:{uid:{ts:{pid:procPoint, ...]},...}, hostproc2point provider, create_uid_point use it
       
        self.lock          = threading.Lock()   #guard the message list hostperf_msgs, ...
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
        #self.pyslurmQueryTime= datetime.now().timestamp()
        self.jobData  = pyslurm.job().get()
        self.nodeData = pyslurm.node().get()

        points        = []
        #autogen.cpu_info: time, hostname, total_socket, total_thread, total_cores, cpu_model, is_vm
        points.extend(self.process_list(self.hostinfo_msgs, self.hostinfo2point))
        #autogen.cpu_load: time, hostname, proc_*, load_*, cpu_*, mem_*, net_*, disk_*
        points.extend(self.process_list(self.hostperf_msgs, self.hostperf2point))
        #autogen.cpu_proc_info, cpu_proc_mon
        points.extend(self.process_list(self.hostproc_msgs, self.hostproc2point))
        if self.nodeUidTs2points:
           #autogen.cpu_uid_mon
           points.extend(self.create_uid_points())

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

           point = {'measurement':'cpu_info', 'time': (int)(ts)}
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
    #ingore non-slurm node
    #nodeUidTs2points = DDict(DDict(list))        #{uid:{pid:[procPoint, ...]},...}
    def hostproc2point (self, msg):
        #{'processes': [{'status': 'sleeping', 'uid': 1083, 'mem': {'lib': 0, 'text': 905216, 'shared': 1343488, 'data': 487424, 'vms': 115986432, 'rss': 1695744}, 'pid': 23825, 'cmdline': ['/bin/bash', '/cm/local/apps/slurm/var/spool/job65834/slurm_script'], 'create_time': 1528790822.57, 'io': {'write_bytes': 40570880, 'read_count': 9712133, 'read_bytes': 642359296, 'write_count': 1067292080}, 'num_fds': 4, 'num_threads': 1, 'name': 'slurm_script', 'ppid': 23821, 'cpu': {'system_time': 0.21, 'affinity': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27], 'user_time': 0.17}}, 
        #...
        #'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostprocesses', 'msg_ts': 1528901819.82538}} 
        ts   = (int)(msg['hdr']['msg_ts'])
        node = msg['hdr']['hostname']
        if ( len(msg['processes']) == 0 ):   return []
        if ( not self.isSlurmNode(node) ):   return []

        infoPoints   = []
        procPoints   = []

        # generate cpu_proc_info and cpu_proc_mon
        for proc in msg['processes']:
            if (not 'user_time' in proc['cpu'] or not 'system_time' in proc['cpu']): logging.error ("CPU info missing ERROR: {}".format(proc))

            uid                       = proc['uid']
            pid                       = proc['pid']
            point                     = {'measurement':'cpu_proc_info', 'time': (int)(proc['create_time'])}
            point['tags']             = MyTool.sub_dict(proc, ['pid', 'uid'])
            point['tags']['hostname'] = node
            point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['cmdline', 'name', 'ppid']))
            #TODO: check if duplicate
            infoPoints.append(point)

            #measurement cpu_proc_mon
            point                     = {'measurement':'cpu_proc_mon', 'time': ts}
            point['tags']             = MyTool.sub_dict(proc, ['pid', 'uid', 'create_time'])
            point['tags']['hostname'] = node
            if 'affinity' in proc['cpu']:
               del proc['cpu']['affinity']  # not including cpu.affinity
            point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['mem', 'io', 'num_fds', 'cpu'], default=0))
            point['fields']['status'] = querySlurm.SlurmStatus.getStatusID(proc['status'])
            procPoints.append (point)

            self.nodeUidTs2points[node][uid][ts][pid]=point
        #logging.debug('hostproc2point generate {} cpu_proc_info and {} cpu_proc_mon points'.format(len(infoPoints), len(procPoints)))

        return infoPoints.extend(procPoints)

    # generate cpu_uid_mon
    def create_uid_points (self):
      uidPoints             = []

      for node, uidTs2points in self.nodeUidTs2points.items():
        for uid, ts2points in uidTs2points.items():
            tsLst = sorted(ts2points.keys())   #sorted ts
            # calculate rate and only keep the newest point inside ts2points
            #for each uid on the node, only keep the newest record
            for idx in range(1, len(tsLst)):
                currTs   = tsLst[idx]
                currDict = ts2points[currTs]     # dict {pid, point}
                preDict  = ts2points.pop(tsLst[idx-1])
                period   = currTs-tsLst[idx-1]
                if ( period < 0.01 ):
                   logging.error("Period is almost 0 between points {} and {}. Ignore.".format(preDict, currDict))
                   continue
                if ( period > 120 ):
                   logging.error("create_uid_points: Period is {} bigger than 60 seconds between {} and {}.".format(period, tsLst[idx-1], currTs))

                point         = {'measurement':'cpu_uid_mon', 'time':currTs, 'fields': {}}
                point['tags'] = {'uid':uid, 'hostname':node}
                for field, attr in [('cpu_system_util','cpu_system_time'), ('cpu_user_util','cpu_user_time'), ('io_read_rate','io_read_bytes'), ('io_write_rate','io_write_bytes')]:
                    # if curr have pid (pre not),           return curr value
                    # if curr does not have pid (pre have), return 0
                    point['fields'][field] = sum([currDict[pid]['fields'][attr] - preDict.get(pid, {}).get('fields',{}).get(attr,0) for pid in currDict.keys()])/period
                for attr in ['mem_data', 'mem_rss', 'mem_shared', 'mem_text', 'mem_vms', 'num_fds']:
                    point['fields'][attr] = sum([currDict[pid]['fields'][attr] for pid in currDict.keys()])
                uidPoints.append (point)
               
            #logging.debug("self.nodeUidTs2points[{}][{}]={}".format(node,uid,self.nodeUidTs2points[node][uid]))

      logging.debug('create_uid_points generate {} cpu_uid_mon points'.format(len(uidPoints)))
      return uidPoints

    #pyslurm.slurm_pid2jobid only on slurm node with slurmd running
    def isSlurmNode (self, hostname):
        return (hostname in self.nodeData )

class PyslurmReader (threading.Thread):
    INTERVAL = 30            #10s
    def __init__(self, influxServer='scclin011'):
        threading.Thread.__init__(self)

        self.points = []
        self.lock   = threading.Lock()       #protect self.points as it is read and write by different threads
   
        self.sav_job_dict = {}               #save job_id:json.dumps(infopoint)
        self.sav_node_dict = {}              #save name:json.dumps(infopoint)
        self.sav_part_dict = {}              #save value
        self.sav_qos_dict  = {}              #save value
        self.sav_res_dict  = {}              #save value
        logging.info("Init PyslurmReader with interval={}".format(self.INTERVAL))

    def run(self):
        #pdb.set_trace()
        logging.info("Start running PyslurmReader ...")
        while True:
          # pyslurm query
          ts       = int(datetime.now().timestamp())
          job_dict = pyslurm.job().get()
          node_dict= pyslurm.node().get()
          part_dict= pyslurm.partition().get()
          qos_dict = pyslurm.qos().get()
          res_dict = pyslurm.reservation().get()
          #js_dict  = pyslurm.jobstep().get()

          #convert to points
          points   = []
          for jid,job in job_dict.items():
              self.slurmJob2point(ts, job, points)
          finishJob = [jid for jid in self.sav_job_dict.keys() if jid not in job_dict.keys()]
          logging.debug ("Finish jobs {}".format(finishJob))
          for jid in finishJob:
              del self.sav_job_dict[jid]

          for node in node_dict.values():
              self.slurmNode2point(ts, node, points)

          if json.dumps(part_dict) != json.dumps(self.sav_part_dict):
              for pname, part in part_dict.items():
                 self.slurmPartition2point(ts, pname, part, points)
              self.sav_part_dict = part_dict

          if json.dumps(qos_dict) != json.dumps(self.sav_qos_dict):
              for qname, qos in qos_dict.items():
                 self.slurmQOS2point(ts, qname, qos, points)
              self.sav_qos_dict = qos_dict

          if json.dumps(res_dict) != json.dumps(self.sav_res_dict):
              for rname, res in res_dict.items():
                 self.slurmReservation2point(ts, rname, res, points)
              self.sav_res_dict = res_dict

          with self.lock:
              logging.info("PyslurmReade.run add points {}".format(len(points)))
              self.points.extend(points)

          time.sleep (PyslurmReader.INTERVAL)

    #return the points, called by InfluxDBWriter 
    def retrievePoints (self):
        with self.lock:
            sav         = self.points
            self.points = []

        return sav


    def slurmJob2point (self, ts, item, points):
    #{'account': 'cca', 'admin_comment': None, 'alloc_node': 'rusty2', 'alloc_sid': 18879, 'array_job_id': None, 'array_task_id': None, 'array_task_str': None, 'array_max_tasks': None, 'assoc_id': 14, 'batch_flag': 1, 'batch_host': 'worker1200', 'batch_script': None, 'billable_tres': None, 'bitflags': 0, 'boards_per_node': 0, 'burst_buffer': None, 'burst_buffer_state': None, 'command': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/iron_start', 'comment': None, 'contiguous': False, 'core_spec': None, 'cores_per_socket': None, 'cpus_per_task': 1, 'cpu_freq_gov': None, 'cpu_freq_max': None, 'cpu_freq_min': None, 'dependency': None, 'derived_ec': '0:0', 'eligible_time': 1529184154, 'end_time': 1529788955, 'exc_nodes': [], 'exit_code': '0:0', 'features': [], 'fed_origin': None, 'fed_siblings': None, 'gres': [], 'group_id': 1119, 'job_id': 69021, 'job_state': 'RUNNING', 'licenses': {}, 'max_cpus': 0, 'max_nodes': 0, 'name': 'bh7_ref2_m000097656_SS01_r01', 'network': None, 'nodes': 'worker[1200-1203]', 'nice': 0, 'ntasks_per_core': None, 'ntasks_per_core_str': 'UNLIMITED', 'ntasks_per_node': 28, 'ntasks_per_socket': None, 'ntasks_per_socket_str': 'UNLIMITED', 'ntasks_per_board': 0, 'num_cpus': 112, 'num_nodes': 4, 'partition': 'cca', 'mem_per_cpu': False, 'min_memory_cpu': None, 'mem_per_node': True, 'min_memory_node': 512000, 'pn_min_memory': 512000, 'pn_min_cpus': 28, 'pn_min_tmp_disk': 0, 'power_flags': 0, 'preempt_time': None, 'priority': 4294901702, 'profile': 0, 'qos': 'cca', 'reboot': 0, 'req_nodes': [], 'req_switch': 0, 'requeue': False, 'resize_time': 0, 'restart_cnt': 0, 'resv_name': None, 'run_time': 232295, 'run_time_str': '2-16:31:35', 'sched_nodes': None, 'shared': '0', 'show_flags': 7, 'sockets_per_board': 0, 'sockets_per_node': None, 'start_time': 1529184155, 'state_reason': 'None', 'std_err': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'std_in': '/dev/null', 'std_out': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'submit_time': 1529184154, 'suspend_time': 0, 'time_limit': 10080, 'time_limit_str': '7-00:00:00', 'time_min': 0, 'threads_per_core': None, 'tres_req_str': 'cpu=112,node=4', 'tres_alloc_str': 'cpu=112,mem=2000G,node=4', 'user_id': 1119, 'wait4switch': 0, 'wckey': None, 'work_dir': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01', 'altered': None, 'block_id': None, 'blrts_image': None, 'cnode_cnt': None, 'ionodes': None, 'linux_image': None, 'mloader_image': None, 'ramdisk_image': None, 'resv_id': None, 'rotate': False, 'conn_type': 'n/a', 'cpus_allocated': {'worker1200': 28, 'worker1201': 28, 'worker1202': 28, 'worker1203': 28}, 'cpus_alloc_layout': {}}
        # remove empty values
        job_id = item['job_id']

        MyTool.remove_dict_empty(item)
        for v in ['run_time_str', 'time_limit_str']: item.pop(v, None)

        # pending_job
        if item['job_state'] == 'PENDING':
           pendpoint          = {'measurement':'slurm_pending', 'time': ts} 
           pendpoint['tags']  = MyTool.sub_dict_exist (item, ['job_id', 'state_reason'])
           pendpoint['fields']= MyTool.sub_dict_exist (item, ['submit_time', 'user_id', 'account', 'qos', 'partition', 'tres_per_node', 'last_sched_eval', 'time_limit', 'start_time'])
           points.append(pendpoint)

        # slurm_job_mon: ts, job_id
        point =  {'measurement':'slurm_job_mon', 'time': ts}
        point['tags']   = MyTool.sub_dict_remove       (item, ['job_id', 'user_id'])
        point['fields'] = MyTool.sub_dict_exist_remove (item, ['job_state', 'num_cpus', 'num_nodes', 'state_reason', 'run_time', 'suspend_time'])
        points.append(point)

        # slurm_job_info: submit_time, job_id, user_id
        infopoint = {'measurement':'slurm_job', 'time': (int)(item.pop('submit_time'))}
        infopoint['tags']   = MyTool.sub_dict (point['tags'], ['job_id', 'user_id'])
        infopoint['fields'] = item
        infopoint['fields'].update (MyTool.sub_dict(point['fields'], ['job_state', 'num_cpus', 'num_nodes', 'state_reason']))
        MyTool.update_dict_value2string(infopoint['fields'])
       
        newValue = json.dumps(infopoint)
        if (job_id not in self.sav_job_dict) or (self.sav_job_dict[job_id] != newValue):
           points.append(infopoint)
           self.sav_job_dict[job_id] = newValue
        #else:
        #   logging.info("duplicate job info for {}".format(job_id))

        points.append(infopoint)

        return points

    def slurmNode2point (self, ts, item, points):
#{'arch': 'x86_64', 'boards': 1, 'boot_time': 1560203329, 'cores': 14, 'core_spec_cnt': 0, 'cores_per_socket': 14, 'cpus': 28, 'cpu_load': 2, 'cpu_spec_list': [], 'features': 'k40', 'features_active': 'k40', 'free_mem': 373354, 'gres': ['gpu:k40c:1', 'gpu:k40c:1'], 'gres_drain': 'N/A', 'gres_used': ['gpu:k40c:0(IDX:N/A)', 'mic:0'], 'mcs_label': None, 'mem_spec_limit': 0, 'name': 'workergpu00', 'node_addr': 'workergpu00', 'node_hostname': 'workergpu00', 'os': 'Linux 3.10.0-957.10.1.el7.x86_64 #1 SMP Mon Mar 18 15:06:45 UTC 2019', 'owner': None, 'partitions': ['gpu'], 'real_memory': 384000, 'slurmd_start_time': 1560203589, 'sockets': 2, 'threads': 1, 'tmp_disk': 0, 'weight': 1, 'tres_fmt_str': 'cpu=28,mem=375G,billing=28,gres/gpu=2', 'version': '18.08', 'reason': None, 'reason_time': None, 'reason_uid': None, 'power_mgmt': {'cap_watts': None}, 'energy': {'current_watts': 0, 'base_consumed_energy': 0, 'consumed_energy': 0, 'base_watts': 0, 'previous_consumed_energy': 0}, 'alloc_cpus': 0, 'err_cpus': 0, 'state': 'IDLE', 'alloc_mem': 0}
#REBOOT state, boot_time and slurmd_start_time is 0

        MyTool.remove_dict_empty(item)

        name = item['name']
        # slurm_node_mon: ts, name
        point           =  {'measurement':'slurm_node_mon', 'time': ts}
        point['tags']   = MyTool.sub_dict_exist_remove (item, ['name', 'boot_time', 'slurmd_start_time'])
        point['fields'] = MyTool.sub_dict_exist_remove (item, ['cpus', 'cpu_load', 'alloc_cpus', 'state', 'free_mem', 'gres', 'gres_used', 'partitions', 'reason', 'reason_time', 'reason_uid', 'err_cpus', 'alloc_mem'])
        MyTool.update_dict_value2string(point['fields'])
        points.append(point)

        # slurm_jobs: slurmd_start_time
        if ( 'boot_time' in point['tags']):
           infopoint = {'measurement':'slurm_node', 'time': (int)(point['tags']['boot_time'])}
           infopoint['tags']   = MyTool.sub_dict_exist (point['tags'],   ['name', 'slurmd_start_time'])
           infopoint['fields'] = MyTool.sub_dict_exist (point['fields'], ['cpus', 'partitions'])
           infopoint['fields'].update (item)
           MyTool.update_dict_value2string(infopoint['fields'])
      
           newValue = json.dumps(infopoint)
           if (name not in self.sav_node_dict) or (self.sav_node_dict[name] != newValue):
              points.append(infopoint)
              self.sav_node_dict[name] = newValue

        return points

    def slurmPartition2point (self, ts, name, item, points):
#{'allow_accounts': 'ALL', 'deny_accounts': None, 'allow_alloc_nodes': 'ALL', 'allow_groups': ['cca'], 'allow_qos': ['gen', 'cca'], 'deny_qos': None, 'alternate': None, 'billing_weights_str': None, 'cr_type': 0, 'def_mem_per_cpu': None, 'def_mem_per_node': 'UNLIMITED', 'default_time': 604800, 'default_time_str': '7-00:00:00', 'flags': {'Default': 0, 'Hidden': 0, 'DisableRootJobs': 0, 'RootOnly': 0, 'Shared': 'EXCLUSIVE', 'LLN': 0, 'ExclusiveUser': 0}, 'grace_time': 0, 'max_cpus_per_node': 'UNLIMITED', 'max_mem_per_cpu': None, 'max_mem_per_node': 'UNLIMITED', 'max_nodes': 'UNLIMITED', 'max_share': 0, 'max_time': 604800, 'max_time_str': '7-00:00:00', 'min_nodes': 1, 'name': 'cca', 'nodes': 'worker[1000-1239,3000-3191]', 'over_time_limit': 0, 'preempt_mode': 'OFF', 'priority_job_factor': 1, 'priority_tier': 1, 'qos_char': 'cca', 'state': 'UP', 'total_cpus': 14400, 'total_nodes': 432, 'tres_fmt_str': 'cpu=14400,mem=264000G,node=432,billing=14400'}

        MyTool.remove_dict_empty(item)

        point           = {'measurement':'slurm_partition_0618', 'time': ts}
        name            = item.pop('name')
        point['tags']   = {'name':name}
        point['fields'] = item

        MyTool.update_dict_value2string(point['fields'])
        points.append(point)

        return points

    def slurmQOS2point (self, ts, name, item, points):
#{'description': 'cca', 'flags': 0, 'grace_time': 0, 'grp_jobs': 4294967295, 'grp_submit_jobs': 4294967295, 'grp_tres': '1=6000', 'grp_tres_mins': None, 'grp_tres_run_mins': None, 'grp_wall': 4294967295, 'max_jobs_pu': 4294967295, 'max_submit_jobs_pu': 4294967295, 'max_tres_mins_pj': None, 'max_tres_pj': None, 'max_tres_pn': None, 'max_tres_pu': '1=840', 'max_tres_run_mins_pu': None, 'max_wall_pj': 10080, 'min_tres_pj': None, 'name': 'cca', 'preempt_mode': 'OFF', 'priority': 15, 'usage_factor': 1.0, 'usage_thres': 4294967295.0}

        MyTool.remove_dict_empty(item)

        point           = {'measurement':'slurm_qos', 'time': ts}
        point['tags']   = {'name': item.pop('name')}
        point['fields'] = item

        MyTool.update_dict_value2string(point['fields'])
        points.append(point)

        return points

    def slurmReservation2point (self, ts, name, item, points):
#{'andras_test': {'accounts': [], 'burst_buffer': [], 'core_cnt': 28, 'end_time': 1591885549, 'features': [], 'flags': 'MAINT,SPEC_NODES', 'licenses': {}, 'node_cnt': 1, 'node_list': 'worker1010', 'partition': None, 'start_time': 1560349549, 'tres_str': ['cpu=28'], 'users': ['root', 'apataki', 'ifisk', 'carriero', 'ntrikoupis']}}
        MyTool.remove_dict_empty(item)

        point           = {'measurement':'slurm_reservation', 'time': ts}
        point['tags']   = {'name': name}
        point['fields'] = item

        MyTool.update_dict_value2string(point['fields'])
        points.append(point)

        return points

def main(influxServer, influxDB):
    r1   = MQTTReader()
    r1.start()
    time.sleep(5)
    r2   = PyslurmReader ()
    r2.start()
    time.sleep(5)

    ifdb = InfluxWriter    (influxServer, influxDB)
    ifdb.addSource (r1)
    ifdb.addSource (r2)
    ifdb.start()

if __name__=="__main__":
   #Usage: python mqtMon2Influx.py [influx_server]

   influxServer = 'scclin011'
   influxDB     = 'slurmdb'
   logFile      = '/tmp/slurm_util/mqtMon2Influx.log'
   debug        = False
   if len(sys.argv) > 1:
      influxServer = sys.argv[1]
   elif os.path.isfile('./config.json'):
      with open('./config.json') as config_file:
           config = json.load(config_file)
      influxServer = config['influxdb']['host']
      influxDB     = config['influxdb']['db']
      logFile      = config['log']['file']
      debug        = config['debug']

   logging.basicConfig(filename=logFile,level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s') # must before influxdb
   print("Start ... influxServer={}:{}, logfile={}, debug={}".format(influxServer, influxDB, logFile, debug))
   main(influxServer, influxDB)

