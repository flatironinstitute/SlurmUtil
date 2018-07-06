#!/usr/bin/env python

import _pickle as cPickle
import urllib.request as urllib2
import json, pwd, sys, time, zlib
import paho.mqtt.client as mqtt
import pyslurm
from datetime import datetime, timezone, timedelta

import influxdb

import collections
from collections import defaultdict as DDict
import pyslurm


# Maps a host name to a tuple (time, pid2info), where time is the time
# stamp of the last update, and pid2info maps a pid to a dictionary of
# info about the process identified by the pid.

def sub_dict(somedict, somekeys, default=None):
    return dict([ (k, somedict.get(k, default)) for k in somekeys ])
def sub_dict_remove(somedict, somekeys, default=None):
    return dict([ (k, somedict.pop(k, default)) for k in somekeys ])

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k

        if k == 'cpus_allocated': print(d)
        if isinstance(v, collections.MutableMapping):
           if v:
              items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list): 
           if v:
           #unquoted string=boolean and cause invalid boolean error in influxDB
              items.append((new_key, repr(v)))
        elif v:
           items.append((new_key, v))
    return dict(items)


class DataReader:
    Interval = 61
    LOCAL_TZ = timezone(timedelta(hours=-4))

    #mqtt client to receive data and save it in influx
    def __init__(self, mqttServer='mon5.flatironinstitute.org', influxServer='localhost'):
        self.influx_client = self.connectInflux (influxServer)
        self.mqtt_client   = self.connectMqtt   (mqttServer)
        self.cpuinfo       = {} 		#static information
        self.activeProc    = {}
        self.activeSlurmJobs= []                 #jid list

        print("influx_client= " + repr(self.influx_client._baseurl))

    def run(self):
        self.hostperf_msgs      = []
        self.hostinfo_msgs      = []
        self.hostproc_msgs      = []
        self.start     = time.time()

        # Asynchronously receive messages
        self.mqtt_client.loop_forever()

    def connectInflux (self, host, port=8086):
        return influxdb.InfluxDBClient(host, 8086, "yliu", "", "slurmdb")


    def connectMqtt (self, host):
        mqtt_client            = mqtt.Client()
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_message = self.on_message
        mqtt_client.connect(host)

        return mqtt_client

    def on_connect(self, client, userdata, flags, rc):
        #self.mqtt_client.subscribe("cluster/hostprocesses/worker1000")
        #print ("on_connect with code %d." %(rc) )
        #self.mqtt_client.subscribe("cluster/hostperf/#")
        self.mqtt_client.subscribe("cluster/hostinfo/#")
        self.mqtt_client.subscribe("cluster/hostperf/#")
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    def on_message(self, client, userdata, msg):
        data     = json.loads(msg.payload)
        #print ("on_message data=" + repr(data['hdr']['msg_type']))
        if ( self.start - data['hdr']['msg_ts'] > 25200 ): # 7 days
           print ("Skip old message=" + repr(data['hdr']))
           return

        if   ( data['hdr']['msg_type'] == 'cluster/hostperf' ):
           self.hostperf_msgs.append(data)
        elif ( data['hdr']['msg_type'] == 'cluster/hostinfo' ):
           self.hostinfo_msgs.append(data)
        elif ( data['hdr']['msg_type'] == 'cluster/hostprocesses' ):
           self.hostproc_msgs.append(data)

        curr     = time.time()
        if (curr - self.start) > DataReader.Interval:  #deal message on interval
            #print (t1, self.msg_count, data, file=sys.stderr)
            self.batch_process ()
            self.start += DataReader.Interval

    def batch_process (self):
        self.slurmTime= datetime.now().timestamp()
        self.jobData  = pyslurm.job().get()
        self.nodeData = pyslurm.node().get()

        self.points   = {"one_week":[], "one_month":[], "one_year":[], "autogen":[]}
        self.process_hostperf ()
        self.process_hostinfo ()
        self.process_hostproc ()
        self.process_slurmjob ()

        for policy, points in self.points.items():
            self.writeInflux(points, policy)

    #cpu_info
    def process_hostinfo (self):
        points=self.process_list(self.hostinfo_msgs, self.hostinfo2point)
        if points:
           self.points['autogen'].extend(points)

    #cpu_load
    def process_hostperf (self):
        points=self.process_list(self.hostperf_msgs, self.hostperf2point)
        if points:
           self.points['one_week'].extend(points)

    #cpu_proc_info, cpu_proc_mon
    def process_hostproc (self):
        points = self.process_list(self.hostproc_msgs, self.hostproc2point)
        for point in points:
            if point['measurement'] == 'cpu_proc_info':
               self.points['autogen'].append(point)
            elif point['measurement'] == 'cpu_proc_mon':
               self.points['one_week'].append(point)

    #slurm_jobs, slurm_jobs_mon
    def process_slurmjob (self):
        print("process_slurmjob " + repr(datetime.now()))
        
        points = self.process_list(list(self.jobData.values()), self.slurmjob2point)
        for point in points:
            if point['measurement'] == 'slurm_jobs':
               self.points['autogen'].append(point)
            elif point['measurement'] == 'slurm_jobs_mon':
               self.points['one_week'].append(point)

    def process_list (self, msg_list, proc_func):
        points=[]
        while (len(msg_list) > 0 ):
            msg = msg_list.pop()
            pts = proc_func(msg)
            if pts:
               points.extend(pts)
        return points

    def writeInflux (self, points, ret_policy="autogen"):
        if ( len(points) == 0 ):
           return

        print ("write data to influx policy "  + ret_policy)
        #print ("write data to influx type"     + repr(type(points)))
        #print ("write data to influx 1st data " + points[0])
        #for point in points:
        #    print(point)
        #    print(point['measurement'])

        try:
           self.influx_client.write_points (points,  retention_policy=ret_policy)
        except influxdb.exceptions.InfluxDBClientError as err:
           print(repr(err))

    def hostinfo2point (self, msg):
        #{'tcp_wmem': [16384, 1048576, 56623104], 'tcp_rmem': [16384, 1048576, 56623104], 'hostname_full': 'worker1000', 
        # 'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostinfo', 'msg_ts': 1528902257.666469}, 
        # 'mem': {'swap_total': 1048572, 'hw_mem_installed': 536870912, 'swappiness': 0, 'hw_mem_max': 536870912, 'mem_total': 528279508}, 'net': [{'ip': '10.128.145.0', 'ifname': 'eno1', 'mac': '7c:d3:0a:c6:0d:da'}, {'ip': '169.254.0.2', 'ifname': 'idrac', 'mac': '7c:d3:0a:c6:0d:df'}], 'udp_mem': [12380898, 16507864, 24761796], 'os': {'kernel_version': '3.10.0-693.5.2.el7.x86_64', 'os_version': 'CentOS Linux release 7.4.1708 (Core)', 'kernel_boot_ts': 1528671180.279176}, 
        # 'cpu': {'total_sockets': 2, 'total_threads': 28, 'total_cores': 28, 'cpu_model': 'Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz', 'is_vm': 0}, 'system': {'system_model': 'PowerEdge C6320', 'bios_vendor': 'Dell Inc.', 'system_vendor': 'Dell Inc.', 'bios_date': '01/09/2017', 'system_serial': 'C5DQMD2', 'bios_version': '2.4.2'}}
        host = msg['hdr']['hostname'] 
        if ((host not in self.cpuinfo) or (self.cpuinfo[host] != repr(msg['cpu'])) ):
           self.cpuinfo[host]= repr(msg['cpu'])

           point = {'measurement':'cpu_info'}
           point['time']   =self.getUTCDateTime(msg['hdr']['msg_ts']).isoformat()
           point['tags']   ={}
           point['tags']['hostname'] = msg['hdr']['hostname']
           point['fields'] =msg['cpu']

           
           return [point]
        else:
           return []

    def hostperf2point(self, msg):
        #{'load': [28.09, 28.02, 27.98], 'proc_total': 1741, 'proc_run': 28, 'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostperf', 'msg_ts': 1528836278.654127}}
        point={'measurement':'cpu_load'}
        point['tags']={}
        point['tags']['hostname'] = msg['hdr']['hostname']
        point['time']=self.getUTCDateTime(msg['hdr']['msg_ts']).isoformat()
        
        point['fields']={}
        point['fields']['load_1min'] =msg['load'][0]
        point['fields']['load_5min'] =msg['load'][1]
        point['fields']['load_15min']=msg['load'][2]
        point['fields']['proc_total']=msg['proc_total']
        point['fields']['proc_run']  =msg['proc_run']

        return [point]

    #ingore non-slurm node
    def hostproc2point (self, msg):
        #{'processes': [{'status': 'sleeping', 'uid': 1083, 'mem': {'lib': 0, 'text': 905216, 'shared': 1343488, 'data': 487424, 'vms': 115986432, 'rss': 1695744}, 'pid': 23825, 'cmdline': ['/bin/bash', '/cm/local/apps/slurm/var/spool/job65834/slurm_script'], 'create_time': 1528790822.57, 'io': {'write_bytes': 40570880, 'read_count': 9712133, 'read_bytes': 642359296, 'write_count': 1067292080}, 'num_fds': 4, 'num_threads': 1, 'name': 'slurm_script', 'ppid': 23821, 'cpu': {'system_time': 0.21, 'affinity': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27], 'user_time': 0.17}}, 
        #...
        #'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostprocesses', 'msg_ts': 1528901819.82538}} 
        #print( "hostproc=" + repr(msg))
        points   = []
        hostname = msg['hdr']['hostname']
        if ( not self.isSlurmNode(hostname) ):
           print ("Ignore non-slurm node " + hostname)
           return points

        ts       = self.getUTCDateTime(msg['hdr']['msg_ts']).isoformat()
        prePs    = self.activeProc.get(hostname, [])
        currPs   = {}
        cntNew   = 0
        cntOld   = 0
        for proc in msg['processes']:
            pid = proc['pid']
            if ( not pid in prePs):
               infopoint = {'measurement':'cpu_proc_info'}
               infopoint['time']   =self.getUTCDateTime(proc['create_time'])
               infopoint['tags']   =sub_dict(proc, ['pid', 'uid'])
               infopoint['tags']['hostname'] = hostname

               infopoint['fields'] =flatten(sub_dict(proc, ['cmdline', 'name', 'ppid']))
               points.append(infopoint)

               #add to self.activeProc
               currPs[pid]=[proc['create_time']]
               cntNew += 1
            else:
               currPs[pid]=prePs.pop(pid)
               cntOld += 1
              
            point = {'measurement':'cpu_proc_mon'}
            point['time']   =ts
            point['tags']   =sub_dict(proc, ['pid', 'uid', 'create_time'])
            point['tags']['hostname'] = hostname

            del proc['cpu']['affinity']
            point['fields'] =flatten(sub_dict(proc, ['status', 'mem', 'io', 'num_fds', 'cpu']))
            points.append (point)
        
        print(hostname + ": # of new processes=" + str(cntNew) + ",old processes=" + str(cntOld) + ",finished processes=" + str(len(prePs)))
        self.activeProc[hostname]=currPs
        return points

    def slurmjob2point (self, item):
    #{'account': 'cca', 'admin_comment': None, 'alloc_node': 'rusty2', 'alloc_sid': 18879, 'array_job_id': None, 'array_task_id': None, 'array_task_str': None, 'array_max_tasks': None, 'assoc_id': 14, 'batch_flag': 1, 'batch_host': 'worker1200', 'batch_script': None, 'billable_tres': None, 'bitflags': 0, 'boards_per_node': 0, 'burst_buffer': None, 'burst_buffer_state': None, 'command': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/iron_start', 'comment': None, 'contiguous': False, 'core_spec': None, 'cores_per_socket': None, 'cpus_per_task': 1, 'cpu_freq_gov': None, 'cpu_freq_max': None, 'cpu_freq_min': None, 'dependency': None, 'derived_ec': '0:0', 'eligible_time': 1529184154, 'end_time': 1529788955, 'exc_nodes': [], 'exit_code': '0:0', 'features': [], 'fed_origin': None, 'fed_siblings': None, 'gres': [], 'group_id': 1119, 'job_id': 69021, 'job_state': 'RUNNING', 'licenses': {}, 'max_cpus': 0, 'max_nodes': 0, 'name': 'bh7_ref2_m000097656_SS01_r01', 'network': None, 'nodes': 'worker[1200-1203]', 'nice': 0, 'ntasks_per_core': None, 'ntasks_per_core_str': 'UNLIMITED', 'ntasks_per_node': 28, 'ntasks_per_socket': None, 'ntasks_per_socket_str': 'UNLIMITED', 'ntasks_per_board': 0, 'num_cpus': 112, 'num_nodes': 4, 'partition': 'cca', 'mem_per_cpu': False, 'min_memory_cpu': None, 'mem_per_node': True, 'min_memory_node': 512000, 'pn_min_memory': 512000, 'pn_min_cpus': 28, 'pn_min_tmp_disk': 0, 'power_flags': 0, 'preempt_time': None, 'priority': 4294901702, 'profile': 0, 'qos': 'cca', 'reboot': 0, 'req_nodes': [], 'req_switch': 0, 'requeue': False, 'resize_time': 0, 'restart_cnt': 0, 'resv_name': None, 'run_time': 232295, 'run_time_str': '2-16:31:35', 'sched_nodes': None, 'shared': '0', 'show_flags': 7, 'sockets_per_board': 0, 'sockets_per_node': None, 'start_time': 1529184155, 'state_reason': 'None', 'std_err': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'std_in': '/dev/null', 'std_out': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01/out.log', 'submit_time': 1529184154, 'suspend_time': 0, 'time_limit': 10080, 'time_limit_str': '7-00:00:00', 'time_min': 0, 'threads_per_core': None, 'tres_req_str': 'cpu=112,node=4', 'tres_alloc_str': 'cpu=112,mem=2000G,node=4', 'user_id': 1119, 'wait4switch': 0, 'wckey': None, 'work_dir': '/mnt/ceph/users/dangles/SMAUG/h113_HR_sn156/bh7_ref2_m000097656_SS01_r01', 'altered': None, 'block_id': None, 'blrts_image': None, 'cnode_cnt': None, 'ionodes': None, 'linux_image': None, 'mloader_image': None, 'ramdisk_image': None, 'resv_id': None, 'rotate': False, 'conn_type': 'n/a', 'cpus_allocated': {'worker1200': 28, 'worker1201': 28, 'worker1202': 28, 'worker1203': 28}, 'cpus_alloc_layout': {}}
        points   = []

        # slurm_jobs_mon: self.slurmTime, job_id
        point =  {'measurement':'slurm_jobs_mon'}
        point['time']   =self.getUTCDateTime(self.slurmTime).isoformat()
        point['tags']   =sub_dict(item, ['job_id'])
        point['fields'] =flatten(sub_dict(item, ['run_time']))
        points.append(point)

        # slurm_jobs: submit_time, job_id, user_id
        infopoint = {'measurement':'slurm_jobs'}
        infopoint['time']   =self.getUTCDateTime(item.pop('submit_time')).isoformat()
        infopoint['tags']   =sub_dict_remove(item, ['user_id', 'job_id'])

        cpu_allocated       = item.pop('cpus_allocated')
        for v in ['run_time_str', 'time_limit_str', 'std_err', 'std_out', 'work_dir', 'cpus_alloc_layout']: del item[v]
        #nodes = item.pop('nodes')
        infopoint['fields'] =flatten(item)
        if cpu_allocated: infopoint['fields']['cpus_allocated']=repr(cpu_allocated)  #otherwise, two many fields
        
        first = True
        if first: 
           print ("slurm_jobs point " + repr(infopoint) )
           first = False
        
        points.append(infopoint)

        return points

    #pyslurm.slurm_pid2jobid only on slurm node with slurmd running
    def isSlurmProcess (self, pid):
        return True

    def isSlurmNode (self, hostname):
        return (hostname in self.nodeData )

    def getStateHack (self):
        global NodeStateHack
        
        if NodeStateHack == None:
           sample = next(iter(nodeData.values()))
           if 'node_state' in sample:
              NodeStateHack = 'node_state'
           elif 'state' in sample:
              NodeStateHack = 'state'
           else:
              print ('Cannot determine key for state in node dictionary:', dir(sample), file=sys.stderr)
              sys.exit(-1)
        return NodeStateHack

    def getUTCDateTime (self, local_ts):
        #print ("UTC " + datetime.fromtimestamp(local_ts, tz=DataReader.LOCAL_TZ).isoformat())
        return datetime.fromtimestamp(local_ts, tz=DataReader.LOCAL_TZ)

def main():
    app = DataReader();
    app.run();


main()
