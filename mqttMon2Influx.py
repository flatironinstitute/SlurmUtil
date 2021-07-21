#!/usr/bin/env python
import argparse, json, os.path, pdb, threading, time 

from collections import defaultdict as DDict
from datetime    import datetime, timezone, timedelta
from operator    import itemgetter

import influxdb
import paho.mqtt.client as mqtt
import pyslurm

import config, EmailSender, MyTool, querySlurm

logger   = config.logger

ignore_count = 0
class InfluxWriter (threading.Thread):
    MAX_SIZE = 10000

    def __init__(self, influxServer='scclin011', influxPort=8086, influxDB='slurmdb', ifx_interval=120, testMode = False):
        threading.Thread.__init__(self)

        self.influx_host   = influxServer
        self.influx_port   = influxPort
        self.influx_db     = influxDB
        self.influx_user   = "yliu"
        self.influx_client = self.connectInflux ()
        self.source        = []
        self.interval      = ifx_interval
        self.test_mode     = testMode
        logger.info("Start InfluxWriter with influx_client={}, interval={}, test={}".format(self.influx_client._baseurl, self.interval, self.test_mode))
        self.influx_client.close()

    def connectInflux (self):
        #return influxdb.InfluxDBClient(host, port, user, "", db, timeout=10)
        #using UDP
        #client = InfluxDBClient(host, db, use_udp=True, udp_port=4444)
        #logger.info("connect {}:{}-{} {}".format(self.influx_host, self.influx_port, self.influx_user, "", self.influx_db)
        client = influxdb.InfluxDBClient(self.influx_host, self.influx_port, self.influx_user, "", self.influx_db)
        logger.info("return client {}".format(client))
        return client

    def writeInflux (self, points, ret_policy="autogen", t_precision="s"):
        try:
           logger.info  ("writeInflux {} {} pts".format(ret_policy, len(points)))
           if self.test_mode:
              logger.info("writeInflux {} points with ret_policy={}\n".format(len(points), ret_policy))
              for idx in range(min(len(points),5)):
                  logger.info("{}".format(points[idx]))
                  logger.info("...")
              return True
           else:
              #ret = self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision=t_precision, batch_size=BATCH_SIZE)
              ret = self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision=t_precision, batch_size=InfluxWriter.MAX_SIZE)
              logger.info("writeInflux return {}".format(ret))
              return ret
        except influxdb.exceptions.InfluxDBClientError as err:
           logger.error ("writeInflux {} ({} points) client ERROR: {}".format(ret_policy, len(points), err))
           return False
        except influxdb.exceptions.InfluxDBServerError as err:
           logger.error ("writeInflux {} ({} points) server ERROR: {}".format(ret_policy, len(points), err))
           return False

    def addSource(self, source):
        self.source.append(source)

    def addSourcePoints (self, rp_points, source_points):
        for rp, point_lst in source_points.items():
            if rp in rp_points:
               rp_points[rp].extend(point_lst)
            else:
               rp_points[rp] = point_lst
            #logger.debug("{}: points number={}".format(rp, len(rp_points[rp])))
        
    def run(self):
        #pdb.set_trace()
        #points = []
        rp_points = {}   #{"retention_policy":[points, ...]
        time.sleep (10)
        while True:
          for s in self.source:
              #points.extend(s.retrieveInfluxPoints ())
              self.addSourcePoints (rp_points, s.retrieveInfluxPoints ())
              sum_s = sum([len(points) for points in rp_points.values()])
              logger.info ("InfluxWriter have {} points after checking source {}".format(sum_s, s))

          if not rp_points: 
              continue
          self.influx_client = self.connectInflux ()
          for rp, points in rp_points.items():
              ret   = self.writeInflux (points, ret_policy=rp)
              time.sleep(5)           # sleep 5 seconds between write
              rp_points[rp].clear()
              #if ret:
              #   rp_points[rp].clear()
              #else: 
                 # show the users list, still failed, reconnect with it
              #   logger.error("write_points return False")

          self.influx_client.close()
          time.sleep (self.interval)

class MQTTReader (threading.Thread):
    TS_FNAME = 'host_up_ts.txt'

    #mqtt client to receive data and save it in influx
    #two threads: one for mqtt client, one for the reader
    def __init__(self, mqttServer='mon5.flatironinstitute.org', test_mode=False):
        threading.Thread.__init__(self)
        self.lock          = threading.Lock()   #guard the message list hostperf_msgs, ...

        self.mqtt_client   = self.connectMqtt   (mqttServer)
        self.hostperf_msgs = []
        #self.hostinfo_msgs = []
        self.hostproc_msgs = []
        #self.hostproc_msgs_byHost = []

        self.startTime     = datetime.now().timestamp()
        self.cpuinfo       = {} 		#static information
        self.nodeUidTs2points = DDict(lambda: DDict(lambda: DDict(dict))) #{node:{uid:{ts:{pid:procPoint, ...]},...}, hostproc2point provider, create_uid_point use it
        self.test_mode     = test_mode      # whether to write nodeUidTs2points to file
       
        # read host_up_ts, TODO: from event_table slurmdb
        if os.path.isfile(self.TS_FNAME):
           with open(self.TS_FNAME, 'r') as f:
                self.cpu_up_ts = json.load(f)
        else:
           self.cpu_up_ts      = {}
        self.cpu_up_ts_count   = 0

        self.node2tsPidsCache = Node2PidsCache (test_mode=test_mode) #data structure to avoid duplicate information
        logger.info("Start MQTTReader with mqtt_client={}".format(self.mqtt_client))

    def run(self):
        # Asynchronously receive messages
        #pdb.set_trace()
        self.mqtt_client.loop_forever()

    def connectMqtt (self, host):
        mqtt_client            = mqtt.Client()
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_message = self.on_message
        mqtt_client.connect(host)

        return mqtt_client

    def on_connect(self, client, userdata, flags, rc):
        #self.mqtt_client.subscribe("cluster/hostprocesses/worker1000")
        #logger.info("on_connect with code %d." %(rc) )
        #self.mqtt_client.subscribe("cluster/hostinfo/#")
        self.mqtt_client.subscribe("cluster/hostperf/#")
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    # put into message queue with incoming message
    def on_message(self, client, userdata, msg):
        data     = json.loads(msg.payload)
        if ( self.startTime - data['hdr']['msg_ts'] > 25200 ): # 7 days
           logger.debug("Skip old message=" + repr(data['hdr']))
           return

        with self.lock:
          if   ( data['hdr']['msg_type'] == 'cluster/hostperf' ):
           self.hostperf_msgs.append(data)
          #elif ( data['hdr']['msg_type'] == 'cluster/hostinfo' ):
          # self.hostinfo_msgs.append(data)
          elif ( data['hdr']['msg_type'] == 'cluster/hostprocesses' ):       #also trigger to updateSlurm
           self.hostproc_msgs.append(data)

    def consume_hostinfo_msgs(self):
        with self.lock:
             hostinfo_msgs      = self.hostinfo_msgs
             self.hostinfo_msgs = []
        return hostinfo_msgs
    
    def consume_hostperf_msgs(self):
        with self.lock:
             hostperf_msgs      = self.hostperf_msgs
             self.hostperf_msgs = []
        return hostperf_msgs

    def consume_hostproc_msgs (self):
        with self.lock:
             hostproc_msgs      = self.hostproc_msgs
             self.hostproc_msgs = []
        hostproc_msgs = self.filter_hostproc_msgs (hostproc_msgs)
        return hostproc_msgs        

    # retrieve points
    def retrieveInfluxPoints (self):
        global ignore_count
        #self.pyslurmQueryTime= datetime.now().timestamp()
        #pdb.set_trace()
        rp_points = DDict(list)      #{'autogen':[point...]}

        self.nodeData = pyslurm.node().get()

        #autogen.cpu_info: time, hostname, total_socket, total_thread, total_cores, cpu_model, is_vm
        #hostinfo_msgs = self.consume_hostinfo_msgs()
        #hostinfo      = self.process_list(hostinfo_msgs, self.hostinfo2point)
        #rp_points['autogen'] = hostinfo
        #logger.debug("MQTTReader generate {} hostinfo points".format(len(hostinfo)))

        #autogen.cpu_load: time, hostname, proc_*, load_*, cpu_*, mem_*, net_*, disk_*
        hostperf_msgs = self.consume_hostperf_msgs()
        hostperf      = self.process_list(hostperf_msgs, self.hostperf2point)
        rp_points['autogen'] = hostperf
        logger.debug("MQTTReader generate {} hostperf points".format(len(hostperf)))

        #autogen.cpu_proc_info, job_proc_mon
        hostproc_msgs = self.consume_hostproc_msgs()
        ignore_count  = 0
        self.process_list_add(rp_points, hostproc_msgs, self.hostproc2point)
        logger.debug("MQTTReader generate {} {} points after adding hostproc, ignore {} points".format(len(rp_points['autogen']), len(rp_points['short_term']), ignore_count))

        #points = hostinfo + hostperf + hostproc
        if self.cpu_up_ts_count > 0:
           with open (self.TS_FNAME, 'w') as f:
                json.dump(self.cpu_up_ts, f)
           self.cpu_up_ts_count = 0

        self.node2tsPidsCache.writeFile ()         #save intermediate data structure, node2
        return rp_points

    def filter_hostproc_msgs (self, hostproc_msgs):
        logger.debug ("before filter {} msgs".format(len(hostproc_msgs)))
        hostproc_msgs.sort(key=lambda msg: msg['hdr']['msg_ts'], reverse=True)   #latest msg first
        host_dict = {}
        rlt       = []
        for msg in hostproc_msgs:
            hostname = msg['hdr']['hostname']
            if hostname not in host_dict:  # save the latest msg from host
               if self.isSlurmNode(hostname):
                  rlt.append (msg)               
               host_dict[hostname] = True
            # else filtered out
        rlt.reverse()      # in increasing ts order
        logger.debug ("after filter {} msgs".format(len(rlt)))
        return rlt

    def process_list (self, msg_list, item_func):
        points=[]
        for idx,msg in enumerate(msg_list):
            pts = item_func(msg)
            if pts:         
               points.extend(pts)
        return points

    def process_list_add (self, rp_points, msg_list, item_func):
        for msg in msg_list:
            points = item_func(msg)
            for rp, lst in points.items():
                rp_points[rp].extend(lst)
        return rp_points

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
           if up_ts not in self.cpu_up_ts[host]:    #up_ts != self.cpu_up_ts[host][-1]
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
        ts    = msg['hdr']['msg_ts']
        point           = {'measurement':'cpu_load', 'time': (int)(ts)}
        point['tags']   = {'hostname'   : msg['hdr']['hostname']}
        point['fields'] = MyTool.flatten(MyTool.sub_dict(msg, ['cpu_times', 'mem', 'net_io', 'disk_io']))
        point['fields'].update ({'load_1min':msg['load'][0], 'load_5min':msg['load'][1], 'load_15min':msg['load'][2], 'proc_total':msg['proc_total'], 'proc_run': msg['proc_run']})
        return [point]

    def hostproc2point (self, msg):
        global ignore_count
        #{'processes': [{'status': 'sleeping', 'uid': 1083, 'jid':11111, 'mem': {'lib': 0, 'text': 90, 'shared': 13, 'data': 48, 'vms': 115, 'rss': 169}, 'pid': 23825, 'cmdline': ['/bin/bash', '/cm/local/apps/slurm/var/spool/job65834/slurm_script'], 'create_time': 1528790822.57, 'io': {'write_bytes': 405, 'read_count': 97, 'read_bytes': 64235, 'write_count': 10}, 'num_fds': 4, 'num_threads': 1, 'name': 'slurm_script', 'ppid': 23821, 'cpu': {'system_time': 0.21, 'affinity': [0, 1], 'user_time': 0.17}}, 
        #...
        #'hdr': {'hostname': 'worker1000', 'msg_process': 'cluster_host_mon', 'msg_type': 'cluster/hostprocesses', 'msg_ts': 1528901819.82538}} 
        #pdb.set_trace()
        ts   = (int)(msg['hdr']['msg_ts'])
        node = msg['hdr']['hostname']
        if ( len(msg['processes']) == 0 ):   return {}

        new_procs, cont_procs, pre_cont_procs, pre_done_procs, pre_ts = self.node2tsPidsCache.addMQTTMsg(node, ts, msg)
        if (not new_procs) and (not cont_procs):
           return {}

        rp_points={'autogen':[], 'short_term':[]}
        for proc in new_procs:
            rp_points['short_term'].append(self.createProcMonPoint  (node, ts, proc))

        cont_pids    = [proc['pid'] for proc in cont_procs]
        pre_pids     = [proc['pid'] for proc in pre_cont_procs]
        check        = True
        if cont_pids != pre_pids:
            logger.info("Missing some items in pre_cont_procs {} or cont_procs {}".format(pre_cont_procs,cont_procs))
            check    = False
        for idx_proc, proc in enumerate(cont_procs):
            if check:
               pre_proc  = pre_cont_procs[idx_proc] if idx_proc<len(pre_cont_procs) else None
               cpu_delta = proc.get('cpu',{}).get('system_time',0)+proc.get('cpu',{}).get('user_time',0) - pre_proc.get('cpu',{}).get('system_time',0)-pre_proc.get('cpu',{}).get('user_time',0) 
            else:
               cpu_delta = 10
            if cpu_delta > 1:
               rp_points['short_term'].append(self.createProcMonPoint  (node, ts, proc))
            else:
               ignore_count += 1

        for proc in new_procs:
            rp_points['short_term'].append(self.createProcInfoPoint (node, proc))
        for proc in pre_done_procs:
            rp_points['short_term'].append(self.createProcInfoPoint (node, proc, end_time=int((pre_ts+ts)/2)))

        uids = set([proc['uid'] for proc in msg['processes']])
        for uid in uids:
            u_new_procs      = [proc for proc in new_procs      if proc['uid'] == uid]
            u_cont_procs     = [proc for proc in cont_procs     if proc['uid'] == uid]
            u_pre_cont_procs = [proc for proc in pre_cont_procs if proc['uid'] == uid]
            if (u_new_procs or u_cont_procs) and (u_pre_cont_procs):
               #if ts - pre_ts < 600:  # longer than 10 minutes, ignore
               rp_points['autogen'].append(self.createUidMonPoint(node, uid, ts, u_new_procs, u_cont_procs, pre_ts, u_pre_cont_procs))
        jids = set([proc['jid'] for proc in msg['processes']])
        for jid in jids:
            j_new_procs      = [proc for proc in new_procs      if proc['jid'] == jid]
            j_cont_procs     = [proc for proc in cont_procs     if proc['jid'] == jid]
            j_pre_cont_procs = [proc for proc in pre_cont_procs if proc['jid'] == jid]
            if (j_new_procs or j_cont_procs) and (j_pre_cont_procs):
               #if ts - pre_ts < 600:  # longer than 10 minutes, ignore
               rp_points['autogen'].append(self.createJidMonPoint(node, jid, ts, j_new_procs, j_cont_procs, pre_ts, j_pre_cont_procs))

        #logger.debug("rp_points=autogen:{} short_term:{}".format(len(rp_points['autogen']), len(rp_points['short_term'])))
        return rp_points

    def createProcInfoPoint (self, node, proc, end_time=None):
        #change 01/17/2021: change to short_term.node_proc_info, hostname+jid as tag
        point                     = {'measurement':'node_proc_info2', 'time': (int)(proc['create_time'])}
        point['tags']             = {'hostname':node, 'pid': proc['pid']}
        point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['ppid', 'uid', 'jid', 'name', 'cmdline']))
        if end_time:
           point['fields']['end_time'] = end_time
           point['fields']['status']   = proc['status']
           point['fields'].update (MyTool.flatten(MyTool.sub_dict(proc, ['mem', 'io', 'num_fds', 'cpu'], default=0)))
        return point

    def createProcMonPoint (self, node, ts, proc):
        #point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['create_time', 'jid', 'mem', 'io', 'num_fds', 'cpu'], default=0))
        #change 01/23/2020: change name from cpu_proc_mon to node_proc_mon
        #                   remove uid from tags
        #change 01/17/2021: change to short_term.node_proc_mon, hostname+jid as tag
        #change 01/19/2021: change to short_term.node_proc_mon, hostname+jid+pid as tag
        
        #point                     = {'measurement':'node_proc_mon1', 'time': ts}
        #point['tags']             = {'hostname':node, 'jid':proc['jid'], 'pid':proc['pid']}
        #point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['uid', 'io', 'num_fds', 'cpu'], default=0))
        point                     = {'measurement':'node_proc_mon', 'time': ts}
        point['tags']             = {'hostname':node, 'pid':proc['pid']}
        point['fields']           = MyTool.flatten(MyTool.sub_dict(proc, ['uid', 'jid', 'io', 'num_fds', 'cpu'], default=0))
        point['fields']['status'] = querySlurm.SlurmStatus.getStatusID(proc['status'])
        point['fields']['mem_data']  = round(proc['mem']['data']   / 1024)
        point['fields']['mem_rss']   = round(proc['mem']['rss']    / 1024) 
        point['fields']['mem_shared']= round(proc['mem']['shared'] / 1024)
        point['fields']['mem_text']  = round(proc['mem']['text']   / 1024)
        point['fields']['mem_vms']   = round(proc['mem']['vms']    / 1024)
        if 'cpu_affinity' in point['fields']:
           point['fields'].pop('cpu_affinity')

        return point

    def createUidMonPoint(self, node, uid, curr_ts, new_procs, cont_procs, pre_ts, pre_cont_procs):
        point = self.createMonPoint ('cpu_uid_mon', node, 'uid', uid, curr_ts, new_procs, cont_procs, pre_ts, pre_cont_procs)
        #point['fields']['jid'] = jid
        return point

    def createJidMonPoint(self, node, jid, curr_ts, new_procs, cont_procs, pre_ts, pre_cont_procs):
        return self.createMonPoint ('cpu_jid_mon', node, 'jid', jid, curr_ts, new_procs, cont_procs, pre_ts, pre_cont_procs)

    #precondition: (u_new_procs or u_cont_procs) and (u_pre_cont_procs):
    def createMonPoint(self, mname, node, idName, idTag, curr_ts, new_procs, cont_procs, pre_ts, pre_cont_procs):
        point        = {'measurement':mname, 'time':curr_ts, 'tags': {'hostname':node, idName:idTag}, 'fields': {}}
        curr_num     = [(proc['cpu']['system_time'],proc['cpu']['user_time'],proc['io']['read_bytes'],proc['io']['write_bytes'],proc['mem']['data'],proc['mem']['rss'],proc['mem']['shared'],proc['mem']['text'],proc['mem']['vms'],proc['num_fds']) for proc in new_procs+cont_procs]
        curr_sum     = list(map(sum, zip(*curr_num)))

        period   = curr_ts - pre_ts
        if period > 500:
           logger.debug("createUidMonPoint: Node{}, Period is {} bigger than 120 seconds between {} and {}. ".format(node, period, pre_ts, curr_ts))
        pre_num      = [(proc['cpu']['system_time'],proc['cpu']['user_time'],proc['io']['read_bytes'],proc['io']['write_bytes']) for proc in pre_cont_procs]
        pre_sum      = list(map(sum, zip(*pre_num)))
        point['fields']['cpu_system_util'] = max(0.0,round((curr_sum[0] - pre_sum[0])/period,4))
        point['fields']['cpu_user_util']   = max(0.0,round((curr_sum[1] - pre_sum[1])/period,4))
        point['fields']['io_read_rate']    = max(0.0,round((curr_sum[2] - pre_sum[2])/period,4))
        point['fields']['io_write_rate']   = max(0.0,round((curr_sum[3] - pre_sum[3])/period,4))
        point['fields']['mem_data_K']      = round(curr_sum[4] / 1024)
        point['fields']['mem_rss_K']       = round(curr_sum[5] / 1024) # modified 09/13/2019
        point['fields']['mem_shared_K']    = round(curr_sum[6] / 1024)
        point['fields']['mem_text_K']      = round(curr_sum[7] / 1024)
        point['fields']['mem_vms_K']       = round(curr_sum[8] / 1024)
        point['fields']['num_fds']         = curr_sum[9]
        point['fields']['num_procs']       = len(new_procs) + len(cont_procs)  #added 03/17/2021
        
        return point

    #pyslurm.slurm_pid2jobid only on slurm node with slurmd running
    def isSlurmNode (self, hostname):
        return (hostname in self.nodeData )

#A dictionary that served as a cache to avoid duplicate information
class Node2PidsCache:
   TS_ACCURACY = 0.01

   def __init__ (self, savFile='node2pids.cache', test_mode=False):
       self.filename    = savFile
       self.test_mode   = test_mode
       sav              = MyTool.readFile(savFile)
       if sav:
          self.node2TsPids = DDict(lambda:DDict(), sav)
       else:
          self.node2TsPids = DDict(lambda: DDict())  #{node: {curr_ts: curr_pids: pre_ts: pre_pids:}}

   #pids is set of pids on node reported at time ts
   #return new_procs, cont_procs, pre_cont_procs, node2TsPids['pre_ts']
   def addMQTTMsg (self, node, ts, msg):
       node2TsPids = self.node2TsPids[node]
       if node2TsPids and (ts < node2TsPids['curr_ts'] + self.TS_ACCURACY):
          logger.info ("Node2PidsCach::addMQTTMsg: Node {}, Deplicate or out of order timestamp {} compared with {}. Ignore.".format(node, ts, node2TsPids['curr_ts']))
          return [], [], [], [], None

       #ts > self.curr_ts
       pids        = set([proc['pid'] for proc in msg['processes']])
       if not node2TsPids: # the first one
          node2TsPids = {'curr_ts': ts, 'curr_pids': pids, 'curr_msg':msg, 'pre_ts': None,                   'pre_pids': set(),                    'pre_msg':{'processes':[]}}
       else:  # update node2TsPids
          node2TsPids = {'curr_ts': ts, 'curr_pids': pids, 'curr_msg':msg, 'pre_ts': node2TsPids['curr_ts'], 'pre_pids': node2TsPids['curr_pids'], 'pre_msg':node2TsPids['curr_msg']}
       self.node2TsPids[node] = node2TsPids   # update self.nodeTsPids

       cont_pids      = node2TsPids['pre_pids'].intersection(node2TsPids['curr_pids'])
       done_pids      = node2TsPids['pre_pids'].difference  (cont_pids)
       new_pids       = node2TsPids['curr_pids'].difference (cont_pids)
       new_procs      = [proc for proc in node2TsPids['curr_msg']['processes'] if proc['pid'] in new_pids]
       pre_done_procs = [proc for proc in node2TsPids['pre_msg']['processes']  if proc['pid'] in done_pids]
       cont_pids      = sorted(cont_pids)
       cont_procs     = [proc for proc in node2TsPids['curr_msg']['processes'] if proc['pid'] in cont_pids]
       pre_cont_procs = [proc for proc in node2TsPids['pre_msg']['processes']  if proc['pid'] in cont_pids]

       return new_procs, cont_procs, pre_cont_procs, pre_done_procs, node2TsPids['pre_ts']

   def writeFile (self):
       if not self.test_mode:
          MyTool.writeFile(self.filename, dict(self.node2TsPids))
       else:
          logger.debug("writeFile test")

class PyslurmReader (threading.Thread):
    def __init__(self, py_interval=300):
        threading.Thread.__init__(self)

        self.points   = []
        self.lock     = threading.Lock()       #protect self.points as it is read and write by different threads
        self.interval = py_interval
   
        self.sav_job_dict = {}               #save job_id:json.dumps(infopoint)   03/15/2021 not used
        self.sav_node_dict = {}              #save name:json.dumps(infopoint)
        self.sav_part_dict = {}              #save value
        self.sav_qos_dict  = {}              #save value
        self.sav_res_dict  = {}              #save value
        logger.info("Init PyslurmReader with interval={}".format(self.interval))

    def run(self):
        #pdb.set_trace()
        logger.info("Start running PyslurmReader ...")
        while True:
          # pyslurm query
          ts       = int(datetime.now().timestamp())
          job_dict = pyslurm.job().get()
          node_dict= pyslurm.node().get()
          part_dict= pyslurm.partition().get()
          qos_dict = pyslurm.qos().get()
          #res_dict = pyslurm.reservation().get()
          res_dict = {}  #TODO: pyslurm reservation coredump ERROR
          #js_dict  = pyslurm.jobstep().get()

          #convert to points
          points   = []
          for jid,job in job_dict.items():
              self.slurmJob2point(ts, job, points)
          finishJob = [jid for jid in self.sav_job_dict.keys() if jid not in job_dict.keys()]
          #logger.debug ("PyslurmReader::run: Finish jobs {}".format(finishJob))
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

          if res_dict and (json.dumps(res_dict) != json.dumps(self.sav_res_dict)):
              for rname, res in res_dict.items():
                 self.slurmReservation2point(ts, rname, res, points)
              self.sav_res_dict = res_dict

          with self.lock:
              self.points.extend(points)

          time.sleep (self.interval)

    #return the points, called by InfluxDBWriter 
    def retrieveInfluxPoints (self):
        with self.lock:
            rlt         = self.points
            self.points = []
        #logger.debug("pyslurm points={}".format(rlt))
        return {'autogen':rlt}

    def slurmJob2point (self, ts, item, points):
    #{'account': 'scc', 'accrue_time': 'Unknown', 'admin_comment': None, 'alloc_node': 'rusty1', 'alloc_sid': 3207927, 'array_job_id': None, 'array_task_id': None, 'array_task_str': None, 'array_max_tasks': None, 'assoc_id': 153, 'batch_flag': 0, 'batch_features': None, 'batch_host': 'worker1085', 'billable_tres': 28.0, 'bitflags': 1048576, 'boards_per_node': 0, 'burst_buffer': None, 'burst_buffer_state': None, 'command': None, 'comment': None, 'contiguous': False, 'core_spec': None, 'cores_per_socket': None, 'cpus_per_task': 1, 'cpus_per_tres': None, 'cpu_freq_gov': None, 'cpu_freq_max': None, 'cpu_freq_min': None, 'dependency': None, 'derived_ec': '0:0', 'eligible_time': 1557337982, 'end_time': 1588873982, 'exc_nodes': [], 'exit_code': '0:0', 'features': [], 'group_id': 1023, 'job_id': 240240, 'job_state': 'RUNNING', 'last_sched_eval': '2019-05-08T13:53:02', 'licenses': {}, 'max_cpus': 0, 'max_nodes': 0, 'mem_per_tres': None, 'name': 'bash', 'network': None, 'nodes': 'worker1085', 'nice': 0, 'ntasks_per_core': None, 'ntasks_per_core_str': 'UNLIMITED', 'ntasks_per_node': 0, 'ntasks_per_socket': None, 'ntasks_per_socket_str': 'UNLIMITED', 'ntasks_per_board': 0, 'num_cpus': 28, 'num_nodes': 1, 'partition': 'scc', 'mem_per_cpu': False, 'min_memory_cpu': None, 'mem_per_node': True, 'min_memory_node': 0, 'pn_min_memory': 0, 'pn_min_cpus': 1, 'pn_min_tmp_disk': 0, 'power_flags': 0, 'preempt_time': None, 'priority': 4294877910, 'profile': 0, 'qos': 'gen', 'reboot': 0, 'req_nodes': [], 'req_switch': 0, 'requeue': False, 'resize_time': 0, 'restart_cnt': 0, 'resv_name': None, 'run_time': 4308086, 'run_time_str': '49-20:41:26', 'sched_nodes': None, 'shared': '0', 'show_flags': 23, 'sockets_per_board': 0, 'sockets_per_node': None, 'start_time': 1557337982, 'state_reason': 'None', 'std_err': None, 'std_in': None, 'std_out': None, 'submit_time': 1557337982, 'suspend_time': 0, 'system_comment': None, 'time_limit': 'UNLIMITED', 'time_limit_str': 'UNLIMITED', 'time_min': 0, 'threads_per_core': None, 'tres_alloc_str': 'cpu=28,mem=500G,node=1,billing=28', 'tres_bind': None, 'tres_freq': None, 'tres_per_job': None, 'tres_per_node': None, 'tres_per_socket': None, 'tres_per_task': None, 'tres_req_str': 'cpu=1,node=1,billing=1', 'user_id': 1022, 'wait4switch': 0, 'wckey': None, 'work_dir': '/mnt/home/apataki', 'cpus_allocated': {'worker1085': 28}, 'cpus_alloc_layout': {'worker1085': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27]}}
        # remove empty values
        job_id = item['job_id']
        MyTool.remove_dict_empty(item)
        for v in ['run_time_str', 'time_limit_str']: item.pop(v, None)

        # pending_job
        if item['job_state'] == 'PENDING':
           pendpoint          = {'measurement':'slurm_pending', 'time': ts} 
           pendpoint['tags']  = MyTool.sub_dict       (item, ['state_reason', 'job_id'])
           pendpoint['fields']= MyTool.sub_dict       (item, ['user_id', 'submit_time', 'account', 'qos', 'partition', 'tres_req_str', 'last_sched_eval', 'time_limit', 'start_time']) #switch from tres_per_node to tres_req_str 06/28/2019
           points.append(pendpoint)

        # slurm_job_mon: ts, job_id
        point =  {'measurement':'slurm_job_mon1', 'time': ts}  #03/23/2020 change
        point['tags']   = MyTool.sub_dict_remove       (item, ['job_id'])
        point['fields'] = MyTool.sub_dict_exist_remove (item, ['user_id', 'job_state', 'state_reason', 'run_time', 'suspend_time', 'num_cpus', 'num_nodes', 'tres_req_str']) # add tres_req_str 06/28/2019
        points.append(point)

        return points

    def slurmNode2point (self, ts, item, points):
#{'arch': 'x86_64', 'boards': 1, 'boot_time': 1560203329, 'cores': 14, 'core_spec_cnt': 0, 'cores_per_socket': 14, 'cpus': 28, 'cpu_load': 2, 'cpu_spec_list': [], 'features': 'k40', 'features_active': 'k40', 'free_mem': 373354, 'gres': ['gpu:k40c:1', 'gpu:k40c:1'], 'gres_drain': 'N/A', 'gres_used': ['gpu:k40c:0(IDX:N/A)', 'mic:0'], 'mcs_label': None, 'mem_spec_limit': 0, 'name': 'workergpu00', 'node_addr': 'workergpu00', 'node_hostname': 'workergpu00', 'os': 'Linux 3.10.0-957.10.1.el7.x86_64 #1 SMP Mon Mar 18 15:06:45 UTC 2019', 'owner': None, 'partitions': ['gpu'], 'real_memory': 384000, 'slurmd_start_time': 1560203589, 'sockets': 2, 'threads': 1, 'tmp_disk': 0, 'weight': 1, 'tres_fmt_str': 'cpu=28,mem=375G,billing=28,gres/gpu=2', 'version': '18.08', 'reason': None, 'reason_time': None, 'reason_uid': None, 'power_mgmt': {'cap_watts': None}, 'energy': {'current_watts': 0, 'base_consumed_energy': 0, 'consumed_energy': 0, 'base_watts': 0, 'previous_consumed_energy': 0}, 'alloc_cpus': 0, 'err_cpus': 0, 'state': 'IDLE', 'alloc_mem': 0}
#REBOOT state, boot_time and slurmd_start_time is 0

        # slurm_node_mon: ts, name
        point           = {'measurement':'slurm_node_mon2', 'time': ts}  #03/19/2021, replace slurm_node_mon1, 03/23/2020, replace slurm_node_mon
        point['tags']   = {'hostname': item['node_hostname']} 
        point['fields'] = MyTool.sub_dict_nonempty (item, ['name', 'boot_time', 'slurmd_start_time', 'cpus', 'cpu_load', 'alloc_cpus', 'state', 'free_mem', 'gres', 'gres_used', 'partitions', 'reason', 'reason_time', 'reason_uid', 'err_cpus', 'alloc_mem'])
        MyTool.dict_complex2str(point['fields'])
        points.append(point)

        # slurm_jobs: slurmd_start_time
        if (('boot_time' in item) and (not MyTool.emptyValue(item['boot_time']))):
           infopoint           = {'measurement':'slurm_node', 'time': (int)(item['boot_time'])}
           infopoint['tags']   = {'hostname': item['node_hostname']}
           infopoint['fields'] = MyTool.sub_dict_nonempty (item, ['name', 'slurmd_start_time', 'cpus', 'partitions', 'arch', 'boards', 'cores', 'features', 'gres', 'node_addr', 'os', 'sockets', 'threads', 'tres_fmt_str'])
           MyTool.dict_complex2str(infopoint['fields'])
      
           newValue = json.dumps(infopoint)
           name     = item['name']
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

        MyTool.dict_complex2str(point['fields'])
        points.append(point)

        return points

    def slurmQOS2point (self, ts, name, item, points):
#{'description': 'cca', 'flags': 0, 'grace_time': 0, 'grp_jobs': 4294967295, 'grp_submit_jobs': 4294967295, 'grp_tres': '1=6000', 'grp_tres_mins': None, 'grp_tres_run_mins': None, 'grp_wall': 4294967295, 'max_jobs_pu': 4294967295, 'max_submit_jobs_pu': 4294967295, 'max_tres_mins_pj': None, 'max_tres_pj': None, 'max_tres_pn': None, 'max_tres_pu': '1=840', 'max_tres_run_mins_pu': None, 'max_wall_pj': 10080, 'min_tres_pj': None, 'name': 'cca', 'preempt_mode': 'OFF', 'priority': 15, 'usage_factor': 1.0, 'usage_thres': 4294967295.0}

        MyTool.remove_dict_empty(item)

        point           = {'measurement':'slurm_qos', 'time': ts}
        point['tags']   = {'name': item.pop('name')}
        point['fields'] = item

        MyTool.dict_complex2str(point['fields'])
        points.append(point)

        return points

    def slurmReservation2point (self, ts, name, item, points):
#{'andras_test': {'accounts': [], 'burst_buffer': [], 'core_cnt': 28, 'end_time': 1591885549, 'features': [], 'flags': 'MAINT,SPEC_NODES', 'licenses': {}, 'node_cnt': 1, 'node_list': 'worker1010', 'partition': None, 'start_time': 1560349549, 'tres_str': ['cpu=28'], 'users': ['root', 'apataki', 'ifisk', 'carriero', 'ntrikoupis']}}
        MyTool.remove_dict_empty(item)

        point           = {'measurement':'slurm_reservation', 'time': ts}
        point['tags']   = {'name': name}
        point['fields'] = item

        MyTool.dict_complex2str(point['fields'])
        points.append(point)

        return points

def startInfluxThread (influxServer, influxPort, influxDB, ifx_interval, mqtt_thd, pyslm_thd, testMode):
    ifx_thd = InfluxWriter    (influxServer, influxPort, influxDB, ifx_interval, testMode)
    ifx_thd.addSource (mqtt_thd)
    ifx_thd.addSource (pyslm_thd)
    ifx_thd.start()
    return ifx_thd

def startPyslurmThread(py_interval):
    pyslm_thd  = PyslurmReader (py_interval)
    pyslm_thd.start()
    return pyslm_thd

def startMQTTThread(mqttServer, testMode):
    mqtt_thd   = MQTTReader(mqttServer, test_mode=testMode)
    mqtt_thd.start()
    return mqtt_thd

def main(influxServer, influxPort, influxDB, ifx_interval, py_interval, mqttServer, testMode=False):
    mqtt_thd   = startMQTTThread (mqttServer, testMode)
    time.sleep(5)
    pyslm_thd  = startPyslurmThread(py_interval)
    time.sleep(5)
    ifx_thd    = startInfluxThread(influxServer, influxPort, influxDB, ifx_interval, mqtt_thd, pyslm_thd, testMode)

    while True:
       if not mqtt_thd.is_alive():
          EmailSender.sendMessage ("ERROR: MQTTReader thread is dead. Restart it!", "Check it!")
          logger.error("ERROR: MQTTReader thread is dead. Restart it!")
          mqtt_thd   = startMQTTThread(mqttServer, testMode)
          ifx_thd    = startInfluxThread(influxServer, influxPort, influxDB, ifx_interval, mqtt_thd, pyslm_thd, testMode)
       if not pyslm_thd.is_alive():
          EmailSender.sendMessage ("ERROR: MQTTReader thread is dead. Restart it!", "Check it!")
          logger.error("ERROR: Pyslurm thread is dead. Restart it!")
          pyslm_thd  = startPyslurmThread(py_interval)
          ifx_thd    = startInfluxThread(influxServer, influxPort, influxDB, ifx_interval, mqtt_thd, pyslm_thd, testMode)
       if not ifx_thd.is_alive():
          EmailSender.sendMessage ("ERROR: MQTTReader thread is dead. Restart it!", "Check it!")
          logger.error("ERROR: Influx thread is dead. Restart it!")
          ifx_thd    = startInfluxThread(influxServer, influxPort, influxDB, ifx_interval, mqtt_thd, pyslm_thd, testMode)

          logger.info("New Influx thread {}.".format(ifx_thd))
          
       time.sleep (600)  # check every 10 minutes

if __name__=="__main__":
   #Usage: python mqtMon2Influx.py
   parser = argparse.ArgumentParser (description='Start a deamon to save mqtt and pyslurm information to InfluxDB.')
   #parser.add_argument('influxServer', help='The hostname of an InfluxDB server.')
   #parser.add_argument('-l', '--logfile',  help='The name of the logfile.')
   #parser.add_argument('--debug',       action='store_true', help='Enable debug mode in which data will not be saved to InfluxDB.')
   parser.add_argument('-c',     '--configFile',  help='The name of the config file.')
   parser.add_argument('--test', action='store_true',  help='The name of the config file.')
   args         = parser.parse_args()
   test         = args.test

   configFile   = args.configFile
   if configFile:
      config.readConfigFile(configFile)
   cfg          = config.APP_CONFIG
   influxServer = cfg['influxdb']['host']
   influxPort   = cfg['influxdb']['port']
   influxDB     = cfg['influxdb']['db']
   ifx_interval = cfg['influxdb']['write_interval']
   py_interval  = cfg['influxdb']['query_pyslurm_interval']
   mqttServer   = cfg['mqtt']['host']
   print("Start ... influxServer={}:{}-{}, test={}".format(influxServer, influxPort, influxDB, test))
   main(influxServer, influxPort, influxDB, ifx_interval, py_interval, mqttServer, test)


