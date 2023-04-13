import argparse
import _pickle as cPickle
import urllib.request as urllib2
import json, logging, os, pwd, sys, threading, time, zlib
import multiprocessing
import paho.mqtt.client as mqtt
import pyslurm
import pdb
import config
import MyTool

from collections import defaultdict as DDict
from IndexedDataFile import IndexedHostData
from mqttMon2Influx import Node2PidsCache

logger   = config.logger        #use app name, report to localhost:8126/data/log
#MyTool.getFileLogger('mqttMonitor', logging.DEBUG)  # use module name

class MQTTReader(multiprocessing.Process):
    def __init__(self, mqtt_server, mqtt_msgs):
        super().__init__()
        # connect mqtt
        self.name                   = "MQTTReader"
        self.mqtt_client            = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(mqtt_server)
        # save incoming messages
        self.msgs                   = mqtt_msgs  # host: msgs

    def run(self):
        # Asynchronously receive messages
        print("Start MQTTReader")
        logger.info("MQTTReader start the loop_forever ...")
        self.mqtt_client.loop_forever()   #loop_forever()  method blocks the program, handles automatic reconnects.

    def on_connect(self, client, userdata, flags, rc):
        logger.info("MQTTReader on_connect with code {}.".format(rc))
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    def on_message(self, client, userdata, msg):
        data     = json.loads(msg.payload)
        hostname = data['hdr']['hostname']
        msg_ts   = data['hdr']['msg_ts']
        if hostname in self.msgs:
            last_msg = self.msgs[hostname][-1]
            if last_msg['hdr']['msg_ts'] < msg_ts:
               self.msgs[hostname] = [last_msg, data]       # keep 2 values per time
        else:
            self.msgs[hostname] = [data]

class MQTTUpdater(multiprocessing.Process):
    #write mqttMsg every interval seconds
    def __init__(self, mqttMsg, interval, config):
        super().__init__()
        self.name     = "MQTTUpdater"
        self.data     = mqttMsg
        self.interval = interval
        self.config   = config

    def run(self):
        print("Start {}".format(self.name))
        while True:
            print("\t{} Send update".format(self.name))
            self.sendUpdate ()
            time.sleep(self.interval)

    # get pyslurm data
    def getPyslurmData (self):
        pyslurm.slurm_init()
        pyslurmData    = {'jobs':pyslurm.job().get(), 'nodes':pyslurm.node().get(), 'partition':pyslurm.partition().get(), 'qos':pyslurm.qos().get(), 'reservation':pyslurm.reservation().get()}
        return pyslurmData

    def sendUpdate (self):
        print("\t{} Send update".format(self.name))

class WebUpdater(MQTTUpdater):
    def __init__(self, mqttMsg, interval, config):
        super().__init__(mqttMsg, interval, config)
        self.name   = "WebUpdater"

    # get user's alloc cpu count on each node {'node': {'user': cpu_count}}
    def getUserAllocCPUOnNode(self, slurmJobs):
        runningJobs     = [job for job in slurmJobs.values() if job['job_state']=='RUNNING']
        node2uid2cpuCnt = DDict(lambda: DDict(int))     #node-uid-allocatedCPUCount
        for job in runningJobs:
           for node, c in job.get('cpus_allocated', {}).items():
               node2uid2cpuCnt[node][job['user_id']] += c
        return node2uid2cpuCnt

   # return user's proces on node hostname
    def getProcsByUser (self, hostname, msg_ts, msg_procs, pre_ts, pre_procs, uid2cpuCnt):

        procsByUser = []                 # [[user, uid, alloc_cores, proc_cnt, totCPURate, totRss, totVMS, procs, totIOBps, totCPUTime]...]
        uid2procs   = DDict(list)        # uid - [[pid, CPURate, create_time, user_time, system_time, rss, vms, cmdline, IOBps]...]
        for pid, proc in msg_procs.items():
            if pid in pre_procs:  # continue proc
               pre_proc = pre_procs[pid]
               c0 = pre_proc['cpu']['user_time']+pre_proc['cpu']['system_time']
               i0 = pre_proc['io']['read_bytes']+pre_proc['io']['write_bytes']
               d  = msg_ts - pre_ts
            else:                # new proc
               c0 = 0.0
               i0 = 0
               d  = msg_ts - proc['create_time']
            if d < 0.1:
               logger.warning("The time period betweeen {} and {} is too small, use 0.1 to calculate the CPU rate".format(msg_ts, pre_ts))
               d = 0.1
            CPURate = (proc['cpu']['user_time']+proc['cpu']['system_time'] - c0)/d #TODO: Replace cheap trick to avoid div0.
            if d < 1:
               logger.warning("The time period betweeen {} and {} is too small, use 1 to calculate IOBps".format(msg_ts, pre_ts))
               d = 1
            IOBps   = int((proc['io']['read_bytes']+proc['io']['write_bytes'] - i0)/d)

            #add jid 12/09/2019, add io_read, write 12/13/2019
            proc_lst = [pid, CPURate, proc['create_time'], proc['cpu']['user_time'], proc['cpu']['system_time'], proc['mem']['rss'], proc['mem']['vms'], proc['cmdline'], IOBps, proc['jid'], proc['num_fds'], proc['io']['read_bytes'], proc['io']['write_bytes'], proc['uid'], proc['num_threads']]
            uid2procs[proc['uid']].append(proc_lst)

        # get summary over processes of uid
        for uid, procs in uid2procs.items():  # proc: [pid, CPURate, create_time, user_time, system_time, rss, vms, cmdline, IOBps]
            totCPUTime = sum([proc[3]+proc[4] for proc in procs])
            totCPURate = sum([proc[1]         for proc in procs])
            totRSS     = sum([proc[5]         for proc in procs])
            totVMS     = sum([proc[6]         for proc in procs])
            totIOBps   = sum([proc[8]         for proc in procs])
            procsByUser.append ([MyTool.getUser(uid), uid, uid2cpuCnt.get(uid,0), len(procs), totCPURate, totRSS, totVMS, procs, totIOBps, totCPUTime])

        return procsByUser

    def getMsgProcs (self, msg):
        msg_ts      = msg['hdr']['msg_ts']
        msg_procs   = dict([(proc['pid'], proc) for proc in msg['processes']]) 
        return msg_ts, msg_procs
 
    def collectData(self):
        ts              = int(time.time())
        pyslurmData     = self.getPyslurmData ()
        node2uid2cpuCnt = self.getUserAllocCPUOnNode(pyslurmData['jobs'])
        nodeUserProcs   = {} # reported data {node: [state, delta, ts, [user, procs]]} 
        #update the information using msg
        for hostname in list(self.data.keys()):
            slurmNode   = pyslurmData['nodes'].get(hostname, None)
            if self.config.get('slurmOnly', True) and not slurmNode:
               logger.info("skip no-slurm node {}".format(hostname))
               continue
            slurm_state = slurmNode.get('state', '?STATE?') if slurmNode else 'NO_SLURM'
            msg_ts, msg_procs = self.getMsgProcs     (self.data[hostname][-1])
            if len(self.data[hostname]) > 1:
                pre_ts, pre_procs = self.getMsgProcs (self.data[hostname][0])
            else:
                pre_ts, pre_procs = msg_ts, {}
            procsByUser = self.getProcsByUser (hostname,msg_ts,msg_procs,pre_ts,pre_procs,node2uid2cpuCnt.get(hostname,{}))
            nodeUserProcs[hostname] = [slurm_state, msg_ts - pre_ts, msg_ts] + procsByUser
        return ts, nodeUserProcs, pyslurmData

    def sendData (self, ts, nodeUserProc, pyslurmData):
        for url in self.config.get("webURL", []):
           zps = zlib.compress(cPickle.dumps((ts, nodeUserProc, pyslurmData), -1))
           try:
               print("sendUpdate to {}".format(url))
               resp = urllib2.urlopen(urllib2.Request(url, zps, {'Content-Type': 'application/octet-stream'}))
               logger.debug("{}: sendUpdate to {} with return code {}".format(MyTool.getTsString(ts), url, resp))
               print("{}: sendUpdate to {} with return code {}".format(MyTool.getTsString(ts), url, resp))
           except Exception as e:
               body = e
               logger.error( 'Failed to update slurm data {}: {}\n{}'.format(url, e, body))

    def sendUpdate (self):
        ts, nodeUserProc, pyslurmData = self.collectData ()
        self.sendData (ts, nodeUserProc, pyslurmData)

class FileWebUpdater(WebUpdater):
    def __init__(self, mqttMsg, interval, config):
        super().__init__(mqttMsg, interval, config)
        self.name = "FileWebUpdater"
        self.hostData_dir = IndexedHostData(config["file_dir"])                # directory for backup files

    def sendData (self, ts, nodeUserProc, pyslurmData):
        super().sendData (ts, nodeUserProc, pyslurmData)
        for hostname, nodeProc in nodeUserProc.items():
            msg_ts = nodeProc[2]                # [slurm_state, delta, msg_ts] + procsByUser
            self.hostData_dir.writeData(hostname, msg_ts, nodeProc)

class InfluxUpdater(MQTTUpdater):
    def __init__(self, mqttMsg, interval, config):
        super().__init__(mqttMsg, interval, config)
        self.name = "InfluxUpdater"

class UpdateProcessManager:
    INTERVAL = 60
    def __init__(self, config):
        self.config = config

    def start(self):
        print("config={}".format(self.config))
        writeWebProc, writeFileWebProc, writeInfluxProc = None, None, None
        with multiprocessing.Manager() as manager:
            # start mqtt client
            mqttMsg        = manager.dict()
            mqttReaderProc = MQTTReader (self.config["server"], mqttMsg)
            mqttReaderProc.start()
            # start updater processes based on config file
            if self.config.get("writeWeb", False):
                if not self.config.get("writeFile", False):
                   writeWebProc     = WebUpdater     (mqttMsg, self.INTERVAL,  self.config)
                   writeWebProc.start()
                else:
                   writeFileWebProc = FileWebUpdater (mqttMsg, self.INTERVAL*2,self.config)
                   writeFileWebProc.start()
            if self.config.get("writeInflux", False):
                writeInfluxProc = InfluxUpdater (mqttMsg, self.INTERVAL*3,self.config)
                writeInfluxProc.start()

            # restart the processes if they die
            while True:
                if not mqttReaderProc.is_alive():
                   mqttReaderProc = MQTTReader (mqttMsg)
                   mqttReaderProc.start()
                if writeWebProc and not writeWebProc.is_alive():
                   writeWebProc = WebUpdater       (mqttMsg, self.INTERVAL,  self.config)
                   writeWebProc.start()
                if writeFileWebProc and not writeFileWebProc.is_alive():
                   writeFileWebProc = FileWebUpdater  (mqttMsg, self.INTERVAL*2,self.config)
                   writeFileWebProc.start()
                if writeInfluxProc and not writeInfluxProc.is_alive():
                   writeInfluxProc = InfluxUpdater (mqttMsg, self.INTERVAL*3,self.config)
                   writeInfluxProc.start()

if __name__=="__main__":
   #Usage: python mqttMonStream.py 
   parser = argparse.ArgumentParser (description='Start a deamon to listen on mqtt, retrieve pyslurm data and send data to web, file and/or influxdb.')
   parser.add_argument('-c', '--configFile',  help='The name of the config file.')
   args       = parser.parse_args()
   configFile = args.configFile
   if configFile:
      config.readConfigFile(configFile)
   cfg   = config.APP_CONFIG
   m = UpdateProcessManager(cfg["mqtt"])
   m.start()

 

