import _pickle as cPickle
import urllib.request as urllib2
import json, logging, pwd, sys, threading, time, zlib
import paho.mqtt.client as mqtt
import pyslurm
import pdb
import MyTool

from collections import defaultdict as DDict
from IndexedDataFile import IndexedHostData
from mqtMon2Influx import Node2PidsCache

logger   = MyTool.getFileLogger('mqttMonStream', logging.DEBUG)  # use module name
test_mode= False

# Maps a host name to a tuple (time, prePid2info), where time is the time
# stamp of the last update, and prePid2info maps a pid to a dictionary of
# info about the process identified by the pid.

class MQTTReader(threading.Thread):
    def __init__(self, mqtt_server='mon5.flatironinstitute.org'):
        threading.Thread.__init__(self)
        # connect mqtt
        self.mqtt_client            = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(mqtt_server)
        # save incoming messages
        self.msgs      = DDict(list)  # host: msgs
        self.lock      = threading.Lock()   #guard the message list hostperf_msgs, ...

    def run(self):
        # Asynchronously receive messages
        logger.info("MQTTReader start the loop_forever ...")
        self.mqtt_client.loop_forever()   #loop_forever()  method blocks the program, handles automatic reconnects.

    def on_connect(self, client, userdata, flags, rc):
        logger.info("MQTTReader on_connect with code {}.".format(rc))
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    #running in the MainThread
    def on_message(self, client, userdata, msg):
        data     = json.loads(msg.payload)
        hostname = data['hdr']['hostname']
        #logger.debug("on_message {}:{}:{}.".format(threading.get_ident(), MyTool.getTsString(data['hdr']['msg_ts']), hostname))
        if hostname.startswith('worker'):
           with self.lock:
               self.msgs[hostname].append(data)

    def retrieveMsgs(self):
        with self.lock:
            logger.debug("retrieveMsgs of {}".format(self.msgs.keys()))
            sav       = self.msgs
            self.msgs = DDict(list)
        return sav
        
class FileWebUpdater(threading.Thread):
    INTERVAL   = 60

    def __init__(self, source, tgt_dir, tgt_urlfile):
        threading.Thread.__init__(self)
        self.source      = source
        self.hostData_dir= IndexedHostData(tgt_dir)           # write to files in the data_dir
        self.urls        = [url[:-1] for url in open(tgt_urlfile)] # send update to urls
        self.time        = time.time()
        self.savNode2TsProcs = DDict(lambda: (-1.0, DDict(lambda: DDict(lambda: DDict))))   #host - ts - pid - proc

        logger.info("Start FileWebUpdater with tgt_dir={}, urls={}, test_mode={}".format(tgt_dir, self.urls, test_mode))

    def run(self):
        #pdb.set_trace()
        while True:
            curr_ts    = time.time()
            slurmJobs  = pyslurm.job().get()
            slurmNodes = pyslurm.node().get()
            msgs       = self.source.retrieveMsgs()
            if msgs:
               self.dealData (curr_ts, slurmJobs, slurmNodes, msgs)

            time.sleep(self.INTERVAL)

    def getUserAllocCPUOnNode(self, slurmJobs):
        runningJobs     = [job for job in slurmJobs.values() if job['job_state']=='RUNNING']
        node2uid2cpuCnt = DDict(lambda: DDict(int))     #node-uid-allocatedCPUCount
        for job in runningJobs:
           for node, c in job.get('cpus_allocated', {}).items():
               node2uid2cpuCnt[node][job['user_id']] += c
        return node2uid2cpuCnt

    # return the proces on hostname
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
            uid2procs[proc['uid']].append([pid, CPURate, proc['create_time'], proc['cpu']['user_time'], proc['cpu']['system_time'], proc['mem']['rss'], proc['mem']['vms'], proc['cmdline'], IOBps])

        # get summary over processes of uid
        for uid, procs in uid2procs.items():  # proc: [pid, CPURate, create_time, user_time, system_time, rss, vms, cmdline, IOBps]
            totCPUTime = sum([proc[3]+proc[4] for proc in procs])
            totCPURate = sum([proc[1]         for proc in procs])
            totRSS     = sum([proc[5]         for proc in procs])
            totVMS     = sum([proc[6]         for proc in procs])
            totIOBps   = sum([proc[8]         for proc in procs])
            procsByUser.append ([MyTool.getUser(uid), uid, uid2cpuCnt.get(uid, -1), len(procs), totCPURate, totRSS, totVMS, procs, totIOBps, totCPUTime])

        return procsByUser

    def dealData(self, ts, slurmJobs, slurmNodes, msgs):
        # faciliated data structure
        node2uid2cpuCnt = self.getUserAllocCPUOnNode(slurmJobs)

        nodeUserProcs = {} #node:state, delta, ts, [user, procs] 
        #update the information using msg
        for hostname, slurmNode in slurmNodes.items(): # need to generate a record for every host to reflect current
                                  # SLURM status, even if we don't have a msg for it.
            pre_ts, pre_procs = self.savNode2TsProcs[hostname]
            if hostname in msgs:
               host_msgs = msgs.pop(hostname)
               msg       = host_msgs[-1]               # get the latest message
               msg_ts    = msg['hdr']['msg_ts']
               msg_procs = dict([(proc['pid'], proc) for proc in msg['processes']]) 
               if len(host_msgs) > 1:
                  pre_msg = host_msgs[-2]
                  if pre_ts < pre_msg['hdr']['msg_ts']:  #saved value is older
                     pre_ts   = pre_msg['hdr']['msg_ts']
                     pre_procs= dict([(proc['pid'], proc) for proc in pre_msg['processes']]) 
               if pre_ts >= msg_ts:
                  self.logger.error ("Ignore {}'s new data at {} because it is older than old one at {}. ".format(hostname, MyTool.getTsString(msg_ts), MyTool.getTsString(pre_ts))) 
                  delta             = 0.0 if -1.0 == pre_ts else ts - pre_ts
                  nodeUserProcs[hostname]   = [slurmNode.get('state', '?STATE?'), delta, ts]
               else:  #pre_ts < msg_ts
                  delta      = 0.0 if -1.0 == pre_ts else msg_ts - pre_ts
                  procsByUser= self.getProcsByUser (hostname, msg_ts, msg_procs, pre_ts, pre_procs, node2uid2cpuCnt.get(hostname, {}))
                  nodeUserProcs[hostname]   = [slurmNode.get('state', '?STATE?'), delta, ts] + procsByUser
                  # upate savNode2TsProcs
                  self.savNode2TsProcs[hostname] = (msg_ts, msg_procs)
            else:
               logger.debug ("dealData has no data on {}".format(hostname))
               delta             = 0.0 if -1.0 == pre_ts else ts - pre_ts
               nodeUserProcs[hostname]   = [slurmNode.get('state', '?STATE?'), delta, ts]

            #save information to files
            if not test_mode:
               self.hostData_dir.writeData(hostname, ts, nodeUserProcs[hostname])
            logger.debug("writeData {}:{}".format(ts, hostname)) 

        self.discardMessage(msgs)
        self.sendUpdate    (ts, slurmJobs, nodeUserProcs, slurmNodes)

    def sendUpdate (self, ts, slurmJobs, hn2data, slurmNodes):
        for url in self.urls:
           zps = zlib.compress(cPickle.dumps((ts, slurmJobs, hn2data, slurmNodes), -1))
           #print ("url=", url, ",", ts, ",", len(jobData))
           try:
               logger.debug("sendUpdate to {}".format(url))
               if not test_mode:
                  resp = urllib2.urlopen(urllib2.Request(url, zps, {'Content-Type': 'application/octet-stream'}))
               else:
                  resp = 0
               logger.debug("{}:{}: sendUpdate to {} with return code {}".format(threading.currentThread().ident, MyTool.getTsString(ts), url, resp))
               #print ( resp.code, resp.read(), file=sys.stderr)
           except Exception as e:
               logger.error( 'Failed to update slurm data (%s): %s'%(str(e), repr(url)))

    def discardMessage(self, msgs):
        hdiscard, mmdiscard = 0, 0
        #self.msgs is a dict
        while (len(msgs) > 0):
            h, value = msgs.popitem()
            hdiscard  += 1
            mmdiscard += len(value)
        if hdiscard: logger.debug('Discarding %d messages from %d hosts (e.g., %s)'%(mmdiscard, hdiscard, h))

def main():
    source = MQTTReader ()
    source.start()
    time.sleep(5)
    app = FileWebUpdater(source, sys.argv[1], sys.argv[2])
    app.start()

if __name__=="__main__":
    main()
 

