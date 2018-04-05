#!/usr/bin/env python

import _pickle as cPickle
import urllib.request as urllib2
import json, pwd, sys, time, zlib
import paho.mqtt.client as mqtt
import pyslurm as SL

from collections import defaultdict as DD
from IndexedDataFile import IndexedHostData
Interval = 61

# Maps a host name to a tuple (time, pid2info), where time is the time
# stamp of the last update, and pid2info maps a pid to a dictionary of
# info about the process identified by the pid.

hn2pid2info = DD(lambda: (-1.0, DD(lambda: DD(lambda: DD)), []))

NodeStateHack = None

class DataReader:

    def __init__(self, prefix, urlfile):
        self.prefix  = prefix
        self.urlfile = urlfile
        self.idxHD   = IndexedHostData(prefix)

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect("mon5.flatironinstitute.org")
        self.msgs = DD(list)
        self.msg_count = 0
        self.t0 = time.time()

        # Asynchronously receive messages
        self.mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        #self.mqtt_client.subscribe("cluster/hostprocesses/worker1000")
        print ("on_connect with code %d." %(rc) )
        self.mqtt_client.subscribe("cluster/hostprocesses/#")

    def on_message(self, client, userdata, msg):
        global NodeStateHack
        
        data = json.loads(msg.payload)
        hostname = data['hdr']['hostname']
        if hostname.startswith('worker'):
            self.msgs[hostname].append(data)
        self.msg_count += 1
        t1 = time.time()
        if (t1 - self.t0) > Interval:
            print (t1, self.msg_count, file=sys.stderr)

            jobData  = SL.job().get()
            nodeData = SL.node().get()

            if NodeStateHack == None:
                sample = next(iter(nodeData.values()))
                if 'node_state' in sample:
                    NodeStateHack = 'node_state'
                elif 'state' in sample:
                    NodeStateHack = 'state'
                else:
                    print ('Cannot determine key for state in node dictionary:', dir(sample), file=sys.stderr)
                    sys.exit(-1)

            hn2uid2allocated = DD(lambda: DD(int))
            for jid, jdata in jobData.items():
                # if jdata['job_state'] != 'RUNNING': continue
                uid = jdata['user_id']
                for hn, c in jdata.get('cpus_allocated', {}).items():
                    hn2uid2allocated[hn][uid] += c

            msgs = self.msgs
            hn2info = {}

            #update the information using msg
            for hostname in nodeData: # need to generate a record for
                                      # every host to reflect current
                                      # SLURM status, even if we don't
                                      # have a msg for it.
                prevTs, pid2info, dummy = hn2pid2info[hostname]
                procsByUser = []
                uid2pp = DD(list)
                if hostname in msgs:
                    # TODO: any value in processing earlier messages if they exist?
                    m = self.msgs.pop(hostname)[-1]
                    
                    assert m['hdr']['hostname'] == hostname
                    ts= m['hdr']['msg_ts']
                    delta = 0.0 if -1.0 == prevTs else ts - prevTs
                    currentPids = set()

                    for process in m['processes']:
                        pid = process['pid']
                        currentPids.add(pid)
                        if pid in pid2info:
                            c0 = pid2info[pid]['cpu']['user_time']
                            t0 = prevTs
                        else:
                            c0 = 0.0
                            t0 = process['create_time']
                        intervalLoadAverage = (float(process['cpu']['user_time']) - c0)/max(.1, ts - t0) #TODO: Replace cheap trick to avoid div0.
                        uid2pp[process['uid']].append([pid, intervalLoadAverage, process['create_time'], process['cpu']['user_time'], process['cpu']['system_time'], process['mem']['rss'], process['mem']['vms'], process['cmdline']])
                        pid2info[pid] = process

                    retirePids = [pid for pid in pid2info if pid not in currentPids]
                    for pid in retirePids: pid2info.pop(pid)

                    for uid, pp in uid2pp.items():
                        totILA, totRSS, totVMS = 0.0, 0, 0
                        for p in pp:
                            totILA += p[1]
                            totRSS += p[5]
                            totVMS += p[6]
                        procsByUser.append([pwd.getpwuid(uid).pw_name, uid, hn2uid2allocated.get(hostname, {}).get(uid, -1), len(pp), totILA, totRSS, totVMS, pp])

                    hn2info[hostname] = [nodeData[hostname].get(NodeStateHack, '?STATE?'), delta, ts] + procsByUser
                    hn2pid2info[hostname] = (ts, pid2info, procsByUser)
                else:
                    ts, pid2info, procsByUser = hn2pid2info[hostname]
                    delta = 0.0 if -1 == ts else t1 - ts
                    hn2info[hostname] = [nodeData[hostname].get(NodeStateHack, '?STATE?'), delta, ts] + procsByUser
                        
                #save information to files
                self.idxHD.writeData(hostname, t1, hn2info[hostname])
                
            self.discardMessage()
                
            for url in open(self.urlfile):
                url = url[:-1]
                zps = zlib.compress(cPickle.dumps((t1, jobData, hn2info, nodeData), -1))
                #print ("url=", url, ",", t1, ",", len(jobData))
                try:
                    resp = urllib2.urlopen(urllib2.Request(url, zps, {'Content-Type': 'application/octet-stream'}))
                    print ( resp.code, resp.read(), file=sys.stderr)
                except Exception as e:
                    print ( 'Failed to update slurm data (%s): %s'%(str(e), repr(url)), file=sys.stderr)

            self.t0 += Interval

    def discardMessage(self):
        hdiscard, mmdiscard = 0, 0
        #self.msgs is a dict
        while (len(self.msgs) > 0):
            h, value = self.msgs.popitem()
            hdiscard  += 1
            mmdiscard += len(value)

        if hdiscard: print ('Discarding %d messages from %d hosts (e.g., %s)'%(mmdiscard, hdiscard, h), file=sys.stderr)

def main():
    app = DataReader(sys.argv[1], sys.argv[2]);


main()
