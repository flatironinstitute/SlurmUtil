#!/usr/bin/env python00

import time
t1=time.time()
from datetime import datetime, timezone, timedelta
import subprocess
import pyslurm
import MyTool

class SlurmStatus:
    STATUS_LIST=['undefined', 'running', 'sleeping', 'disk-sleep', 'zombie', 'stopped', 'tracing-stop']

    @classmethod
    def getStatusID (cls, statusStr):
        if statusStr not in cls.STATUS_LIST:
           print("SlurmStatus ERROR: status not existed! " + statusStr)
           cls.STATUS_LIST.append(statusStr)
           return len(cls.STATUS_LIST)-1
        else:
           return cls.STATUS_LIST.index(statusStr)
        
    @classmethod
    def getStatus (cls, idInt):
        return cls.STATUS_LIST[idInt]

class SlurmCmdQuery:
    LOCAL_TZ = timezone(timedelta(hours=-4))

    def __init__(self):
        pass

    # given a jid, return [jid, uid, start_time, end_time]
    # if end_time is unknown, return current time
    # if start_time is unknown, error
    def getSlurmJobInfo (self, jid): 
        cmd = ['sacct', '-n', '-P', '-X', '-j', str(jid), '-o', 'JobID,User,NodeList,Start,End']
        try:
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" ERROR: returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))

        d=d.decode('utf-8')
        print (d)
        info = d.rstrip().split('|')
        print (repr(info))
        if info[4]!='Unknown':
           return [info[0], MyTool.getUid(info[1]), MyTool.convert2list(info[2]), MyTool.getSlurmTimeStamp(info[3]), MyTool.getSlurmTimeStamp(info[4])]
        else:
           return [info[0], MyTool.getUid(info[1]), MyTool.convert2list(info[2]), MyTool.getSlurmTimeStamp(info[3]), time.time()]
        
    def getSlurmJobNodes (self, jid):
        pass
        
    def getDictIntValue (self, point, name):
        pass
           
    def getSlurmNodeMon (self, hostname, uid, start_time, end_time):
        pass

    #return [[...],[...],[...]], each of which
    #[{'node':nodename, 'data':[[timestamp, value]...]} ...]
    def getSlurmJobMonData(self, jid, uid, nodelist, start_time, stop_time):
        pass

    #return [[...],[...],[...]], each of which
    #[{'node':nodename, 'data':[[timestamp, value]...]} ...]
    def getSlurmUidMonData(self, uid, nodelist, start_time, stop_time):
        pass

def main():
    #job= pyslurm.job().find_id ('88318')
    #job= pyslurm.slurmdb_jobs().get ()
    #print(repr(job))
    t1=time.time()
    #print(SlurmStatus.getStatusID('running'))
    #print(SlurmStatus.getStatusID('running1'))
    #print(SlurmStatus.getStatus(1))
    client = SlurmCmdQuery()
    #info = client.getSlurmJobInfo('105179')
    info = client.getSlurmJobInfo('110972')
    print(repr(info))
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
