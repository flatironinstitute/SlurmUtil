#!/usr/bin/env python00

import time
t1=time.time()
import os,re,subprocess
import pyslurm
import config, MyTool
from datetime import date, timedelta
from functools import reduce

logger    = config.logger
CSV_DIR   = "/mnt/home/yliu/projects/slurm/utils/data/"

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
    TS_ASSOC   = 0
    DICT_ASSOC = {}  # {'user':'account'}
    SET_ACCT   = set()
    USER_ASSOC = {"Flatiron":["./data/sacctmgr_assoc.csv",        0, {}],
                  "Popeye":  ["./data/sacctmgr_assoc_popeye.csv", 0, {}]}   #file_name, modify_time, user2record
                               #User|Def Acct|Admin|Cluster|Account|Partition|Share|Priority|MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS

    def __init__(self, cluster):
        self.cluster    = cluster;

    def refreshUserAssoc (cluster):
        fileName = SlurmCmdQuery.USER_ASSOC[cluster][0]
        if not os.path.isfile(fileName):
           return {}

        ts = os.path.getmtime(fileName)
        if ts > SlurmCmdQuery.USER_ASSOC[cluster][1]: # modified since last read
           SlurmCmdQuery.USER_ASSOC[cluster][2] = SlurmCmdQuery.readUserAssocFile (fileName)
           SlurmCmdQuery.USER_ASSOC[cluster][1] = ts

    def readUserAssocFile (file_nm):
        with open(file_nm) as fp:
             lines = fp.read().splitlines()

        fields = lines[0].split('|')           # read field name from first line
        d      = {}
        for i in range(1, len(lines)):
            values       = lines[i].split('|')       #arora|cca|None|slurm|cca||1||||||||cca,gen,ib,preempt|gen
            d[values[0]] = dict(zip(fields,values))
        return d

    def getUserAssoc (self, user):
        SlurmCmdQuery.refreshUserAssoc (self.cluster)
        u_assoc = SlurmCmdQuery.USER_ASSOC[self.cluster][2]
        if user in u_assoc:
           return u_assoc[user]
        return {}

    @staticmethod
    def getUserDoneJobReport (user, days=3, output='JobID,JobIDRaw,JobName,AllocCPUS,AllocTRES,State,ExitCode,User,NodeList,Start,End'):
        job_list = SlurmCmdQuery.sacct_getReport(['-u', user], days, output, skipJobStep=True)
        rlt      = []
        for job in job_list:
            if job['State'] in ['RUNNING','PENDING'] :   # seff not available for pending jobs and not accurate for running jobs
               continue
            #eff = SlurmCmdQuery.seff_cmd(job['JobID'])
            #eff.pop("State")
            #job.update(eff)
            rlt.append(job)
        return rlt

    @staticmethod
    def getNodeDoneJobReport (node, days=3, output='JobID,JobIDRaw,JobName,AllocCPUS,AllocTRES,State,ExitCode,User,NodeList,Start,End'):
        job_list = SlurmCmdQuery.sacct_getReport(['-N', node], days, output, skipJobStep=True)
        rlt      = []
        for job in job_list:
            if job['State'] in ['RUNNING','PENDING']:
               continue
            eff = SlurmCmdQuery.seff_cmd(job['JobID'])
            eff.pop("State")
            job.update(eff)
            rlt.append(job)
        return rlt

    @staticmethod
    def sacct_getNodeReport (nodeName, days=3, output = 'JobID,JobIDRaw,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End,AllocTRES', skipJobStep=True):
        jobs = SlurmCmdQuery.sacct_getReport(['-N', nodeName], days, output, skipJobStep)
        return jobs
 
    @staticmethod
    def sacct_getJobReport (jobid, skipJobStep=False):
        output = 'JobID,JobIDRaw,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End,AllocNodes,NodeList'
        # may include sub jobs
        jobs   = SlurmCmdQuery.sacct_getReport(['-j', str(jobid)], days=None, output=output, skipJobStep=skipJobStep)
        if not jobs:
           return None
        return jobs

    # return [jid:jinfo, ...}
    @staticmethod
    def sacct_getReport (criteria, days=3, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End', skipJobStep=True):
        #print('sacct_getReport {} {} {}'.format(criteria, days, skipJobStep))
        if days:
           t = date.today() + timedelta(days=-days)
           startDate = '%d-%02d-%02d'%(t.year, t.month, t.day)
           criteria  = ['-S', startDate] + criteria

        #Constraints has problem
        field_str, sacct_rlt = SlurmCmdQuery.sacctCmd (criteria, output)
        keys                 = field_str.split(sep='|')
        jobs                 = []
        jid_idx              = keys.index('JobID')
        for line in sacct_rlt:
            ff = line.split(sep='|')
            if (skipJobStep and '.' in ff[jid_idx]): continue # indicates a job step --- under what circumstances should these be broken out?
            #508550_0.extern, 508550_[111-626%20], (array job) 511269+0, 511269+0.extern, 511269+0.0 (?)
            if ( '.' in ff[jid_idx] ):
               ff0 = ff[jid_idx].split(sep='.')[0]
            else:
               ff0 = ff[jid_idx]

            m  = re.fullmatch(r'(\d+)([_\+])(.*)', ff0)
            if not m:
               jid = int(ff0)
            else:
               jid = int(m.group(1))
            if ff[3].startswith('CANCELLED by '):
                uid   = ff[3].rsplit(' ', 1)[1]
                uname = MyTool.getUser(uid)
                ff[3] = '%s (%s)'%(ff[3], uname)
            job = dict(zip(keys, ff))
            jobs.append(job)

        if 'AllocTRES' in output:
           for job in jobs:
               job['AllocGPUS']=MyTool.getTresGPUCount(job['AllocTRES'])

        return jobs

    @staticmethod
    def sacctCmd (criteria, output='JobID,JobName,AllocCPUS,State,ExitCode,User,NodeList,Start,End'):
        #has problem with constrains such as skylank|broadwell
        cmd = ['sacct', '-P', '-o', output] + criteria
        #print('{}'.format(cmd)) 
        try:
            #TODO: capture standard error separately?
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))
        fields, *rlt = d.decode('utf-8').splitlines()
        return fields, rlt

    @staticmethod
    def seff_cmd (jid):
        #has problem with constrains such as skylank|broadwell
        cmd = ['seff', str(jid)]
        #print('{}'.format(cmd)) 
        try:
            #TODO: capture standard error separately?
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))
        rlt = {}
        for line in d.decode('utf-8').splitlines():
            key, value = line.split(': ',1)
            rlt[key] = value
        return rlt

    @staticmethod
    def updateAssoc ():
        file_nm = CSV_DIR + "sacctmgr_assoc.csv"
        file_ts = os.path.getmtime(file_nm)
        if file_ts > SlurmCmdQuery.TS_ASSOC:      # if file is newer
           with open(file_nm) as fp: 
                lines = fp.read().splitlines()
           fields = lines[0].split('|')           # read field name from first line
                                                  # User|Def Acct|Admin|Cluster|Account|Partition|Share|Priority|MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS
           d      = {}
           for i in range(1, len(lines)):
               values = lines[i].split('|')       #arora|cca|None|slurm|cca||1||||||||cca,gen,ib,preempt|gen
               d[values[0]] = dict(zip(fields,values))
           SlurmCmdQuery.TS_ASSOC   = file_ts
           SlurmCmdQuery.DICT_ASSOC = d
           SlurmCmdQuery.SET_ACCT   = set([item['Def Acct'] for item in d.values()])

    @staticmethod
    def getAllUserAssoc (cluster="Flatiron"):
        SlurmCmdQuery.refreshUserAssoc (cluster)
        return SlurmCmdQuery.USER_ASSOC[cluster][2]

    @staticmethod
    def getAccounts ():
        SlurmCmdQuery.updateAssoc ()
        return SlurmCmdQuery.SET_ACCT

def test1():
    rlt=SlurmCmdQuery.seff_cmd (964648)
    print("rlt={}".format(rlt))

def test2():
    for cluster in ["Flatiron", "Popeye"]:
        ins = SlurmCmdQuery(cluster)
        print(ins.getUserAssoc("aarora"))

def test4():
    jobs=SlurmCmdQuery.sacct_getJobReport(927525)
    print(repr(jobs))

def test5():
    jobs=SlurmCmdQuery.sacct_getNodeReport('workergpu00')
    print(repr(jobs))

def main():
    t1=time.time()
    test2()
    print("main take time " + str(time.time()-t1))

if __name__=="__main__":
   main()
