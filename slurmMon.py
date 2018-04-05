#!/mnt/xfs1/bioinfoCentos7/software/installs/python/2.7.10/bin/python

import _pickle as cPickle, os, pwd, pyslurm as SL, psutil as PS, subprocess as SUB, sys, zlib
from collections import defaultdict as DD
from multiprocessing import Pool, Queue 

DataVersion = '170615'

wai = os.path.dirname(os.path.realpath(sys.argv[0]))

def runCmd(cmd):
    try:
        output = SUB.check_output(cmd, shell=True)
        returncode = 0
    except SUB.CalledProcessError as e:
        output = e.output
        returncode = e.returncode
    return (returncode, output)

# Different releases of Slurm uses different labels for the node
# state, this variable allows us to switch between.
NodeStateHack = None

def getInfo(mp):
    global NodeStateHack

    jobData = SL.job().get()
    nodeData = SL.node().get()
    nodeNames = sorted(nodeData.keys())
    if NodeStateHack == None:
        sample = nodeData[nodeNames[0]]
        if 'node_state' in sample:
            NodeStateHack = 'node_state'
        elif 'state' in sample:
            NodeStateHack = 'state'
        else:
            print ('Cannot determine key for state in node dictionary:', dir(sample), file=sys.stderr)
            sys.exit(-1)

    target, skip = [], []
    for nn in nodeNames:
        if nodeData[nn][NodeStateHack][-1] == '*': skip.append(nn) # trust SLURM on this state, potentially saving us a few timeouts.
        else: target.append(nn)
            
    # these commands may be queued up by the processing pool. by
    # incorporating "date" in the command sequence, we get a time on
    # the host close to when each "ssh" is actually executed.
    # currently, this is used to look for clock skew.
    psData = dict(zip(target, mp.map(runCmd, ['date +%%s.%%N ; ssh -n -o PasswordAuthentication=no -o ConnectTimeout=3 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s %s/ps.py < /dev/null'%(nn, wai) for nn in target])))
    for nn in skip: psData[nn] = (-1, '')

    uid2allocated = DD(dict)
    nn2uids = DD(dict)
    for jid, jdata in jobData.items():
        if jdata['job_state'] != 'RUNNING': continue
        uid = jdata['user_id']
        dua = uid2allocated[uid]
        for nn, c in jdata['cpus_allocated'].items():
            dua[nn] = dua.get(nn, 0) + c
            nn2uids[nn][uid] = True

    nn2info = {}
    for nn, (rc, output) in sorted(psData.items()):
        t = [nodeData[nn][NodeStateHack]]
        if not rc: # i.e., the remote command appears to have worked.
            hnow, pps = output.split('\n', 1)
            hnow = float(hnow)
            lnow, procs = cPickle.loads(pps)
            t.extend([lnow - hnow, lnow])

            uid2pp = DD(list)
            for p in procs:
                if p[1] >= 1000: uid2pp[p[1]].append(p)

            for uid, pp in uid2pp.items():
                t.append((pwd.getpwuid(uid).pw_name, uid, uid2allocated[uid].get(nn, -1), len(pp), pp))
            for uid in [uid for uid in nn2uids[nn] if uid not in uid2pp]: # users who have an allocation, but no processes.
                t.append((pwd.getpwuid(uid).pw_name, uid, uid2allocated[uid].get(nn, -1), 0, []))
                
        nn2info[nn] = t

    return (jobData, nn2info, DataVersion)

if '__main__' == __name__:
    mp = Pool()

    picklePath = sys.argv[-1]
    if len(sys.argv) > 1 and sys.argv[1] == 'demon':
        import time

        sweepInterval = 97 # TODO: make parameter

        while 1:
            t0 = time.time()
            info = getInfo(mp)
            t1 = time.time()
            print (time.strftime("%Y/%m/%d, %H:%M:%S %Z", time.localtime(t1)), 'Scan took %.2f s.'%(t1 - t0), file=sys.stderr)
            p = zlib.compress(cPickle.dumps(info, -1))
            lp = len(p)
            open(picklePath, 'a').write('%020d%020d'%(t0, lp) + p)
            time.sleep(max(1, (sweepInterval - (t1 - t0))))

    else:
        (jobData, nn2info) = getInfo(mp)
        for nn, info in sorted(nn2info.items()):
            print(nn, info)
        for jid, jinfo in sorted(jobData.items()):
            for k, v in sorted(jinfo.items()):
                print('%20d'%jid, '%20s'%k, v)
