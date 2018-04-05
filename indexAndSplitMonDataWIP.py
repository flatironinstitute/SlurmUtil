import cPickle, os, sys, urllib2, zlib

z8 = '00000000'

def digestOne(smd):
    ts = smd.read(20)
    if not ts: return '', None
    if not ts.startswith(z8): raise Exception('Bogus time stamp: %s'%ts)
    pl = int(smd.read(20))
    if pl > 100000000: raise Exception('Bogus payload: %d'%pl)
    p = zlib.decompress(smd.read(pl))
    d = cPickle.loads(p)
    return int(ts), d

class Splitter(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.node2fd = {}
        self.node2pid2cpu = {}

    def close(self):
        for p, px in self.node2fd.itervalues():
            p.close()
            px.close()

    def readRecord (self):
        fx = open(self.prefix+'%s_sm.px'%n, 'r')
        fx.seek(-40, 2)
        tsOffset = fx.read(40)
        fx.close()
        ts, offset = int(tsOffset[:20]), int(tsOffset[20:])

        f = open(self.prefix+'%s_sm.p'%n, 'rb')
        f.seek(offset)
        tsZlen = f.read(40)
        f.close()
        ts, zlen = int(tsZlen[:20]), int(tsZlen[20:])
        pinfo = cPickle.loads(zlib.decompress(f.read(zlen)))

        return pinfo

    def process(self, nodeData, currTs):
        node2info = {}
        for n, info in nodeData.items():
            if n not in self.node2pid2cpu:
                # we haven't seen this node, so load in old data if present.
                pid2cpu = {}
                try: 
                    #fx = open(self.prefix+'%s_sm.px'%n, 'r')
                    #fx.seek(-40, 2)
                    #tsOffset = fx.read(40)
                    #fx.close()
                    #ts, offset = int(tsOffset[:20]), int(tsOffset[20:])
                    #f = open(self.prefix+'%s_sm.p'%n, 'rb')
                    #f.seek(offset)
                    #tsZlen = f.read(40)
                    #ts, zlen = int(tsZlen[:20]), int(tsZlen[20:])
                    #pinfo = cPickle.loads(zlib.decompress(f.read(zlen)))
                    #f.close()
                    pinfo = readRecord()
                    for user in pinfo[3:]:
                        for proc in user[7]: pid2cpu[proc[0]] = (proc[3] + proc[4], pinfo[2])
                except Exception as e:
                    print ('No history for %s, starting from scratch. (%s)'%(n, str(e)), file=sys.stderr)
                self.node2pid2cpu[n] = pid2cpu
                self.node2fd[n] = (open(self.prefix+'%s_sm.p'%n, 'ab'), open(self.prefix+'%s_sm.px'%n, 'a'))

            if len(info) < 3:
                node2info[n] = info
            else:
                state, delay, nodeNow = info[:3]
                pids = set()
                pid2cpu = self.node2pid2cpu[n]
                newuu = []
                for uname, uid, alloc, procs, pp in info[3:]:
                    newpp = []
                    loadIntTotal, rssTotal, vmsTotal = 0.0, 0.0, 0.0
                    for pid, uid, createTime, cpuUser, cpuSys, memRss, memVms, cmdLine in pp:
                        cpu = cpuUser + cpuSys
                        pids.add(pid)
                        if pid in pid2cpu:
                            lastCpu, lastTs = pid2cpu[pid]
                            delta = float(nodeNow - lastTs)
                            if delta <= 0.1:
                                print ('Time step problem (history)', nodeNow, lastTs, cpu, lastCpu, file=sys.stderr)
                                loadInt = cpu/lastTs
                            else:
                                loadInt = (cpu - lastCpu)/delta
                        else:
                            delta = float(nodeNow - createTime)
                            if delta <= 0.1:
                                print ('Time step problem (no history)', nodeNow, createTime, cpu, file=sys.stderr)
                                loadInt = cpu
                            else:
                                loadInt = cpu/delta
                        pid2cpu[pid] = (cpu, nodeNow)
                        loadIntTotal += loadInt
                        rssTotal += memRss
                        vmsTotal += memVms
                        newpp.append([pid, loadInt, createTime, cpuUser, cpuSys, memRss, memVms, cmdLine])
                    newuu.append([uname, uid, alloc, procs, loadIntTotal, rssTotal, vmsTotal, newpp])
                #retire process ids that weren't seen on this sweep.
                for pid in [p for p in pid2cpu if p not in pids]: pid2cpu.pop(pid)

                node2info[n] = info[:3] + newuu
                zps = zlib.compress(cPickle.dumps(node2info[n]))
                self.node2fd[n][1].write('%020d%020d'%(currTs, self.node2fd[n][0].tell()))
                self.node2fd[n][0].write('%020d%020d'%(currTs, len(zps)))
                self.node2fd[n][0].write(zps)
        return node2info

smdfn,smdxfn,prefix,url = sys.argv[1:]
smd = open(smdfn)
try:
    smdx = open(smdxfn, 'r')
    smdx.seek(-40, 2)
    ts, offset = int(smdx.read(20)), int(smdx.read(20))
    smdx.close()
    smd.seek(offset, 0)
    digestOne(smd)
    smdx = open(smdxfn, 'a')
except Exception as e:
    ts, offset = 0, 0
    smdx = open(smdxfn, 'w')

split = Splitter(prefix)

c, f, dumpMe, lastTime = 0, 0, None, None
while 1:
    offset = smd.tell()
    try:
        ts, r = digestOne(smd)
        if not ts: break
    except Exception as e:
        f += 1
        print ('Problem (%s): %d'%(str(e), offset), file=sys.stderr)
        smd.seek(offset, 0)
        while 1:
            tloc = smd.tell()
            b = smd.read(1000000)
            x = b.find(z8)
            if -1 == x:
                print ('Failed to find next record.', file=sys.stderr)
                break
            smd.seek(tloc+x)
            print ('Skipped', smd.tell() - loc, file=sys.stderr)
        continue
    try:
        if lastTime: assert ts > lastTime
        lastTime = ts
        jobData, nodeData, version = r
        node2info = split.process(nodeData, ts) # TODO: are we sure that this will always result in node2info being the last *valid* node data set?
    except Exception as e:
        f += 1
        print ('Incompatible data (%s) at %d.'%(str(e), offset), file=sys.stderr)
        continue

    dumpMe = (ts, jobData, node2info)
    smdx.write('%020d%020d'%(ts, offset))
    c += 1
    if 0 == c % 10000: print (c, file=sys.stderr)

if dumpMe:
    p = zlib.compress(cPickle.dumps(dumpMe, -1))
    urls = [url]
    notify = os.path.join(prefix, 'notify')
    if os.path.exists(notify): urls += [l[:-1] for l in open(notify).readlines()]
    for url in urls:
        try:
            resp = urllib2.urlopen(urllib2.Request(url, p, {'Content-Type': 'application/octet-stream'}))
            print (resp.code, resp.read(), file=sys.stderr)
        except Exception as e:
            print ('Failed to update slurm data (%s): %s'%(str(e), repr(url)), file=sys.stderr)

smd.close()
split.close()

print ('%d good, %d failures.'%(c, f), file=sys.stderr)
