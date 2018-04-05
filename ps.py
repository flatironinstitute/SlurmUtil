#!/mnt/xfs1/bioinfoCentos7/software/installs/python/2.7.10/bin/python

import _pickle as cPickle, os, psutil, time

now = time.time()

ex = [os.getpid(), os.getppid()]

pp = []
for p in psutil.process_iter():
    try:
        uid = p.uids().effective
        if uid >= 1000 and p.pid not in ex:
            ct = p.cpu_times()
            mi = p.memory_info()
            pp.append((p.pid, uid, p.create_time(), ct.user, ct.system, mi.rss, mi.vms, p.cmdline()))
    except psutil.NoSuchProcess:
        pass

print(cPickle.dumps((now, pp)))
