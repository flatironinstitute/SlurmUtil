
import glob, pwd, sys

from random import randint
from math import log

FileSystems = {
    # Label displayed in web page, path to directory with summary data, suffix for appropriate summary data file
    'ceph_users': ['Ceph Users', '/mnt/xfs1/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 3, 4],
    'ceph_full': ['Ceph Full', '/mnt/xfs1/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 1, 2],
    'home': ['Home', '/mnt/xfs1/home/carriero/projects/fileCensus/data', '_full.sum', 0, 3, 4],
    'xfs1': ['xfs1', '/mnt/xfs1/home/carriero/projects/fileCensus/data', '_full.sum', 0, 1, 2],
}    

def anonimize(s):
    ns = ''
    for c in s:
        if c in 'aeiou':
            ns += 'aeio'[randint(0, 3)]
        else:
            ns += 'bcdfghlmnprstvy'[randint(0, 14)]
    return ns

def gendata(yyyymmdd, fs, anon=False):
    if fs not in FileSystems: return 'Unknown file system: "%s"'%fs, ''
    label, dataDir, suffix, uidx, fcx, bcx = FileSystems[fs]
    ff = sorted(glob.glob(dataDir+'/2*'+suffix))
    me = 0
    for x, f in enumerate(ff):
        if yyyymmdd in f:
            me = x
            break
    else: return 'Date not found', ''

    u2s = {}
    tdfc, tdfb = 0, 0
    for x, f in enumerate(ff[me-1:me+1]):
        for l in open(f):
            ff = l[:-1].split('\t')
            uid, fc, bytes = [int(ff[x]) for x in [uidx, fcx, bcx]]
            old = u2s.get(uid, [0, 0, fc, bytes])
            dfc, dfb = fc - old[2], bytes - old[3]
            tdfc += dfc
            tdfb += dfb
            u2s[uid] = [dfc, dfb, fc, bytes]

    # find N50 wrt file count.
    s = 0
    uid2x = {}
    cutoff = None
    for x, (dfc, uid) in enumerate(sorted([(dfc, uid) for uid, [dfc, d, d, d] in u2s.items()], reverse=True)):
        s += dfc 
        if 2*s > tdfc: cutoff = x
        uid2x[uid] = x

    if 4*x > len(u2s): cutoff = 2

    r = ''
    for uid, v in u2s.items():
        if uid < 1000: continue
        try:
            uname = pwd.getpwuid(uid).pw_name
        except:
            uname = 'User_%d'%uid

        if anon: uname = anonimize(uname)

        marker = ''
        if cutoff and uid2x[uid] <= cutoff: marker = ', marker: { fillColor: "rgba(236,124,181,0.5)" } '
        if 0 == v[2] or 0 == v[3]:
            # non-home user, skip
            continue
        r +=  '\t{ x: %d, y: %d, z: %d, dfb: %d, dfc: %d, name: "%s"%s},\n' %(v[3], v[2], log(max(2**20, v[1]), 2)-19, v[1], v[0], uname, marker)

    return (label, r)

if '__main__' == __name__:
    print (gendata(*sys.argv[1:]))

