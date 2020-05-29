
import glob, pwd, sys
import os, re
import pandas
import MyTool

from datetime import datetime, date
from random import randint
from math import log

FileSystems = {
    # Label displayed in web page, path to directory with summary data, suffix for appropriate summary data file
    'ceph_users': ['Ceph Users', '/mnt/xfs1/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 3, 4, '(\d{8})_full.sum'],
    'ceph_full':  ['Ceph Full',  '/mnt/xfs1/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 1, 2, '(\d{8})_full.sum'],
    'home':       ['Home',       '/mnt/xfs1/home/carriero/projects/fileCensus/data',     '_full.sum', 0, 3, 4, '(\d{8})_.*_full.sum'],
    'xfs1':       ['xfs1',       '/mnt/xfs1/home/carriero/projects/fileCensus/data',     '_full.sum', 0, 1, 2, '(\d{8})_.*_full.sum'],
}    

def anonimize(s):
    ns = ''
    for c in s:
        if c in 'aeiou':
            ns += 'aeio'[randint(0, 3)]
        else:
            ns += 'bcdfghlmnprstvy'[randint(0, 14)]
    return ns

#return {date: filenames, ...} sorted by date
def getFilenames(dataDir, rge):
    rlt = {}
    for fname in os.listdir(dataDir):
        match = re.fullmatch(rge, fname)
        if match: 
           d = int(datetime.strptime(match.group(1), '%Y%m%d').timestamp())
           rlt[d] = os.path.join(dataDir, fname)

    return rlt

def getFilehead (uidx, fcx, bcx):
    fhead      =['col0', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6']
    fhead[uidx]='uid'
    fhead[fcx] ='fc'
    fhead[bcx] ='bc'
    return fhead

def getDateFromFilename (dataDir, rge, fname):
    x = dataDir + '/' + rge
    mo = re.match(x, fname)
    if mo:
       return mo[1]
    else:
       return None

#get data of a specific user
#[{name:fs,data[[day,fc],],},]
def gendata_user(user, start='', stop=''):
    fc_rlt, bc_rlt = [],[]  # {filesystem: data, ...}
    uid = MyTool.getUid(user)
    for fs in FileSystems:
        fs_dict          = gendata_fs_history(fs, start, stop)
        fc_list, bc_list = [],[]
        for day, df in fs_dict.items():
            u_df         = df.loc[df['uid']==uid]  #filter on uid
            if not u_df.empty:
                values = u_df.values.tolist()[0]   #one record for each day
                fc_list.append([day, values[1]])
                bc_list.append([day, values[2]])
        fc_rlt.append({"name":fs, "data":fc_list}) 
        bc_rlt.append({"name":fs, "data":bc_list}) 
        
    return fc_rlt, bc_rlt

#assume fs is valid
def gendata_fs_history(fs, start='', stop=''):
    label, dataDir, suffix, uidx, fcx, bcx, rge = FileSystems[fs] #uidx is uid index in file
    fsDict                                      = getFilenames (dataDir, rge)
    # read files one by one to save the data in u2s
    dDict   = {}
    for d, fname in fsDict.items():
        flag = True
        if start: flag &= ( d>=start)
        if stop:  flag &= ( d<= stop)
        if flag:
           #0: uid, 3: fc(filecount), 4: bc(byte_count)
           df            = pandas.read_csv(fname, sep='\t', header=None, usecols=[uidx,fcx,bcx])
           df.columns    = ['uid', 'fc', 'bc']
           dDict[d*1000]      = df  #?really need to *1000
    return dDict

def gendata_all(fs, start='', stop='', topN=5):
    if fs not in FileSystems: 
       print("WARNING gendata_all: Unknown file system: {}".format(fs))
       return [], []
    dDict   = gendata_fs_history (fs, start, stop)
    if not dDict:
       return [], []

    df      = pandas.concat  (dDict, names=['ts','idx'])
    dfg     = df.groupby ('uid')
    # loop over dfg to generate uid2seq
    sumDf   = dfg.sum()
    sumDf1  = sumDf.sort_values (['fc', 'bc'], ascending=False)
    sumDf2  = sumDf.sort_values (['bc', 'fc'], ascending=False)

    # for each uid, dfg.get_group
    uid2seq1 = []  #{ uid: [(ts, value), ...], ...}
    for uid in sumDf1.head(n=topN).index.values:
        uidDf         = dfg.get_group(uid).reset_index()
        uname         = MyTool.getUser(uid)
        uid2seq1.append ({'name': uname, 'data': uidDf.loc[:,['ts','fc']].values.tolist()})
        
    uid2seq2 = []  #{ uid: [(ts, value), ...], ...}
    for uid in sumDf2.head(n=topN).index.values:
        uidDf         = dfg.get_group(uid).reset_index()
        uname         = MyTool.getUser(uid)
        uid2seq2.append ({'name': uname, 'data': uidDf.loc[:,['ts','bc']].values.tolist()})
        
    return uid2seq1, uid2seq2

def gendata(yyyymmdd, anon=False):
    rlt = {}
    for fs in FileSystems.keys():
        rlt[fs] = gendata_fs(yyyymmdd, fs, anon)
    return rlt

def gendata_fs(yyyymmdd, fs, anon=False):
    if fs not in FileSystems: return 'Unknown file system: "%s"'%fs, ''
    label, dataDir, suffix, uidx, fcx, bcx, rge = FileSystems[fs]
    ff = sorted(glob.glob(dataDir+'/2*'+suffix))
    me = 0
    for x, f in enumerate(ff):
        if yyyymmdd in f:
            me = x
            break
    else: 
        me = len(ff)-1
        print('gendata WARNING: Date {}:{} not found. Use most recent {} instead.'.format(fs, yyyymmdd, ff[me]))
        yyyymmdd = getDateFromFilename(dataDir, rge, ff[me])

    u2s = {}
    tdfc, tdfb = 0, 0  # total dfc(delta file count), dfb(delta file bytes)
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

    r=[]
    for uid, v in u2s.items():
        if uid < 1000: continue
        try:
            uname = pwd.getpwuid(uid).pw_name
        except:
            uname = 'User_%d'%uid

        if anon: uname = anonimize(uname)

        if 0 == v[2] or 0 == v[3]:
            # non-home user, skip
            continue
        #r +=  '\t{ x: %d, y: %d, z: %d, dfb: %d, dfc: %d, name: "%s"%s},\n' %(v[3], v[2], log(max(2**20, v[1]), 2)-19, v[1], v[0], uname, marker)
        if cutoff and uid2x[uid] <= cutoff:
           r.append({'x':v[3], 'y':v[2], 'z':log(max(2**20, v[1]),2)-19, 'dfb':v[1], 'dfc':v[0], 'name':'{}'.format(uname), 'marker':{'fillColor': 'rgba(236,124,181,0.5)'}})
        else:
           r.append({'x':v[3], 'y':v[2], 'z':log(max(2**20, v[1]),2)-19, 'dfb':v[1], 'dfc':v[0], 'name':'{}'.format(uname)})

    return [label, r, yyyymmdd]

def test1(user):
    print("Test user {}'s history".format(user))
    print (gendata_user(user))
   
if '__main__' == __name__:
    #print (gendata(*sys.argv[1:]))
    #print (gendata_all(*sys.argv[1:], 2))
    #print (gendata_user(*sys.argv[1:]))
    test1 (sys.argv[1])

