import csv, glob, os, re, sys, time
import pandas
import config, MyTool

from datetime import datetime, date
from random   import randint
from math     import log

logger = config.logger

FileSystems = {
    # name: Label, path to directory, suffix for summary data file, uid_idx, filecount_idx, bytecount_idx, filename_regular_exp
    'ceph_users': ['Ceph Users', '/mnt/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 3, 4, '(\d{8})_full.sum'],
    'ceph_full':  ['Ceph Full',  '/mnt/home/carriero/projects/fileCensus/cephdata', '_full.sum', 0, 1, 2, '(\d{8})_full.sum'],
    'home':       ['Home',       '/mnt/home/carriero/projects/fileCensus/data',     '_full.sum', 0, 3, 4, '(\d{8})_.*_full.sum'],
    'xfs1':       ['xfs1',       '/mnt/home/carriero/projects/fileCensus/data',     '_full.sum', 0, 1, 2, '(\d{8})_.*_full.sum'],
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
def getFileDate(dataDir, rge):
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

def getDateFromFileName (rge, fname):
    m = re.match(rge, fname)
    if m:
       return m[1]
    else:
       return None


#get data of a specific user
#[{name:fs,data[[day,fc],],},]
def gendata_user(uid, start='', stop=''):
    fc_rlt, bc_rlt = [],[]  # {filesystem: data, ...}
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

def gendata_user_latest(uid, file_systems=None):
    rlt = {}
    if not file_systems:
       file_systems = FileSystems
    for fs in file_systems:
        label, dataDir, suffix, uidx, fcx, bcx, rge = FileSystems[fs]
        fsDict  = getFileDate (dataDir, rge)
        d       = list(sorted(fsDict))[-1]
        df      = pandas.read_csv(fsDict[d], sep='\t', header=None, usecols=[uidx,fcx,bcx])        
        df      = df.sort_values(by=[2],ascending=False,ignore_index=True)
        u_df    = df.loc[df[0]==uid]
        if not u_df.empty:
           values = u_df.values.tolist()[0]
           idx    = u_df.index.tolist()[0]
           rlt[fs] = [values[1], values[2], idx+1]  # third one is the n-th top user
        else:
           continue
    return rlt

#assume fs is valid
def gendata_fs_history(fs, start='', stop=''):
    label, dataDir, suffix, uidx, fcx, bcx, rge = FileSystems[fs] #uidx is uid index in file
    fsDict  = getFileDate (dataDir, rge)
    dDict   = {}
    for d in sorted(fsDict):
        if start and d<start:
           continue
        if stop and d>stop:
           break
        fname = fsDict[d]
        #flag = True
        #if start: flag &= ( d>=start)
        #if stop:  flag &= ( d<= stop)
        #if flag:
        df            = pandas.read_csv(fname, sep='\t', header=None, usecols=[uidx,fcx,bcx])
        df.columns    = ['uid', 'fc', 'bc']
        dDict[d*1000]      = df  #?really need to *1000
    return dDict

def gendata_all(fs, start='', stop='', topN=5):
    if fs not in FileSystems: 
       logger.warning("WARNING gendata_all: Unknown file system: {}".format(fs))
       return [], []
    dDict    = gendata_fs_history (fs, start, stop)
    if not dDict:
       return [], []
    # get last ts, decide top users based on latest usage
    last_ts  = list(sorted(dDict))[-1]
    top_df   = dDict[last_ts].nlargest(5,["fc","bc"],keep='first')
    top_uid1 = top_df['uid'].tolist()
    top_df   = dDict[last_ts].nlargest(5,["bc","fc"],keep='first')
    top_uid2 = top_df['uid'].tolist()

    df      = pandas.concat  (dDict, names=['ts','idx'])
    dfg     = df.groupby ('uid')
    # for each uid, dfg.get_group
    uid2seq1 = []  #{ uid: [(ts, value), ...], ...}
    for uid in top_uid1:
        uidDf         = dfg.get_group(uid).reset_index()
        uname         = MyTool.getUser(uid, fakeName=True)
        uid2seq1.append ({'name': uname, 'data': uidDf.loc[:,['ts','fc']].values.tolist()})
        
    uid2seq2 = []  #{ uid: [(ts, value), ...], ...}
    for uid in top_uid2:
        uidDf         = dfg.get_group(uid).reset_index()
        uname         = MyTool.getUser(uid, fakeName=True)
        uid2seq2.append ({'name': uname, 'data': uidDf.loc[:,['ts','bc']].values.tolist()})
        
    return uid2seq1, uid2seq2

#gendata with date, whether anonomys and delta_day
#return {fs_name: data}
def gendata(yyyymmdd, anon=False, delta_day=1):
    users = MyTool.getAnsibleUsers(config.CSV_DIR)
    rlt = {}
    for fs in FileSystems.keys():
        rlt[fs] = gendata_fs(yyyymmdd, fs, users, anon=anon, delta_day=delta_day)
    return rlt

def extract (line, key_idx, idx_lst):
    lst    = line.split('\t')
    values = [int(lst[idx]) for idx in idx_lst]
    return (int(lst[key_idx]), values)
 
def read_file (filename, key_idx, idx_lst):
    with open(filename) as f:
         d = dict([extract(line.rstrip(), key_idx, idx_lst) for line in f])
    return d

def minus_list (lst1, lst2):
    if not lst1:
       return [-v for v in lst2]
    if not lst2:
       return lst1
    return [lst1[i]-lst2[i] for i in range(len(lst1))]

#return data for a specific filesystem fs [label, data_list, yyyymmdd]
def gendata_fs(yyyymmdd, fs, ansible_users={}, anon=False, delta_day=1):
    if fs not in FileSystems: 
       return ['Unknown file system: {}'.format(fs), [], yyyymmdd]

    label, dataDir, suffix, uidx, fcx, bcx, rge = FileSystems[fs]
    ff  = sorted(glob.glob(dataDir+'/2*'+suffix))
    if len(ff) == 0:
        return ['No file under {}/2*{}'.format(dataDir, suffix), [], yyyymmdd]

    idx = 0
    for x, f in enumerate(ff):
        if yyyymmdd in os.path.basename(f):  #filename without dir
            idx = x
            break
    else: # after for loop without break 
        idx      = len(ff)-1
        logger.warning('Date {}:{} not found. Use most recent {} instead.'.format(fs, yyyymmdd, ff[-1]))
        yyyymmdd = getDateFromFileName(rge, os.path.basename(ff[-1]))

    #calculate delta and cut_off
    pre   = read_file (ff[idx-delta_day], uidx, [fcx,bcx])
    curr  = read_file (ff[idx],   uidx, [fcx,bcx])
    delta = { k:minus_list(curr.get(k,None),pre.get(k, None))+curr.get(k,[0,0]) for k in (set(pre) | set(curr))}  #uid: [delta_fc, delta_bc, curr_fc, curr_bc]
    t_dfc = sum([v[0] for v in delta.values()])
    t_dbc = sum([v[1] for v in delta.values()])

    # find N50 wrt file count.
    s = 0
    uid2x = {}
    cutoff = None
    for idx, (dfc, uid) in enumerate(sorted([(dfc, uid) for uid, [dfc, d, d, d] in delta.items()], reverse=True)):
        s += dfc 
        if 2*s > t_dfc: cutoff = idx
        uid2x[uid] = idx
    if 4*idx > len(delta): cutoff = 2

    #non_home_user = [(uid, MyTool.getUser(uid)) for uid, v in delta.items() if v[2]==0 or v[3]==0]
    #MyTool.logTmp("{} non_home_user={}".format(fs, non_home_user), time.time())
    r=[]
    for uid, v in delta.items():
        if uid < 1000:               # skip 
            continue       
        if 0 == v[2] or 0 == v[3]:   # curr_f==0 or curr_bc ==0, skip
            continue

        #uname = MyTool.getUser(uid, fakeName=False)   # slurm user name
        user      = ansible_users.get(uid, {})
        if not user:
           user['name']   = MyTool.getUser(uid, fakeName=False)
           user['status'] = '-'
           if user['name']:        # uid has an account
              user['expired'] = False
           else:
              user['name']    = 'User_{}'.format(uid)
              user['expired'] = True
        if anon:
           user['name'] = anonimize(user['name'])

        d         = {'x':v[3], 'y':v[2], 'z':log(max(2**20, v[1]),2)-19, 'dfb':v[1], 'dfc':v[0], 'id':uid, 'status':user['status'], 'name':user['name'], 'expired':user['expired']}
        if user['expired']: #'F' for flatiron, 'S' for simons, 'E' for external
           d['marker'] = {'fillColor': 'rgba(255,225,0,0.5)'}
        if cutoff and uid2x[uid] <= cutoff:
           d['marker'] = {'fillColor': 'rgba(236,124,181,0.9)'}  # pink mark file count cutoff
        r.append(d)

        #if not uname:                                 # expired user
           #uname       = ansible_users.get(uid, {'name':'User_{}'.format(uid)})['name']       # ansilble user name
           #uname       = "User_{}".format(uid) if not uname else uname
           #d['name']   = anonimize(uname)      if anon      else uname
           #d['marker'] = {'fillColor': 'rgba(255,225,0,0.5)'}   # yellow mark expired user
           #r.append(d)
        #else:
           #d['name']   = anonimize(uname)      if anon      else uname
           #if cutoff and uid2x[uid] <= cutoff:
           #   d['marker'] = {'fillColor': 'rgba(236,124,181,0.9)'}  # pink mark file count cutoff
           #r.append(d)

    return [label, r, yyyymmdd]

def test1(user):
    print("Test user {}'s history".format(user))
    start, stop = MyTool.getStartStopTS(days=30)
    uid = MyTool.getUid(user)
    print (gendata_user(uid, start, stop))
   
def test2():
    print("Test all's past 3 days history")
    stop         = int(time.time())
    start        = stop - 7*24*60*60
    fcSer, bcSer = gendata_all('ceph_full', start, stop, 5)
    print("fcSer={} \n\n bcSer={}".format(fcSer, bcSer))

def test3():
    yyyymmdd='20210224'
    gendata(yyyymmdd)

if '__main__' == __name__:
    #print (gendata(*sys.argv[1:]))
    #print (gendata_user(*sys.argv[1:]))
    #test1 (sys.argv[1])
    test2 ()

