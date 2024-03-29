import csv, re, sys, time
import operator, pwd,grp,logging,subprocess
import dateutil.parser
import _pickle as cPickle
import os.path

from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

import collections
if sys.version_info.major == 3 and sys.version_info.minor >= 10:
    from collections.abc import MutableMapping
else:
    from collections import MutableMapping

THREE_DAYS_SEC    = 3*24*3600
ONEDAY_SECS       = 24*3600

LOCAL_TZ   = timezone(timedelta(hours=-4))
ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccm','ccn','ccq', 'scc']))
    
def getAllUsers():
    lst = pwd.getpwall()
    return [p[0] for p in lst]

SLURM_USERS = {"Rusty":["./data/users.csv",0, {'name':'user'}, {}, {}], 
               "Popeye":  ["./data/sdsc.csv", 0, {},              {}, {}]}   #file_name, modify_time, rename_column, uid2record, user2record
                                                                             #popeye is for users that does not have the same id as in FI 
def refreshUsers (cluster):
    fileName = SLURM_USERS[cluster][0]
    if not os.path.isfile(fileName):
       return {}

    ts = os.path.getmtime(fileName)
    if ts > SLURM_USERS[cluster][1]:
       # modified since last read
       SLURM_USERS[cluster][3],SLURM_USERS[cluster][4] = readUserFile (fileName, SLURM_USERS[cluster][2])
       SLURM_USERS[cluster][1] = ts

def getUid2User (cluster):
    refreshUsers (cluster)
    return SLURM_USERS[cluster][3]
def getUser2Record (cluster):
    refreshUsers (cluster)
    return SLURM_USERS[cluster][4]

def readUserFile (filename, rename={}):
    uid2record  = {}
    user2record = {}
    with open (filename) as f:
         r        = csv.reader(f)
         header   = next(r)        #read header
                                   #user.csv 
                                   #uid,status,dept,name,firstname,lastname,mail,sponsor,groups,location,phone,cpuhours,gpuhours,storegb,comment,activated,expires,sdsc,shell,passwd
                                   #sdsc.csv 
                                   #user,name,uid,gid
         for nm, mod in rename.items():
             header[header.index(nm)] = mod
         for row in r:
             record        = dict(zip(header, row))
             record['uid'] = int(record['uid'])
             uid2record[record['uid']]  = record
             user2record[record['user']] = record
    return uid2record, user2record

def getAnsibleUsers(d):
    today = datetime.now()
    with open (os.path.join(d, "users.csv")) as f:
         r        = csv.reader(f)
         header   = next(r)        #skip head
         mapping  = dict([(h,idx) for idx, h in enumerate(header)])
         idx1     = mapping['status']
         idx2     = mapping['expires']
         #user2uid = dict((int(row[0]),{'name':row[3], 'status':row[idx1], 'expired':False if row[idx2] and datetime.strptime(row[idx2], "%Y/%m/%d") > today else True}) for row in r)
         uid2user = {}
         for row in r:
             expired  = False
             if row[idx2]:
                try:
                   expired = datetime.strptime(row[idx2], "%Y/%m/%d") < today
                except:
                   expired = False #datetime.strptime(row[idx2], "%Y-%m-%d") < today
             uid2user[int(row[0])] = {'name':row[3], 'status':row[idx1], 'expired':expired} 
    return uid2user

def getUserRecord (user, cluster):
    user2rec = getUser2Record(cluster);
    return user2rec.get(user, None)
    
def getUid (user, cluster="Rusty"):
    record = getUserRecord (user, cluster);
    if record:
       return record["uid"]
    elif cluster != "Rusty":  # sdsc.csv only include names that have the same name and uid
       record = getUserRecord (user, "Rusty");
       if record:
          return record["uid"]
    return None

def getUser (uid, cluster="Rusty", fakeName=True):
    if uid == None:
       return None

    uid      = int(uid)
    uid2user = getUid2User("Rusty")
    if uid in uid2user:
       return uid2user[uid]['user']
    elif cluster!="Rusty":     # popeye is for exception
       uid2user = getUid2User(cluster)
       if uid in uid2user:
          return uid2user[uid]['user']

    if fakeName:
       return "User_{}".format(uid)
    else:
       return None
    #p = getUserStruct(int(uid))
    #if p:
    #   return p.pw_name
    #if fakeName:
    #   return "User_{}".format(uid)
    #return None

def getUserFullName(user):
    refreshUsers("Rusty")
    records = SLURM_USERS["Rusty"][4]
    if user in records:
       r = records[user]
       print(r)
       return '{} {}'.format(r['firstname'], r['lastname']);
    else:
       return user

def getUserStruct (uid=None, uname=None):
    p = None
    try:
       if uid != None:
          p=pwd.getpwuid(int(uid))
       if uname:
          p=pwd.getpwnam(uname)
    except KeyError:
       #print('ERROR: MyTool::getUser user with uid {} cannot be found.'.format(uid))
       return None
    return p

def getUserGroups (user):
    groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    gid    = pwd.getpwnam(user).pw_gid
    groups.append(grp.getgrgid(gid).gr_name)
    return groups

def getUserOrgGroup (user):
    groups     = [g.gr_name for g in ORG_GROUPS if user in g.gr_mem]
    if groups:
       return groups[0]
    else:
       return 'unknown'

def expandRange(matchobj):
    l = list(range(int(matchobj.group(1)), int(matchobj.group(2))+1))
    s = repr(l)
    return s[1:len(s)-1]  # get ride of the string quote

def expandList(matchobj, width=4):
    l   = re.split(',\W*', matchobj.group(2))
    rlt = ""
    for item in l: rlt = rlt + matchobj.group(1) + str(item).zfill(width) +","
    return rlt[0:len(rlt)-1]
    
def expandStr(s, numberWidth=4):
    if '-' in s:
       s  = re.sub('([0-9]+)-([0-9]+)',       expandRange, s)
    #print("expandRange " + s)
    if '[' in s:
       s  = re.sub('([a-zA-Z0-9]+)\[(.*?)\]', expandList,  s)
    #print("expandList " + s)
    return s

#convert job['nodes'] short format to list: worker[1015-1031,1066-1077,1137-1154,1180-1193,1200-1202] to [worker1015, worker1016, ... worker1202]
def convert2list(s, numberWidth=4):
    #print("convert " + s)
    lststr = expandStr (s, numberWidth)
    #print("expandStr " + lststr)
    return re.split(',\W*', lststr)

#return string represention of ts in seconds
#'%b %d %H:%M:%S' 'Jan 08 23:11:31'
def getTS_strftime (ts, fmt='%Y/%m/%d'):
    d = datetime.fromtimestamp(ts)
    return d.strftime(fmt)
  
#2021-01-08 23:03:56
def getTsString (ts, sep=' ', timespec='seconds'):
    d = datetime.fromtimestamp(ts)
    return d.isoformat(sep, timespec)

def getUTCDateTime (local_ts):
    #print ("UTC " + datetime.fromtimestamp(local_ts, tz=DataReader.LOCAL_TZ).isoformat())
    return datetime.fromtimestamp(local_ts, LOCAL_TZ)

#parse '2020/02/10 21:15:01' (bright) and 2020-01-28T14:51:49 (sacct)
def str2ts (slurm_datetime_str):
    if (not slurm_datetime_str) or slurm_datetime_str=='Unknown':
       return 0
    d = dateutil.parser.parse(slurm_datetime_str)
    return int(d.timestamp())
    
def sub_dict(somedict, somekeys, default=None):
    return dict([ (k, somedict.get(k, default)) for k in somekeys ])
def sub_dict_remove(somedict, somekeys, default=None):
    return dict([ (k, somedict.pop(k, default)) for k in somekeys ])
#return only existing keys
def sub_dict_exist(somedict, somekeys):
    return dict([ (k, somedict[k]) for k in somekeys if (k in somedict) and (somedict[k])])
def sub_dict_exist_remove(somedict, somekeys):
    return dict([ (k, somedict.pop(k)) for k in somekeys if (k in somedict) and (somedict[k])])
def sub_dict_nonempty(somedict, somekeys):
    return dict([ (k, somedict[k]) for k in somekeys if (k in somedict) and (not emptyValue(somedict[k]))])
#return n seconds in slurm format Day-hour:min:second
def time_sec2str (n):
    n           = int(n)
    minute, sec = divmod (n, 60)
    hour, minute= divmod (minute, 60)
    day, hour   = divmod (hour, 24)
    return "{}-{:02d}:{:02d}:{:02d}".format(day, hour, minute, sec)
def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, MutableMapping):
           if v:
              items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
           if v:
           #unquoted string=boolean and cause invalid boolean error in influxDB
              items.append((new_key, repr(v)))
        else:
           items.append((new_key, v))
    return dict(items)

def flatten1 (d, keys):
    rlt = {}
    for key in keys:
        if not d[key]: continue
        for k2, v2 in d[key].items():
            rlt['{}_{}'.format(key, k2)] = v2
    return rlt

def emptyValue (v):
    return (not v) or (v in ['N/A', 'None', 'NONE', 'UNLIMITED', 'Unknown'])

#assume remove_dict_empty has been called before
#update fields who are dict or other to string
def dict_complex2str (d):
    for k, v in d.items():
        if not isinstance (v, (int, float, str, bool)):
           d[k] = repr(v)
    return d

#update fields who are dict or other to string
def remove_dict_empty (d):
    emptyKey = []
    for k, v in d.items():
        if emptyValue(v):
           emptyKey.append (k)
        elif isinstance(v, dict):
           v=dict([(k1,v1) for k1,v1 in v.items() if not emptyValue(v1)])
           if v:
              d[k] = v
           else:
              emptyKey.append(k)
    for k in emptyKey:
        d.pop(k)
    return d

#did not consider str to int
def getDictNumValue (d, key):
    value = d.get(key, 0)
    if ( type(value) == int or type(value) == float):
       return value
    else:
       print("WARNING: getDictIntValue has a non-int type " + repr(type(value)))
       return 0

def getTS (t, formatStr='%Y%m%d'):
    return int(time.mktime(time.strptime(t, formatStr)))
    
def getStartStopTS (start='', stop=None, formatStr='%Y%m%d', days=3, setStop=True):
    if stop:
        if isinstance (stop, str):
           stop = time.mktime(time.strptime(stop, formatStr))
        else:
           stop = int(stop)
    elif setStop:
        stop = int(time.time())+1
    # else: stop remains as None

    if start:
        if isinstance (start, str):
           start = time.mktime(time.strptime(start, formatStr))
        else:
           start = int(start)
    else:
        start = max(0, int(time.time()) - days * ONEDAY_SECS)

    return start, stop

def func1 (df):
    return df[['user','job','load']].groupby('user').apply(func2)

def func2 (df):
    df.groupby('partition').apply(lambda x: x[['user','job','node','load']].to_dict(orient='record'))
    df.groupby('partition').apply(lambda x: x.groupby('user').apply(lambda y: y.groupby('job').apply(lambda z: z.to_dict(orient='record'))))
    df.groupby(['partition', 'user', 'job']).apply(lambda x: x[['node','load']].to_dict(orient='record'))
    d = {k: f.groupby('user')['job', 'node', 'load'].apply(lambda x: x.to_dict(orient='record')).to_dict() for k, f in df.groupby('partition')}

    return df.groupby('job').apply(func3)
    
def func3 (df):
    return df[['node','load']].to_dict(orient='record')

# createNestedDict
def list2nestedDict (rootSysname, data_lst, level_lst, val_idx):
    #Find element in children list if exists or return none
    def find_element(children_list,name):
            for i in children_list:
                if i["name"] == name:
                    return i
            #If not found return None
            return None

    def add_node(path, value, parent):
            """
            The path is a list.  Each element is a name that corresponds 
            to a level in the final nested dictionary.  
            """
            #Get first name from path
            this_name = path.pop(0)
            while (not this_name) and (len(path)>0):  # skip the None value in path
               this_name = path.pop(0)
            #Does the element exist already?
            element   = find_element(parent["children"], this_name)

            #If the element exists, we can use it, otherwise we need to create a new one
            if not element:
               if len(path)>0:
                  element = {"name": this_name, "children":[]}
               else:
                  element = {"name": this_name, "value":value}
               parent["children"].append(element)
            if len(path)>0:
               add_node(path,value, element)

    root   = {"name": "root", "sysname":rootSysname, "children": []}
    for row in data_lst:
        value = row[val_idx]
        path  = [row[i] for i in level_lst]
        add_node (path, value, root)
    return root


def createNestedDict (rootSysname, levels, data_df, valColname):
        def find_element(children_list,name):
            """
            Find element in children list
            if exists or return none
            """
            for e in children_list:
                if e["name"] == name:
                    return e
            #If not found return None
            return None

        def add_node(path,value,nest, level=0):
            """
            The path is a list.  Each element is a name that corresponds 
            to a level in the final nested dictionary.  
            """
            #Get first name from path
            this_name = path.pop(0)

            #Does the element exist already?
            element = find_element(nest["children"], this_name)

            #If the element exists, we can use it, otherwise we need to create a new one
            if element:
                if len(path)>0:
                    add_node(path,value, element, level+1)
                    #Else it does not exist so create it and return its children
            else:
                if len(path) == 0:
                    #TODO: Hack, Replace when we've redesigned the data representation.
                    url = ''
                    if level == 3:
                        url = 'nodeDetails?node=' + this_name

                    nest["children"].append({"name": this_name, "value": value, 'url': url})
                else:
                    #TODO: Hack, Replace when we've redesigned the data representation.
                    url = ''
                    if level == 2:
                        url = 'jobDetails?jid=' + str(this_name)
                    elif level == 1:
                        url = 'userDetails?user=' + this_name
                    #Add new element
                    nest["children"].append({"name": this_name, 'url': url, "children":[]})
                    #Get added element 
                    element = nest["children"][-1]
                    #Still elements of path left so recurse
                    add_node(path,value, element, level+1)

        # createNestedDict
        root   = {"name": "root", "children": []}
        root["sysname"] = rootSysname

        for row in data_df.iterrows():
            r     = row[1]
            path  = list(r[levels])
            value = r[valColname]
            add_node(path,value,root)

        return root

def loadSwitch(c, l, low, normal, high):
    if c == -1 or c < l-1: return high
    if c > l+1: return low
    return normal

def most_common(lst):
    if len(lst)>0:
        return max(set(lst), key=lst.count)

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    return [ atoi(c) for c in re.split('(\d+)', text) ]

def mean(data):
    """Return the sample arithmetic mean of data."""
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')
    return sum(data)/n # in Python 2 use sum(data)/float(n)

def _ss(data):
    """Return sum of square deviations of sequence data."""
    c = mean(data)
    ss = sum((x-c)**2 for x in data)
    return ss

def pstdev(data):
    """Calculates the population standard deviation."""
    n = len(data)
    if n < 2:
        raise ValueError('variance requires at least two data points')
    ss = _ss(data)
    pvar = ss/n # the population variance
    return pvar**0.5

# data are all positive numbers
def stddev(data):
    d = pstdev(data)
    m = mean(data)
    if m !=0 :
       return d/m
    else:
       return 0

# extract 56 from "1=56,2=1024000,4=2"
# used for map function when only one parameter is used, otherwise use getTresDict
def extract1 (s):
    if not s:
       print("extrac1 has a empty value")
       return 0
    d = getTresDict(s)
    return d.get('1',0)

def extract2 (s):
    if not s:
       return 0
    d = getTresDict(s)
    return d.get('2',0)

def extract4 (s):
    if not s:
       print("extrac4 has a empty value")
       return 0
    d = getTresDict(s)
    return d.get('4',0)

#a=1,b=2 to {a:1, b:2}
def str2dict (dict_str):
    if not dict_str:
       return None
    d = {}
    for item_str in dict_str.split(','):
        k, v     = item_str.split('=')
        d[k]     = v
    return d

def getDFBetween(df, field, start=None, stop=None):
    if start:
        if type(start) != int:
           start = time.mktime(time.strptime(start, '%Y-%m-%d'))
        df    = df[df[field] >= start]
    if stop:
        if type(stop) != int:
           stop  = time.mktime(time.strptime(stop,  '%Y-%m-%d'))
        df    = df[df[field] <= stop]
    if not df.empty:
        start = df.iloc[0][field]      # first item
        stop  = df.iloc[-1][field]     # last item

    return start, stop, df

# Expand slurm's cute compressed node listing scheme into a full list
# of nodes.
def nl2flat(nl):
    if not nl: return []
    flat = []
    prefix = None
    for x in re.split(r'(\[.*?\])', nl):
        if x == '': continue
        if x[0] == '[':
            x = x[1:-1]
            for r in x.split(','):
                lo = hi = r
                if '-' in r:
                    lo, hi = r.split('-')
                fmt = '%s%%0%dd'%(prefix, max(len(lo), len(hi)))
                for x in range(int(lo), int(hi)+1): flat.append(fmt%x)
            prefix = None
        else:
            if prefix: flat.append(prefix)
            pp = x.split(',')
            [flat.append(p) for p in pp[:-1] if p]
            prefix = pp[-1]
    if prefix: flat.append(prefix)
    return flat

def gresList2Dict(gresList):
    return dict([tuple(gres.split(':')) for gres in gresList])
 
def gresList2Str (gresList):
    rlt = ''
    for gres in gresList:
        if rlt: rlt += ','
        rlt += gres.replace(':','=')
    return rlt

def readFile (filename):
   result = None
   if os.path.isfile(filename) and os.stat(filename).st_size>0:
      with open(filename, 'rb') as f:
         result = cPickle.load(f)
   return result

def writeFile (filename, data):
   with open(filename, 'wb') as f:
      cPickle.dump(data, f)

#convert 100G and 100M to xxK
def convert2K (s):
    s = str(s)
    if s[-1] == 'G':
       return int(s[0:-1]) * 1024 * 1024
    elif s[-1] == 'M':
       return int(s[0:-1]) * 1024
    elif s[-1] == 'K':
       return int(s[0:-1])
    else:
       return int(s) / 1024

#Rotate file logger
def getFileLogger (name, level=logging.WARNING, file_name=None, rotate=True):
    if name not in logging.root.manager.loggerDict:
       if not file_name:
          file_name = '{}_{}.log'.format(name, getTsString(time.time(), '_', timespec='hours'))
       f_format  = logging.Formatter('%(asctime)s %(levelname)s %(module)s::%(funcName)s - %(message)s')
       if rotate:
          f_handler = RotatingFileHandler(file_name, maxBytes=1<<20, backupCount=5)
       else:
          f_handler = logging.FileHandler(file_name)
       f_handler.setLevel(level)
       f_handler.setFormatter(f_format)

    logger    = logging.getLogger(name)
    logger.addHandler(f_handler)
    logger.setLevel  (level)

    return logger

def getSeqDiff (seq, idx, no_neg=False):
    x1    = [ item[idx] for item in seq ]
    x2    = x1[0:len(x1)-1]
    x1.pop(0)
    rlt   = list(map(operator.sub, x1, x2))
    if no_neg:
       rlt = [max(0,i) for i in rlt]
    return rlt

#get derivative of seq 
def getSeqDeri (seq, xIdx, yIdx, no_neg=False):
    xDiff = getSeqDiff (seq, xIdx)
    yDiff = getSeqDiff (seq, yIdx, no_neg)
    return list(map(operator.truediv, yDiff, xDiff))

def getSeqDeri_x (seq, xIdx, yIdx, no_neg=False):
    deri = getSeqDeri(seq, xIdx, yIdx, no_neg)
    x    = [ item[xIdx] for item in seq ]
    x.pop(0)
    return [[item[0], item[1]] for item in zip(x,deri)]

#job 'tres_alloc_str': 'cpu=2,mem=36000M,node=2,billing=2,gres/gpu=4'
#    'tres_req_str': 'cpu=2,mem=36000M,node=2,billing=2,gres/gpu=4'
def getTresGPUCount (tres_str):
    if not tres_str:
       return 0
    m = re.search('gres/gpu=(\d+)', tres_str)
    if not m or not m.group(0):
       return 0
    return int(m.group(1))
    
#slurm 20 'gres': ['gpu:v100-16gb:2(S:0-1)'],  ['gpu:v100s-32gb:4']
def getNodeGresStringGPUCount (gres_str):
    m = re.search('gpu:(.+?):(\d+).*', gres_str)  #.+? match not greepy
    if not m or not m.group(0):
       return 0
    return int(m.group(2))

#node 'gres': ['gpu:k40c:1', 'gpu:k40c:1'], 'gres_used': ['gpu:k40c:2(IDX:0-1)', 'mic:0']
#slurm 20: ['gpu:v100-16gb:2(S:0-1)']
def getNodeGresGPUCount (gres_list):
    if not gres_list:
       return 0
    gpu_total = sum([getNodeGresStringGPUCount(g) for g in gres_list if 'gpu:' in g])
    return gpu_total

#['gpu:v100s-32gb:4(IDX:0-3)', 'mic:0']
#['gpu:v100s-32gb:0', 'mic:0'], no gpu used
def getNodeGresUsedGPUCount (gres_used_list):
    if not gres_used_list:
       return 0
    gpu_used  = 0
    for gres in gres_used_list:
       #if 'gpu:' not in gres:    #not gpu, ignore
       #    continue
       m = re.search('gpu:(.+):(\d+)\(IDX:(.+)\)', gres)
       if not m or not m.group(0):
           continue
       cate      = m.group(1)
       gpu_used += int(m.group(2))
       idx_str   = m.group(3)
       #idx_list = str2intList (idx_str)
       #print ("getNodeGresUsedGPUCount {} cate={},gpu_used={},idx_str={}".format(gres_used_list, cate, gpu_used, idx_str))
    return gpu_used

#node['gres']: ['gpu:k40c:1', 'gpu:k40c:1'], 'gres_used': ['gpu:k40c:2(IDX:0-1)', 'mic:0']
#     'tres_fmt_str': 'cpu=28,mem=375G,billing=28,gres/gpu=2'
#return gpus, alloc_gpus, alloc_gpus_idx
def getGPUCount (gres_list, gres_used_list=[]):
    gpu_total = getNodeGresGPUCount (gres_list)
    if not gpu_total:
       return 0, 0

    gpu_used  = getNodeGresUsedGPUCount (gres_used_list) if gres_used_list else 0
    return gpu_total, gpu_used

#gpu:v100-16gb(IDX:0-1) or gpu(IDX:0-3) or gpu(IDX:0,3), in job['gres_detail']
#TODO: check node format
def parse_gpu_detail(gpu_str):
    rlt = []
    m = re.match('gpu.*\(IDX:(.+)\)', gpu_str)
    if not m or not m.group(1):
       return rlt
    # idx_str 0-1 or 0
    lst1 = m.group(1).split(',')
    for i1 in lst1:
       lst2 = [eval(item) for item in i1.split('-')]
       if len(lst2) > 1:
          lst2 = list(range(lst2[0], lst2[1]+1))
       rlt.extend(lst2)
    return rlt

#return gpu allocate index on node_iter, {node: [0],}
def getGPUAlloc_layout (node_iter, gpu_detail_iter):
    #assert (len(node_iter) == len(gpu_detail_iter))
    #print("getGPUAlloc_layout {}\n\t{}".format(node_iter, gpu_detail_iter))
    result  = collections.defaultdict(list)
    idx     = 0
    for node in node_iter:
        if not node['gres']:
            continue
        gpu_total, gpu_used = getGPUCount(node['gres'])
        if gpu_total:  #gpu node
           if idx >= len(gpu_detail_iter):
              break
           result[node['name']]=parse_gpu_detail(gpu_detail_iter[idx])
           idx += 1

    return dict(result)
     
#'tres_req_str': 'cpu=400,node=10,billing=400'
#'tres_fmt_str': 'cpu=1612,mem=28375G,node=43,billing=1612,gres/gpu=148'
#mapKey=true, then for "1=40,2=700000", return {cpu:40,...}
#+---------------+---------+------+----------------+------+
#| creation_time | deleted | id   | type           | name |
#+---------------+---------+------+----------------+------+
#|    1505726834 |       0 |    1 | cpu            |      |
#|    1505726834 |       0 |    2 | mem            |      |
#|    1505726834 |       0 |    3 | energy         |      |
#|    1505726834 |       0 |    4 | node           |      |
#|    1544835407 |       0 |    5 | billing        |      |
#|    1553370225 |       0 |    6 | fs             | disk |
#|    1553370225 |       0 |    7 | vmem           |      |
#|    1553370225 |       0 |    8 | pages          |      |
#|    1536253094 |       1 | 1000 | dynamic_offset |      |
#|    1559052921 |       0 | 1001 | gres           | gpu  |
#+---------------+---------+------+----------------+------+
#
TRES_KEY_MAP={1:'cpu',2:'mem',4:'node',1001:'gpu'}
def getTresDict (tres_str, mapKey=False):
    d  = {}
    if tres_str and isinstance(tres_str, str):
       for item in tres_str.split(','):
           k,v  = item.split('=')
           if v.isnumeric():
              v = int(v)
           if mapKey and k.isnumeric():
              k = TRES_KEY_MAP[int(k)]
           d[k] = v
    return d

#input is Bps
def getDisplayBps (n):
   return '{}Bps'.format(getDisplayF(n))
#input is B
def getDisplayB (n):
   if n < 1024:
      return '{}B'.format(n)
   n /= 1024
   return getDisplayKB(n)
#input is nKB
def getDisplayKB (n):
    return '{}B'.format(getDisplayK(n))

def getDisplayF (n):
   if n < 1024:
      return '{:.2f}'.format(n)
   n /= 1024
   return getDisplayK(n)

def getDisplayI (n):
   if n < 1024:
      return '{}'.format(n)
   n /= 1024
   return getDisplayK(n)

#get display of nK
def getDisplayK (n):
   if n < 1024:
      if isinstance(n, int):
         return '{}K'.format(n)
      else:
         return '{:.2f}K'.format(n)
   n /= 1024
   if n < 1024: 
      return '{:.2f}M'.format(n)
   n /= 1024
   if n < 1024: 
      return '{:.2f}G'.format(n)
   n = n / 1024
   return '{:.2f}T'.format(n)

#sum of ['123', '123K', '123M']
def sumOfListWithUnit_lst (lst):
    if not lst:   
       return 0,''
    lst_D = list(filter(lambda x: str.isnumeric(x), lst))
    lst_K = list(filter(lambda x: x[-1]=='K', lst))
    lst_M = list(filter(lambda x: x[-1]=='M', lst))
    lst_G = list(filter(lambda x: x[-1]=='G', lst))
    total = 0
    if lst_G:
       total   = sum([float(n[:-1]) for n in lst_G])
       postfix = 'G'
    if lst_M:
       total   = sum([float(n[:-1]) for n in lst_M]) + total * 1024
       postfix = 'M'
    if lst_K:
       total   = sum([float(n[:-1]) for n in lst_K]) + total * 1024
       postfix = 'K'
    if lst_D:
       total   = sum([float(n)      for n in lst_D]) + total * 1024
       postfix = ''
    return total, postfix

def sumOfListWithUnit (lst):
    total, postfix = sumOfListWithUnit_lst(lst)
    return str(total) + postfix
    
def convert2M (mem_unit):
    if mem_unit[-1] in ['K','M','G', 'T']:
       mem,unit = float(mem_unit[:-1]),mem_unit[-1]
    else:
       mem      = float(mem_unit)
       unit     = ''
    mem_MB   = mem
    if unit == 'T':
       mem_MB = mem * 1024 * 1024
    elif unit == 'G':
       mem_MB = mem * 1024
    elif unit == 'K':
       mem_MB = mem / 1024
    elif unit == '':
       mem_MB = mem / 1024 / 1024
    return mem_MB

#seq is a list [[time_msec, value], ....]
#startTS and stopTS is ts
def getTimeSeqAvg (seq, startTS, stopTS):
        #print("getTimeSeqAvg {}-{}, seq={}".format(startTS, stopTS, seq))
        if not seq:         return 0
        #skip the data before startTS
        idx= 0
        while (idx < len(seq)) and (seq[idx][0] < startTS):  idx+= 1    
        if idx == len(seq): return 0

        #seq[idx].time > startTS > seq[idx-1].time
        #calculate total during [startTS, seq[idx].time
        if idx == 0: #no value before seq[0].time
           total   = 0
           startTS = seq[0][0]
        else:
           total   = seq[idx-1][1] * (seq[idx][0]-startTS)   
        #print("getTimeSeqAvg {}:{} total={}".format(idx, seq[idx], total))
        # sum over until stopTS
        savIdx = idx
        idx   += 1
        while idx < len(seq):
           if seq[idx][0] > stopTS:
              total += seq[idx-1][1] * (stopTS - seq[idx-1][0])
              #print("getTimeSeqAvg {}:{} total={}".format(idx, seq[idx], total))
              break
           total += seq[idx-1][1] * ((seq[idx][0] - seq[idx-1][0]))
           #print("getTimeSeqAvg {}:{} total={}".format(idx, seq[idx], total))
           idx   += 1
        #print("---total={},avg={},seq={}".format(total, total / (stopTS - startTS), seq[savIdx:]))
        return total / (stopTS - startTS)

def df_cmd (uname):
        cmd = ['df', '-h', '/mnt/home/'+uname]
        try:
            d = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return 'Command "%s" returned %d with output %s.<br>'%(' '.join(cmd), e.returncode, repr(e.output))
        line = d.decode('utf-8').splitlines()[1]
        lst  = line.split()  #gpfs-nfs:/mnt/home  1.0T   29G  996G   3% /mnt/home
        return "{} ({} of {} quota)".format(lst[2], lst[4], lst[1])

def getDisplayN (n):
    if n < 1024:
       return n
    n = n / 1024
    if n < 1024:
       return "{:.2f}K".format(n)
    n = n / 1024
    if n < 1024:
       return "{:.2f}M".format(n)
    n = n / 1024
    if n < 1024:
       return "{:.2f}G".format(n)
    n = n / 1024
    if n < 1024:
       return "{:.2f}T".format(n)

def logTmp (msg, pre_ts):
        with open("tmp.log", "a") as f:
             f.write("Took {}:{}\n".format(time.time()-pre_ts, msg))
        return time.time()

def test1():
    level = logging.DEBUG
    logger=getFileLogger('test', level)
    logger.log     (level, "log level {}".format(logger.getEffectiveLevel()))
    logger.debug   ("debug")
    logger.info    ("info")
    logger.warning ("warning")
    logger.error   ("error")
    logger.critical("error")
    
def test2(argv):
    for s in argv:
        c = convert2list(s)
        print (repr(c))

def test3():
    seq = [[1000, 10], [3000, 40], [5000, 4], [10000, 9]]
    startTS =1
    stopTS  =11
    print(getTimeSeqAvg(seq, startTS, stopTS))

def test4():
    user = getUser(519052, cluster="Popeye")
    print(user)
    user = getUser(519052)
    print(user)
    uid  = getUid("aojha", "Rusty")
    print(uid)
    uid  = getUid("aojha", "Popeye")
    print(uid)
    name = getUserFullName("yliu")
    print(name)

def main(argv):
    test4()

if __name__=="__main__":
   main(sys.argv[1:])
