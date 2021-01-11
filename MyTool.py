import sys
import re
import collections
import time
import operator, pwd,grp,logging
import dateutil.parser
import _pickle as cPickle
import os.path

from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

THREE_DAYS_SEC     = 3*24*3600
ONEDAY_SECS       = 24*3600

LOCAL_TZ   = timezone(timedelta(hours=-4))
ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccm','ccn','ccq', 'scc']))
    
def getAllUsers():
    lst = pwd.getpwall()
    return [p[0] for p in lst]

def getUid (user):
    p = getUserStruct(uname=user)
    if p:
       return p.pw_uid
    return None

def getUser (uid, returnName=True):
    p = getUserStruct(int(uid))
    if p:
       return p.pw_name
    if returnName:
       return "User_{}".format(uid)
    return None

def getUserFullName(uid, returnName=True):
    p = getUserStruct(int(uid))
    if p:
       return p.pw_gecos.split(',')[0]
    if returnName:
       return "User {}".format(uid)
    return None

def getUserStruct (uid=None, uname=None):
    p = None
    try:
       if uid != None:
          p=pwd.getpwuid(int(uid))
       if uname:
          p=pwd.getpwnam(uname)
    except KeyError:
       print('ERROR: MyTool::getUser user with uid {} cannot be found.'.format(uid))
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

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, collections.MutableMapping):
           if v:
              items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
           if v:
           #unquoted string=boolean and cause invalid boolean error in influxDB
              items.append((new_key, repr(v)))
        else:
           items.append((new_key, v))
    return dict(items)

def emptyValue (v):
    return (not v) or (v in ['N/A', 'None', 'NONE', 'UNLIMITED', 'Unknown'])

#assume remove_dict_empty has been called before
#update fields who are dict or other to string
def update_dict_value2string (d):
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
    
def getStartStopTS (start='', stop='', formatStr='%Y%m%d', days=3):
    if stop:
        stop = time.mktime(time.strptime(stop, formatStr))
    else:
        stop = time.time()
    if start:
        if isinstance (start, str):
           start = time.mktime(time.strptime(start, formatStr))
    else:
        start = max(0, int(stop - days * ONEDAY_SECS))

    return int(start), int(stop)

def createNestedDict (rootSysname, levels, data_df, valColname):
        def find_element(children_list,name):
            """
            Find element in children list
            if exists or return none
            """
            for i in children_list:
                if i["name"] == name:
                    return i
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

# extract 56 from "1=56,2=1024000,4=2"
def extract1 (s):
    if not s:
       print("extrac1 has a empty value")
       return 0
    lst = s.split(',')[0].split('=')
    if len(lst) > 1:
       return int(lst[1])
    else:
       return 0

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

def getSeqDiff (seq, idx):
    x1    = [ item[idx] for item in seq ]
    x2    = x1[0:len(x1)-1]
    x1.pop(0)
    return list(map(operator.sub, x1, x2))

#get derivative of seq 
def getSeqDeri (seq, xIdx, yIdx):
    xDiff = getSeqDiff (seq, xIdx)
    yDiff = getSeqDiff (seq, yIdx)
    return list(map(operator.truediv, yDiff, xDiff))

def getSeqDeri_x (seq, xIdx, yIdx):
    deri = getSeqDeri(seq, xIdx, yIdx)
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

    gpu_used  = getNodeGresUsedGPUCount (gres_used_list)
    return gpu_total, gpu_used

#gpu:v100-16gb(IDX:0-1) or gpu(IDX:0-3) or gpu(IDX:0,3), in job['gres_detail']
def parse_gpu_detail(gpu_str):
    m = re.match('gpu.*\(IDX:(.+)\)', gpu_str)
    if not m or not m.group(1):
       return None
    # idx_str 0-1 or 0
    rlt = []
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
TRES_KEY_MAP={1:'cpu',2:'mem',4:'node',1001:'gpu'}
def getTresDict (tres_str, mapKey=False):
    d  = {}
    if tres_str:
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
def sumOfListWithUnit (lst):
    if not lst:   
       return '0'
    lst_D = list(filter(lambda x: str.isnumeric(x), lst))
    lst_K = list(filter(lambda x: x[-1]=='K', lst))
    lst_M = list(filter(lambda x: x[-1]=='M', lst))
    lst_G = list(filter(lambda x: x[-1]=='G', lst))
    total = 0
    if lst_G:
       total   = sum([int(n[:-1]) for n in lst_G])
       postfix = 'G'
    if lst_M:
       total   = sum([int(n[:-1]) for n in lst_M]) + total * 1024
       postfix = 'M'
    if lst_K:
       total   = sum([int(n[:-1]) for n in lst_K]) + total * 1024
       postfix = 'K'
    if lst_D:
       total   = sum([int(n)      for n in lst_D]) + total * 1024
       postfix = ''
    return str(str(total) + postfix)

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

def main(argv):
    test3()

if __name__=="__main__":
   main(sys.argv[1:])
