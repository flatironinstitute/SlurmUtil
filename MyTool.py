import sys
import re
import collections
from datetime import datetime, timezone, timedelta
import time
import pwd,grp
import dateutil.parser

LOCAL_TZ   = timezone(timedelta(hours=-4))
#ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccm', 'ccq', 'scc']))
ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccq', 'scc']))

def getUid (user):
    return pwd.getpwnam(user).pw_gid

def getUser (uid):
    try:
       p=pwd.getpwuid(int(uid))
    except KeyError:
       return('UidNotFound')

    return p.pw_name

def getUserGroups (user):
    groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    gid    = pwd.getpwnam(user).pw_gid
    groups.append(grp.getgrgid(gid).gr_name)

    return groups

def getUserOrgGroup (user):
    #groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    #gid = pwd.getpwnam(user).pw_gid
    #groups.append(grp.getgrgid(gid).gr_name)
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
def getTS_strftime (ts, fmt='%Y/%m/%d'):
    d = datetime.fromtimestamp(ts)
    return d.strftime(fmt)
  
def getTimeString (ts):
    d = datetime.fromtimestamp(ts)
    return d.isoformat(' ')

def getUTCDateTime (local_ts):
    #print ("UTC " + datetime.fromtimestamp(local_ts, tz=DataReader.LOCAL_TZ).isoformat())
    return datetime.fromtimestamp(local_ts, LOCAL_TZ)

def getSlurmTimeStamp (slurm_datetime_str):
    d = dateutil.parser.parse(slurm_datetime_str)
    return d.timestamp()
    
def upd_dict(d1, d2):
    d2.update(d1)
    return d2

def sub_dict(somedict, somekeys, default=None):
    return dict([ (k, somedict.get(k, default)) for k in somekeys ])
def sub_dict_remove(somedict, somekeys, default=None):
    return dict([ (k, somedict.pop(k, default)) for k in somekeys ])

#did not consider str to int
def getDictNumValue (d, key):
    value = d.get(key, 0)
    if ( type(value) == int or type(value) == float):
       return value
    else:
       print("WARNING: getDictIntValue has a non-int type " + repr(type(value)))
       return 0

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k

        #if k == 'cpus_allocated': print(d)
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
                        url = 'jobDetails?jobid=' + str(this_name)
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

def getDFBetween(df, field, start, stop):
    if start:
        if (type(start) != int):
           start = time.mktime(time.strptime(start, '%Y-%m-%d'))
        df    = df[df[field] >= start]
    if stop:
        if (type(stop) != int):
           stop  = time.mktime(time.strptime(stop,  '%Y-%m-%d'))
        df    = df[df[field] <= stop]
    if not df.empty:
        start = df.iloc[0][field]
        stop  = df.iloc[-1][field]

    return start, stop, df

# Expand slurm's cute compressed node listing scheme into a full list
# of nodes.
def nl2flat(nl):
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

def main(argv):
    for s in argv:
        c = convert2list(s)
        print (repr(c))

if __name__=="__main__":
   main(sys.argv[1:])
