import sys
import re
import collections
from datetime import datetime, timezone, timedelta
import pwd,grp

LOCAL_TZ   = timezone(timedelta(hours=-4))
#ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccm', 'ccq', 'scc']))
ORG_GROUPS = list(map((lambda x: grp.getgrnam(x)), ['genedata','cca','ccb','ccq', 'scc']))

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
    return s[1:len(s)-1]

def expandList(matchobj):
    l   = re.split(',\W*', matchobj.group(2))
    rlt = ""
    for item in l: rlt = rlt + matchobj.group(1) + str(item) +","
    return rlt[0:len(rlt)-1]
    
def expandStr(s):
    #print("input " + s)
    #if s contains -
    if '-' in s:
       s  = re.sub('([0-9]+)-([0-9]+)',       expandRange, s)
    #print("expandRange " + s)

    #if s1 contains []
    if '[' in s:
       s  = re.sub('([a-zA-Z0-9]+)\[(.*?)\]', expandList,  s)
    #print("expandList " + s)
   
    return s

def convert2list(s):
    #print("convert " + s)
    lststr = expandStr (s)
    #print("expandStr " + lststr)
    lst    = re.split(',\W*', lststr)
    #print("result " + repr(lst))
    return lst

def getUTCDateTime (local_ts):
    #print ("UTC " + datetime.fromtimestamp(local_ts, tz=DataReader.LOCAL_TZ).isoformat())
    return datetime.fromtimestamp(local_ts, LOCAL_TZ)

def sub_dict(somedict, somekeys, default=None):
    return dict([ (k, somedict.get(k, default)) for k in somekeys ])
def sub_dict_remove(somedict, somekeys, default=None):
    return dict([ (k, somedict.pop(k, default)) for k in somekeys ])

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
        elif v:
           items.append((new_key, v))
    return dict(items)

def main(argv):
    for s in argv:
        convert2list(s)

if __name__=="__main__":
   main(sys.argv[1:])
