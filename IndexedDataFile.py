#!/usr/bin/env python
import _pickle as pickle
import os.path, sys, zlib
from collections import defaultdict
import time
import config, MyTool

logger = config.logger
class IndexedDataFile(object):
    def __init__(self, prefix, name):
        self.prefix    = prefix + '/'
        self.name      = name
        self.data_file = None

    def __del__ (self):
        if ( self.data_file != None):
           self.data_file.close()

    def writeData(self, ts, d):
        #save information to files
        with open(self.prefix+self.name+'.p', 'ab') as df, open(self.prefix+self.name+'.px', 'a') as idx:
            zps = zlib.compress(pickle.dumps(d))
            idx.write('%020d%020d'%(ts, df.tell()))

            df.write('{:0>20}'.format(int(ts)).encode('utf-8'))
            df.write('{:0>20}'.format(len(zps)).encode('utf-8'))
            df.write(zps)

    #read one piece of data starting at offset, return timestamp, data
    def readData(self, offset, stopTime):
      if ( self.data_file == None):
          self.data_file = open (self.prefix + self.name+'.p', 'rb')

      if offset != self.data_file.tell():
         self.data_file.seek(offset, 0)

      ts = int(self.data_file.read(20))
      if ts > stopTime:  # no history saved
         return ts, None
      len = int(self.data_file.read(20))
      return ts, pickle.loads(zlib.decompress(self.data_file.read(len)))


class IndexedHostData(object):
    def __init__(self, prefix, hostname=None):
        self.prefix    = prefix
        self.hostname  = hostname
        if ( hostname != None ):
           self.data_file = open(self.prefix + '/%s_sm.p'%hostname, 'rb')
        else:
           self.data_file = None
   
    def __del__ (self):
      if ( self.data_file != None ):
         self.data_file.close()      
        
    def writeData(self, hostname, ts, d):
        #save information to files
        with open(self.prefix+'/%s_sm.p'%hostname, 'ab') as df, open(self.prefix+'/%s_sm.px'%hostname, 'a') as idx:
            zps = zlib.compress(pickle.dumps(d))
            idx.write('%020d%020d'%(ts, df.tell()))

            df.write('{:0>20}'.format(int(ts)).encode('utf-8'))
            df.write('{:0>20}'.format(len(zps)).encode('utf-8'))
            df.write(zps)

    #readData is used to read data from one file successively
    def readData(self, df, offset, stopTime):
      if offset != df.tell(): 
         df.seek(offset, 0)
      ts = int(df.read(20))
      if ts > stopTime:  # no history saved
         return ts, None
      len = int(df.read(20))
      return ts, pickle.loads(zlib.decompress(df.read(len)))

    def readData4(self, hostname, offset, stopTime):
        with open(self.prefix+'/%s_sm.p'%hostname, 'rb') as df:
            if offset != df.tell(): 
                df.seek(offset, 0)
            ts = int(df.read(20))
            if ts > stopTime:
                return ts, None

            len = int(df.read(20))
            return ts, pickle.loads(zlib.decompress(df.read(len)))

    def getIndexFileName (self, hostname):
        return '{}/{}_sm.px'.format(self.prefix, hostname)
    def getDataFileName (self, hostname):
        return '{}/{}_sm.p'.format(self.prefix, hostname)

    def queryDataHosts(self, nodes, start, end, uname=None):
        cpu_all_seq, mem_all_seq, io_all_seq = [], [], []
        for node in nodes:
            lst = self.queryData (node, start, end, uname)
            if lst:
               cpu_all_seq.append(lst[0])
               mem_all_seq.append(lst[1])
               io_all_seq.append (lst[2])
        return cpu_all_seq, mem_all_seq, io_all_seq

    #query data of the user procs on hostname during (ts_start, ts_end)
    def queryData(self, hostname, ts_start, ts_end=None, username=None):
        fname = self.getDataFileName(hostname)
        if not os.path.isfile(fname):
           logger.error("Cannot locate file {}".format(fname))

        with open(fname, 'rb') as df:
          idx      = SearchIndex(self.getIndexFileName(hostname), 40)
          idx_pos,ts_pos = idx.find(ts_start)
          if ts_end and ts_end < ts_pos:
             logger.warning("Host {} data during {}-{} is not saved in the file {}-".format(hostname, ts_start, ts_end, ts_pos))
             return None

          ms_cpu   = defaultdict(list) #{ts: []}
          ms_rssKB = defaultdict(list)
          ms_io    = defaultdict(list)
          for x in range(idx_pos, idx.len):
            offset = int(idx[x][20:])
            ts, nd = self.readData (df, offset, ts_end) #state, delta, ts, procsByUser
            if ( nd == None): 
               break
            else:
               #save the data
               ts = int(nd[2])
               for usrdata in nd[3:]: # [u_name, uid, alloc_cpu, len(pp), totIUA, totRSS, totVMS, pp, totIO, totCPU
                   if not username:
                      ms_cpu[ts].append   (usrdata[4])
                      ms_rssKB[ts].append (int(usrdata[5]/1024))
                      ms_io[ts].append    (usrdata[8])
                   elif username and usrdata[0] == username:
                      ms_cpu[ts].append   (usrdata[4])
                      ms_rssKB[ts].append (int(usrdata[5]/1024))
                      ms_io[ts].append    (usrdata[8])

        ms_cpu_lst   = [[ts, sum(value_lst)] for ts,value_lst in ms_cpu.items()]
        ms_rssKB_lst = [[ts, sum(value_lst)] for ts,value_lst in ms_rssKB.items()]
        ms_io_lst    = [[ts, sum(value_lst)] for ts,value_lst in ms_io.items()]
        return {'name':hostname, 'data':ms_cpu_lst}, {'name':hostname, 'data':ms_rssKB_lst}, {'name':hostname, 'data':ms_io_lst}

class SearchIndex(object):
    def __init__(self, filename, record_len):
        self.idxFile    = open(filename, 'r')
        self.idxFile.seek(0, 2)
        fsize           = self.idxFile.tell()
        self.len        = fsize//record_len
        self.record_len = record_len
        assert (self.len * record_len) == fsize

    def __del__ (self):
      if ( self.idxFile != None ):
         self.idxFile.close()

    def __len__(self):
        return self.len

    def __getitem__(self, item):
        self.idxFile.seek(item * self.record_len)
        return self.idxFile.read(self.record_len)

    # find ts location pos
    def find(self, ts_start):
        lo, hi = 0, self.len
        while lo < hi:
           mid = (lo + hi) // 2
           if int(self[mid][:20]) < ts_start:
              lo = mid + 1
           else:
              hi = mid
        return lo, int(self[mid][:20]) 

def test1():
    print("hello world!")
    f=IndexedHostData('.')
    d={'a':1, 'b':2}
    print(type(d))
    f.writeData('test', 34567, d)

    data=f.readData4('test', 0, 2234567)
    print(data)

if __name__ == "__main__":
    # execute only if run as a script
    hostname, user = sys.argv[1:3]
    hostname=list(hostname.split(','))

    ts       = int(time.time())
    #hostfile = IndexedHostData('/mnt/home/yliu/projects/slurm/utils/mqtMonStreamRecord')
    hostfile = IndexedHostData('/mnt/ceph/users/yliu/mqtMonStreamRecord')
    d        = hostfile.queryDataHosts(hostname, ts-3600, ts, uname=user)
    print ('{}'.format(d))
