#!/usr/bin/env python
import bisect 
import _pickle as pickle
import zlib

class IndexedHostData(object):
    def __init__(self, prefix):
        self.prefix = prefix

    def writeData(self, hostname, ts, d):
        #save information to files
        with open(self.prefix+'/%s_sm.p'%hostname, 'ab') as df, open(self.prefix+'/%s_sm.px'%hostname, 'a') as idx:
            zps = zlib.compress(pickle.dumps(d))
            idx.write('%020d%020d'%(ts, df.tell()))

            df.write('{:0>20}'.format(int(ts)).encode('utf-8'))
            df.write('{:0>20}'.format(len(zps)).encode('utf-8'))
            df.write(zps)
#           p.write('%020d%020d'%(t1, len(zps)))
#           p.write(zps)

    def readData(self, hostname, offset, stopTime):
        with open(self.prefix+'/%s_sm.p'%hostname, 'rb') as df:
            if offset != df.tell(): 
                df.seek(offset, 0)
            ts = int(df.read(20))
            if ts > stopTime:
                return ts, None

            len = int(df.read(20))
            return ts, pickle.loads(zlib.decompress(df.read(len)))

class SearchIndex(object):
    def __init__(self, filename, reclen, compFunc):
        self.compFunc = compFunc
        self.idxFile  = open(filename, 'r')
        self.idxFile.seek(0, 2)
        fsize = self.idxFile.tell()
        self.len = fsize//reclen
        self.reclen = reclen
        assert (self.len * reclen) == fsize

    def __len__(self):
        return self.len

    def __getitem__(self, item):
        self.idxFile.seek(item * self.reclen)
        return self.idxFile.read(self.reclen)

    # 'tricks' bisect_left. yes, somewhat hackish, but arguably a
    # reasonable encapsulation nonetheless.
    def __gt__(self, comp):
        return self.compFunc(self.value, comp)

    def find(self, value):
        #print ("find %s"%value)
        self.value = value
        pos        = bisect.bisect_left(self, self.value)
        return pos

def compTimestamps(q, v):
    return q > v[:20]

if __name__ == "__main__":
    # execute only if run as a script
    print("hello world!")
    f=IndexedHostData('.')
    d={'a':1, 'b':2}
    print(type(d))
    f.writeData('test', 34567, d)

    data=f.readData('test', 0, 2234567)
    print(data)
