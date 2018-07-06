import bisect, _pickle as cPickle, os, sys, time, zlib
from collections import defaultdict as DD
from IndexedDataFile import SearchIndex, IndexedHostData, compTimestamps

# The data gathered to monitor node activity is split by node. Each
# node has two files: a data file to which monitoring data is
# constantly appended, and an index file that contains entries ordered
# by time stamp with associated offsets into the data file.
#
# This code uses the index to quickly locate and retrieve the
# monitoring data for a given node and time interval. The data is
# returned in a format compatible with highcharts' graphing widget.

def getSMData(SMDir, targetNode, start, stop):
    #print ("getSMdata %s"%targetNode)
    sm  = SearchIndex(SMDir+'/%s_sm.px'%targetNode, 40, compTimestamps)
    smd = IndexedHostData(SMDir, targetNode)

    usr2d = DD(list)
    pos = sm.find('%020d'%start)
    #print (pos)
    for x in range(pos, sm.len):
        offset = int(sm[x][20:])
        ts, nd = smd.readData (offset, stop)
        if ( nd == None): break

        print("nd=" + repr(nd))
        for usrdata in nd[3:]: # username, userdata
            usr2d[usrdata[0]].append([ts] + list(usrdata[1:7]))
        
    lseries, mseries = [], []
    for usrname in sorted(usr2d.keys()):
        l, m = [], []
        for e in usr2d[usrname]:
            ts = e[0]*1000
            l.append([ts, e[4]])
            m.append([ts, e[6]])
        lseries.append({'data': l, 'name': usrname})
        mseries.append({'data': m, 'name': usrname})
    
    #[{'name':username, 'data':[[timestamp, value]...]} ...]
    return lseries, mseries

if __name__ == '__main__':
    SMDir, outbn, targetNode = sys.argv[1:4]
    now = time.time()
    if len(sys.argv) == 5:
        start = time.mktime(time.strptime(sys.argv[4], '%Y%m%d'))
        if len(sys.argv) == 6:
            stop = time.mktime(time.strptime(sys.argv[5], '%Y%m%d'))
        else:
            stop = now
    else:
        stop = now
        start = now - 3*24*3600

    lseries, mseries = getSMData(SMDir, targetNode, start, stop)
    open(outbn+'_load.json', 'w').write(repr(lseries).replace('\'', '"'))
    open(outbn+'_mem.json',  'w').write(repr(mseries).replace('\'', '"'))
