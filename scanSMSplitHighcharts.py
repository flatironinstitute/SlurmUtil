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
    #print ("getSMdata %020d"%start)
    sm = SearchIndex(SMDir+'/%s_sm.px'%targetNode, 40, compTimestamps)

    #print (SMDir+'/%s_sm.p'%targetNode)
    #smd = open(SMDir+'/%s_sm.p'%targetNode, 'rb')
    smd = IndexedHostData(SMDir)

    u2d = DD(list)
    pos = sm.find('%020d'%start)
    #print (pos)
    for x in range(pos, sm.len):
        offset = int(sm[x][20:])
        ts, nd = smd.readData (targetNode, offset, stop)
        if ( nd == None): break

        for ud in nd[3:]:
            u2d[ud[0]].append([ts] + list(ud[1:7]))
        
    lseries, mseries = [], []
    for u in sorted(u2d.keys()):
        l, m = [], []
        for e in u2d[u]:
            ts = e[0]*1000
            l.append([ts, e[4]])
            m.append([ts, e[6]])
        lseries.append({'data': l, 'name': u})
        mseries.append({'data': m, 'name': u})
    
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
    open(outbn+'_mem.json', 'w').write(repr(mseries).replace('\'', '"'))
