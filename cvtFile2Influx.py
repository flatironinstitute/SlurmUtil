import bisect, _pickle as pickle, os, sys, time, zlib
from collections import defaultdict as DD
from IndexedDataFile import SearchIndex, IndexedHostData, compTimestamps
import influxdb
import glob

# The data gathered to monitor node activity is split by node. Each
# node has two files: a data file to which monitoring data is
# constantly appended, and an index file that contains entries ordered
# by time stamp with associated offsets into the data file.
#

class InfluxDBClientWrapper(object):
    def __init__(self, hostname="scclin011.flatironinstitute.org", dbname="slurmdb"):
        self.influx_client = influxdb.InfluxDBClient(hostname, 8086, "yliu", "", dbname)
        print("influx_client= " + repr(self.influx_client._baseurl))

    def writeInflux (self, points, ret_policy="autogen"):
        if ( len(points) == 0 ):
           return

        try:
           print("writeInflux " + str(len(points)) + " points")
           #self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision='ms')
        except influxdb.exceptions.InfluxDBClientError as err:
           print("IDBClientWrapper:writeInflux " + ret_policy + ":" + repr(err))

class HostDataFile(object):
    def __init__(self, datafilename, batchSize=5120):
        self.data_file = open(datafilename, 'rb')
        self.activePids= []
        self.eof       = False
        self.batchSize = batchSize
        self.basedir, self.filename  = os.path.split(datafilename)

        self.resetBatchCount ()

    def __del__ (self):
        self.data_file.close()
    
    def resetBatchCount (self):
        self.count      = self.batchSize
        self.count_info = 0
        self.count_mon  = 0
        self.count_uid  = 0

    def readItem(self, offset):
      if self.count == 0:
         return ''

      if offset != self.data_file.tell():
         self.data_file.seek(offset, 0)

      tmp  = self.data_file.read(20)
      if not tmp: #end of file
         self.eof = True
         return ''

      ts   = int(tmp)
      leng = int(self.data_file.read(20))
      data = pickle.loads(zlib.decompress(self.data_file.read(leng)))

      if self.usefulData(data):      
         #print ("readItem " + str(self.count) + ':' + str(offset) )
         self.count -= 1

      return ts, offset+20+20+leng, data

    def usefulData (self, data):
        if len(data) <= 3:
           return False

        if len(data) == 4:
           if type(data[-1]) is int:
              return False

        return True

    def getCmdName (self, cmds):
       if (not cmds):
          return ''

       cmd = cmds[0].split('/')[-1]
       if (cmd != 'bash' and cmd != 'sh'):
          return cmd
       else:
          if ( len(cmds) > 1 ):
             return cmds[1].split('/')[-1]
          else:
             return cmd
         
    def create_cpu_proc_info (self, create_time, pid, uid, hostname, cmds):
       infopoint = {'measurement':'cpu_proc_info'}
       infopoint['time']   = int(create_time * 1000)    # such as 1523904247.82
       infopoint['tags']   = {}
       infopoint['tags']['pid']      = pid
       infopoint['tags']['uid']      = uid
       infopoint['tags']['hostname'] = hostname

       infopoint['fields'] = {}
       infopoint['fields']['cmdline']= repr(cmds)
       infopoint['fields']['name']   = self.getCmdName(cmds)

       self.count_info +=1
       return infopoint

    def create_cpu_proc_mon (self, ts, pid, uid, create_time, hostname, rss, vms, system_time, user_time):
       point = {'measurement':'cpu_proc_mon'}
       #time                cpu_system_time cpu_user_time create_time  hostname   io_read_bytes io_read_count io_write_bytes io_write_count mem_data mem_rss mem_shared mem_text mem_vms   num_fds pid    status   uid
       point['time']   = int(ts * 1000)
       point['tags']   = {}
       point['tags']['pid']         = pid
       point['tags']['uid']         = uid
       point['tags']['create_time'] = create_time
       point['tags']['hostname']    = hostname

       point['fields'] = {}
       point['fields']['mem_rss']   = rss
       point['fields']['mem_vms']   = vms
       point['fields']['cpu_system_time'] = system_time
       point['fields']['cpu_user_time']   = user_time
  
       self.count_mon +=1
       return point

    def create_cpu_uid_mon(self, ts, hostname, uid, infoDict):
       point = {'measurement':'cpu_uid_mon'}
       point['time']   =int(ts * 1000)
       point['tags']={'hostname':hostname, 'uid': uid}

       point['fields'] =infoDict
       #print("create_cpu_uid_mon " + repr(point))
       self.count_uid +=1
       return point

    def createPoints (self, hostname, ts, data):
        #[state, delta, ts, procsByUser]
        #print( "hostproc=" + repr(msg))

        state, delta, ts, procsByUsers = data[0], data[1], data[2], data[3:]
        if not procsByUsers:
           return []

        points   = []
        currPids = []
        userDict = {} #uid: {total_mem, total_io, total_num_fds, total_cpu}
        for procsByUser in procsByUsers:
           #get cpu_proc_info points, will overwrite existed one in the database
           if ( type(procsByUser) is not list ) or (len(procsByUser) != 8 ):
              print("Format ERROR: procsByUser=" + repr(procsByUser) + " Ignore")
              continue

           user_name, uid, cpus_allocated, procs_count, total_load, total_rss, total_vms, procs = procsByUser
           for pid, intervalUsertimeAvg, create_time, user_time, system_time, rss, vms, cmds in procs:
                   currPids.append        (pid)
                   if pid not in self.activePids:  # assume pid is not reused for the period of time
                      points.append  (self.create_cpu_proc_info(create_time, pid, uid, hostname, cmds))

                   points.append (self.create_cpu_proc_mon (ts, pid, uid, create_time, hostname, rss, vms, system_time, user_time))

                   if uid not in userDict:
                      userDict[uid] = {'mem_rss':0, 'mem_vms':0, 'cpu_system_time':0,'cpu_user_time':0,'io_read_bytes':0,'io_write_bytes':0}
                   userDict[uid]['mem_rss']         += rss
                   userDict[uid]['mem_vms']         += vms
                   userDict[uid]['cpu_system_time'] += system_time
                   userDict[uid]['cpu_user_time']   += user_time

        for uid, infoDict in userDict.items():
            points.append(self.create_cpu_uid_mon(ts, hostname, uid, infoDict))
        self.activePids = currPids

        return points

    def batch_createPoints (self, next_offset):
        points       = []
        for ts, next_offset, data in iter(lambda: dataFile.readItem(next_offset), ''):
            if self.usefulData(data) :
               #print("\t" + str(ts)+":"+str(next_offset)+":"+repr(data))
               points.extend (self.createPoints(node, ts, data))

        return next_offset, points

    def cvt2Points (self):
        #read offset from a file 
        next_offset = 0
        offsetFile  = "offsetHistory/" + self.filename+"_nextOffset"
        if os.path.isfile(offsetFile):
           with open(offsetFile, "r") as f:
               next_offset = int(f.read())

        while not self.eof:
           #read data from offset and write to influx
           print(self.filename + ": cvtFile from " + str(next_offset))
           next_offset,points  = dataFile.batch_createPoints(next_offset)
           print("\tcreate info " + str(self.count_info) + " , mon " + str(self.count_mon) + ", uid " + str(self.count_uid))
           
           self.resetBatchCount     ()
           with open(offsetFile, "a") as f:
               f.write(str(next_offset)+"\n")

        return points

    def cvtFile (self, ifclient, dataDir, nodeName, next_offset=0):
        #read offset from a file 
        if os.path.isfile(nodeName+"_nextOffset"):
           with open(nodeName+"_nextOffset", "r") as f:
               next_offset = int(f.read())

        while not self.eof:
           #read data from offset and write to influx
           print(nodeName + ": cvtFile from " + str(next_offset))
           next_offset,points  = dataFile.batch_createPoints(next_offset)
           print("\tcreate info " + str(self.count_info) + " , mon " + str(self.count_mon) + ", uid " + str(self.count_uid))
           ifclient.writeInflux (points)
           

           self.resetBatchCount     ()

           #write next offset to a file
           #with open(nodeName+"_nextOffset", "w") as f:
           #    f.write(str(next_offset))
           with open(nodeName+"_offsetHistory", "a") as f:
               f.write(str(next_offset)+"\n")


if __name__ == '__main__':
    #rm worker0000*
    #. ~yliu/anaconda3/lib/python3.6/site-packages/pyslurmenv/bin/activate
    #Usage: python cvtFile2Influx.py dbhost dbname worker0000 /mnt/ceph/users/carriero/tmp/mqtMonTest
    dbhost, dbname, node, dataDir = sys.argv[1:5]

    ifclient     = InfluxDBClientWrapper(dbhost, dbname)

    for datafilename in glob.glob(dataDir + "/*.p"):
        print(datafilename)    
        dataFile     = HostDataFile(datafilename)
        print(dataFile.filename)
        dataFile.cvt2Points ()
     

