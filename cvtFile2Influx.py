import _pickle as pickle, os, sys, time, zlib
import influxdb
import glob
import logging
import traceback

# The data gathered to monitor node activity is split by node. Each
# node has two files: a data file to which monitoring data is
# constantly appended, and an index file that contains entries ordered
# by time stamp with associated offsets into the data file.
#

_f_handler=logging.FileHandler("cvtFile2Influx.log")
_f_handler.setLevel(logging.DEBUG)
_f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(_f_handler)

class InfluxDBClientWrapper(object):
    def __init__(self, hostname, dbname):
        self.influx_client = influxdb.InfluxDBClient(hostname, 8086, "yliu", "", dbname)
        _logger.info("influx_client= " + repr(self.influx_client._baseurl))

    def writeInflux (self, points, ret_policy="autogen"):
        if ( len(points) == 0 ):
           return

        try:
           _logger.info("writeInflux " + str(len(points)) + " points")
           #time.sleep(2)
           self.influx_client.write_points (points,  retention_policy=ret_policy, time_precision='s', batch_size=1000)
        except influxdb.exceptions.InfluxDBClientError as err:
           _logger.error("IDBClientWrapper:writeInflux " + ret_policy + ":" + repr(err))
           raise

class HostDataFile(object):
    def __init__(self, datafilename, cvtSize=10000):
        basedir, filename = os.path.split(datafilename)
        self.nodename     = filename.split('.')[0].split('_')[0] # worker0001_sm.p
        self.datafname    = datafilename
        self.offHstfname  = "./offsetHistory/" + self.nodename+".offset"
        self.offMapfname  = "./mqtMonTestFilesMod/" + self.nodename+".offsetMap"
        self.data_file    = open(datafilename, 'rb')
        self.activePids   = []
        self.eof          = False
        self.cvtSize    = cvtSize                            # control how many points to read
        self.savState    = None                                 # used to save last state point

        self.offsetMapping = {}
        self.resetBatchCount ()

    def __del__ (self):
        self.data_file.close()
    
    # count differnt type of points
    def resetBatchCount (self):
        self.count_info  = 0
        self.count_mon   = 0
        self.count_state = 0
        self.count_uid   = 0

    #read ts, len and data at offset, return ts, next_offset and data
    #return '' when reach the limit or end of file
    def readItem(self, offset):
      if (self.count_info+self.count_mon+self.count_uid) > self.cvtSize or self.eof: 
         # reach the limit or eof
         return ''

      offset = self.offsetMapping.get(offset, offset)
      if offset != self.data_file.tell():
         self.data_file.seek(offset, 0)

      tmp  = self.data_file.read(20)
      if not tmp: #end of file
         self.eof = True
         return ''

      len     = 0
      try:
         ts   = int(tmp)
         len  = int(self.data_file.read(20))
         data = pickle.loads(zlib.decompress(self.data_file.read(len)))
      except Exception as e:
         _logger.error("node=" + self.nodename + ",offset=" + repr(offset) + ",len=" + repr(len))
         _logger.error(traceback.format_exc())
         raise

      return ts, offset+20+20+len, data

    # get exectable name
    def getCmdName (self, cmds):
       if (not cmds): return ''

       cmd = cmds[0].split('/')[-1]
       if (cmd != 'bash' and cmd != 'sh'):
          return cmd
       else:
          if ( len(cmds) > 1 ):
             return cmds[1].split('/')[-1]
          else:
             return cmd
         
    # create point for measurement cpu_state if needed
    def create_cpu_state (self, ts, hostname, state):
       if self.savState and self.savState == state:
          return None
       else:  #state is changed, save in database
          point           = {'measurement':'cpu_state', 'time':int(ts)}  # the ts in file is already in s, from mqt is 1523904247.82
          point['tags']   = {'hostname':    hostname}
          point['fields'] = {'slurm_state': state}

          self.savState     = state
          self.count_state += 1
          return point

    # create point for measurement cpu_proc_info
    def create_cpu_proc_info (self, create_time, pid, uid, hostname, cmds):
       point           = {'measurement':'cpu_proc_info', 'time': int(create_time+0.5)}
       point['tags']   = {'pid':pid, 'uid':uid, 'hostname':hostname}
       point['fields'] = {'cmdline': repr(cmds)}

       self.count_info +=1
       return point

    # create point for measurement cpu_proc_mon
    def create_cpu_proc_mon (self, ts, pid, uid, create_time, hostname, rss, vms, system_time, user_time):
       point           = {'measurement':'cpu_proc_mon', 'time':int(ts)}
       #time                cpu_system_time cpu_user_time create_time  hostname   io_read_bytes io_read_count io_write_bytes io_write_count mem_data mem_rss mem_shared mem_text mem_vms   num_fds pid    status   uid
       point['tags']   = {'pid':pid, 'uid':uid, 'hostname':hostname}
       point['fields'] = {'mem_rss':rss, 'mem_vms':vms, 'cpu_system_time':system_time, 'cpu_user_time':user_time}
  
       self.count_mon +=1
       return point

    def create_cpu_uid_mon(self, ts, hostname, uid, infoDict):
       point           = {'measurement':'cpu_uid_mon', 'time':int(ts)}
       point['tags']   = {'hostname':hostname, 'uid': uid}
       point['fields'] = infoDict
       #print("create_cpu_uid_mon " + repr(point))
       self.count_uid +=1
       return point

    # create points based on data
    def createPoints (self, hostname, ts, data):
        #[state, delta, ts, procsByUser]
        points   = []
        if len(data) < 3:
           _logger.error("Format ERROR: data=" + repr(data) + " Ignore")
           return []

        state, delta, ts, procsByUsers = data[0], data[1], data[2], data[3:]
        p = self.create_cpu_state(ts, hostname, state)
        if p: points.append(p)
        if not procsByUsers:
           return points
        if len(procsByUsers) == 1 and type(procsByUsers[0]) is int: #short-lived format mod by Yanbin 
           return points

        # deal with process information procsByUsers
        currPids = []   # record pids to see if new pid
        for procsByUser in procsByUsers:
           #get cpu_proc_info points, will overwrite existed one in the database
           if ( type(procsByUser) is not list ) or (len(procsByUser) != 8 ):
              _logger.error("Format ERROR: procsByUser=" + repr(procsByUser) + " Ignore")
              continue

           user_name, uid, cpus_allocated, procs_count, total_load, total_rss, total_vms, procs = procsByUser
           for pid, intervalUsertimeAvg, create_time, user_time, system_time, rss, vms, cmds in procs:
                   currPids.append        (pid)
                   if pid not in self.activePids:  # assume pid is not reused for the period of time
                      points.append  (self.create_cpu_proc_info(create_time, pid, uid, hostname, cmds))

                   points.append (self.create_cpu_proc_mon (ts, pid, uid, create_time, hostname, rss, vms, system_time, user_time))

        self.activePids = currPids
        return points

    def batch_createPoints (self, next_offset):
        points       = []
        for ts, next_offset, data in iter(lambda: self.readItem(next_offset), ''):
            points.extend(self.createPoints(self.nodename, ts, data))
        return next_offset, points

    # starting from the offset saving in files, create one batch of points
    def cvt2Points (self):
        next_offset = 0

        #read offset from a file 
        if os.path.isfile(self.offHstfname):
           with open(self.offHstfname, "r") as f:
               lines       = f.read().splitlines()
               next_offset = int(lines[-1])

        #read data from offset and write to influx
        _logger.info(self.nodename + ": cvtFile from " + str(next_offset))
        next_offset,points  = self.batch_createPoints(next_offset)
        _logger.info("\tcreate info " + str(self.count_info) + " , mon " + str(self.count_mon) + ", state " + str(self.count_state))
           
        #write next offset back to the file
        with open(self.offHstfname, "a") as f:
            f.write(str(next_offset)+"\n")

        return points

    def readOffsetMapping (self):
        self.offsetMapping = {}
        filename = "./mqtMonTestFilesMod/" + self.nodename + ".offsetMap"
        if os.path.isfile(filename):
          with open(filename) as f:
            lines = f.read().splitlines()
            for line in lines:
                if line:
                   oldOff, newOff = line.split(' ')
                   self.offsetMapping[int(oldOff)] = int(newOff)
          _logger.debug("readOffsetMapping " + self.nodename + "=" + repr(self.offsetMapping))
        
    #readItem from startOffset, if exception, generate offset mapping 
    def checkFormat(self, startOffset):
      offset         = fun1(self.datafname, self.offMapfname, startOffset)
      while not self.eof:
        try:
           ts, offset, data = self.readItem(offset)
        except AttributeError:
           continue
        except ValueError as e:
           offset      = fun_valueError(self.datafname, self.offMapfname, offset)
           self.readOffsetMapping()
        except:
           offset      = fun1(self.datafname, self.offMapfname, offset)
           self.readOffsetMapping()

      removeDup(self.nodename)

def removeDup (nodename):
    fname  = "./mqtMonTestFilesMod/" + nodename + ".offsetMap"
    lst    = []
    with open(fname, 'r') as f:
       for line in f.read().splitlines():
           lst.extend(line.split(' '))
    curr  = 0
    while curr+2 < len(lst):
       if (lst[curr+1] == lst[curr+2]):
          del lst[curr+1:curr+3]
       else:
          curr += 2
    with open(fname, 'w') as f:
       curr = 0
       while curr+1 < len(lst):
          f.write(str(lst[curr]) + ' ' + str(lst[curr+1]) + '\n')
          curr += 2
  
#start from offset, cannot read out int(20) and int(20)      
def fun_valueError(datafilename, offsetfilename, errorOffset):
    with open(datafilename, 'rb') as f:
        f.seek(errorOffset)
        f.seek(-30, 1)
        idx = f.read(100).index(b'0000')
        f.seek(-100+idx,1)
        sav = f.tell() 
        print(repr(f.read(20)))
        print(repr(f.read(20)))
        print('ADD: ' + repr(errorOffset) + ' ' + repr(sav))
    with open(offsetfilename, 'a') as f:
        f.write(repr(errorOffset) + ' ' + repr(sav)+'\n')
        return sav

#start from offset, read ts, offset
def fun1(fname1, fname2, errorOffset):
    with open(fname1, 'rb') as f:
        f.seek(errorOffset)
        ts = int(f.read(20))
        l  = int(f.read(20))
        try:
           f.read(l)
        except OverflowError:
           errorOffset = errorOffset + 1
           f.seek(errorOffset)
           ts = int(f.read(20))
           l  = int(f.read(20))
           f.read(l)
           with open(fname2, 'a') as f:
              f.write(repr(errorOffset-1) + ' ' + repr(errorOffset)+'\n')
           return errorOffset

        idx = 0
        while True:
           try:
              idx = f.read(l).index(b'0000')
              break
           except:
              continue
        f.seek(-l+idx, 1)
        sav=f.tell()
        print(repr(f.read(20)))
        print(repr(f.read(20)))
        print('ADD: ' + repr(errorOffset) + ' ' + repr(sav))
    with open(fname2, 'a') as f:
        f.write(repr(errorOffset) + ' ' + repr(sav)+'\n')

    return sav
        
def cvtSingleFile(datafilename, ifclient):
        _logger.info(datafilename+'\n')    

        dataFile = HostDataFile(datafilename)
        dataFile.readOffsetMapping()  # read offset mapping from files
        while not dataFile.eof:
           dataFile.resetBatchCount()
           points = dataFile.cvt2Points ()  # read batch of points
           ifclient.writeInflux (points)

if __name__ == '__main__':
    #rm ./offsetHistory/*
    #Usage: python cvtFile2Influx.py mon7 mondb /mnt/ceph/users/carriero/tmp/mqtMonTest
    #save last offset in ./offsetHistory/, save offset mapping(due to format error) in ./mqtMonTestFilesMod/
    #logging.basicConfig(filename='./log/cvt2Influx.log',level=_logger.DEBUG, format='%(asctime)s %(levelname)s:%(message)s') # must before InfluxDB
    dbhost, dbname, dataDir = sys.argv[1:4]
    ifclient                = InfluxDBClientWrapper(dbhost, dbname)

    #cvtingleFile('/mnt/ceph/users/carriero/tmp/mqtMonTest/worker1005_sm.p', ifclient)
    _logger.info ('start reading files from {}'.format(dataDir))
    for datafilename in glob.glob(dataDir + "/*.p"):
        cvtSingleFile(datafilename, ifclient)
