#!/usr/bin/env python

import time
import pyslurm
import MyTool
from collections import defaultdict
from datetime    import timedelta

MSG_LOW_UTIL  ="Job has run for {} on node {} with a low resource usage. Average CPU utilization is {:.2f} and MEM utilization is {:.2f}."

# a bulletin board 
class BulletinBoard:
   def __init__ (self):
      self._store     = MessageCache ()
   
   # low util jobs that are evaluated at ts
   def addLowUtilJobNotice (self, ts, jobs):
      #print('--addLowUtilJobNotice {}'.format(jobs.keys()))
      for jid, job in jobs.items():
          msg =  MSG_LOW_UTIL.format(timedelta(seconds=int(time.time()) - int(job['start_time'])), job.get('nodes',''), job.get('job_avg_util',-1), job.get('job_mem_util',-1))
          self._store.append(ts, msg, jid)

   def getLatest (self):
      return self._store.latest()

   def get (self, latestN):
      return self._store.get(latestN)
   
   def setLowUtilJobMsg (job_dict):
      for jid, job in job_dict.items():
          job['low_util_msg'] =  MSG_LOW_UTIL.format(timedelta(seconds=int(time.time()) - int(job['start_time'])), job.get('nodes',''), job.get('job_avg_util',-1), job.get('job_mem_util',-1))
       
   
# save message in in-mem cache with size, flushed to file from time to time 
class MessageCache:
   def __init__ (self, maxSize=2048, flushSize=1024):
      self._queue     = []     # ts, message, idx, reverse_index
      self._reverse   = defaultdict(list)     # hashcode(message): [idx,...]
      self._maxSize   = maxSize
      self._flushSize = flushSize
      self._baseIdx   = 0

   def append (self, ts, msg, id_code):
      idx  = self._baseIdx + len(self._queue)
      self._queue.append({'id':id_code, 'ts':ts, 'msg':msg, 'idx':idx, 'history':self._reverse[id_code]}) # data is saved at self._queue[idx-self._baseIdx]
      self._reverse[id_code].append(idx)
      if len(self._queue) == self._maxSize:
         self.flush ()
     
      return self._queue

   # get latest topN message
   def get (self, topN=1):
      return self._queue[-topN:]

   def latest (self):
      msg = []
      for id_code, lst in self._reverse.items():
          idx = lst[-1] - self._baseIdx
          msg.append (self._queue[idx])
      return msg

   # flush back to size
   def flush(self):
      # write to file
      with open('bb.history', 'w') as f:  #TODO: add ts to filename
         for i in range(self._flushSize):
            f.write(json.dumps(self._queue[i]))
            
      self._baseIdx += self._flushSize
      self._queue   =  self._queue[self._flushSize:]
      #TODO: self._reverse get too big, remove data that is 1) not in memory, 2) job finished and latest data more than 1 month ago
      return self._queue

if __name__ == "__main__":
   ins  = BulletinBoard()
   jobs = pyslurm.job().get()
   jobs = [job for job in jobs.values() if job['job_state']=='RUNNING']
   ins.addLowUtilJobNotice (time.time(), jobs)
   msgs = ins.get(100)
   print('{}'.format(msgs))

         
      
      

        
