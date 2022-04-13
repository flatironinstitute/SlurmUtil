from threading import Lock

class RWLock_2_1:
      #one writer, multiple readers
      #writer preference 

   def __init__(self):
       self.resource = Lock()             # lock the critical section in writer
       self.readCount= 0                  # count number of readers
       self.rmutex   = Lock()             # protect self.readCount
       self.readTry  = Lock()             # writers preference, writer lock it to prevent later readers

   def writer_acquire(self):
       self.readTry.acquire()             # prevent other later readers
       self.resource.acquire()            # protect critical section

   def writer_release(self):
       self.resource.release()
       self.readTry.release()             # allow readers

   def reader_acquire(self):
       self.readTry.acquire()             # try read
       self.rmutex.acquire()              # prectect self.readCount ++
       self.readCount += 1
       if (self.readCount == 1):
          self.resource.acquire()         # first reader prevent writer
       self.rmutex.release()
       self.readTry.release()

   def reader_acquire(self):
       self.rmutex.acquire()              # protect self.readCount --
       self.readCount -= 1
       if (self.readCount == 0):         
          self.resource.release()         # last reader allow writer
       self.rmutex.release()
       
