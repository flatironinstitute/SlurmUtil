#!/usr/bin/env python

import time
t1=time.time()
import json, pwd, sys
from datetime import datetime, timezone, timedelta
import os.path

import influxdb
import collections
import MyTool

class TextfileQueryClient:

    def __init__(self, file_name='host_up_ts.txt'):
        self.ts_fname = file_name
        self.up_ts    = None
        self.mod_time = None

    #TODO: should synchronize file access
    def refresh (self):
        t1 = time.time()

        self.mod_time     = os.path.getmtime(self.ts_fname)
        with open (self.ts_fname, 'r') as f:
             self.up_ts        = json.load(f)     

        print("refresh take time " + str(time.time()-t1))

    # return the timestamp {'node':[], ...} for a list of nodes
    def getNodeUpTS (self, nodes):
        
        if (self.up_ts== None) or (os.path.getmtime(self.ts_fname) > self.mod_time) :
           self.refresh()
 
        result = {n:self.up_ts[n] for n in nodes}
        return result

def main():
    app   = TextfileQueryClient()
    print (repr(app.getNodeUpTS(['worker0000'])))

if __name__=="__main__":
   t1=time.time()
   main()
   print("Total take time " + str(time.time()-t1))
