import argparse, time
t1=time.time()
from querySlurmDB import CSV_DIR, SlurmDBQuery
from prophet      import MyProphet
from queryInflux  import InfluxQueryClient

if __name__=="__main__":
   parser = argparse.ArgumentParser(description='Collect CPU and other resources (mem, io) utilization data')
   parser.add_argument('-y', '--years',    type=int, default=2, help='years of history used to make prediction')
   parser.add_argument('-c', '--clusters', nargs="+", default=['slurm'],   help='clusters')
   parser.add_argument('-f', '--use_file', default=False, action='store_true', help='use data from saved file')

   args = parser.parse_args()
   print(args)

   for cluster in args.clusters:
       fname1 = "{}/{}_{}".format(CSV_DIR, cluster, "cpuAllocDF.csv")
       fname2 = "{}/{}_{}".format(CSV_DIR, cluster, "cpuAllocDF_*.csv")
       if not args.use_file:
          start,stop,df = SlurmDBQuery.savCPUAlloc       (cluster, fname1, args.years)  # cannot put into prophet.py 
          start1,stop1  = SlurmDBQuery.savAccountCPUAlloc(cluster, fname2, args.years, df)  # cannot put into prophet.py 
          days        = int((stop-start)/3600/24)
          print("{}-{}={} day".format(stop, start, days)) 

       inst = MyProphet(years=2)                      # use up to 2 years of hisotry
       inst.clusterUsage_forecast (fname1, days=14)   # predict 2 weeks
       inst.accountUsage_forecast (fname2, days=14)

       # generate pickle file
       app  = InfluxQueryClient()
       app.savNodeHistory      ()  # default is 7 days
       app.savJobRequestHistory()
