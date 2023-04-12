import argparse, time
t1=time.time()
from querySlurmDB import CSV_DIR, SlurmDBQuery
from myProphet      import daily
from queryInflux  import InfluxQueryClient

#Also in MyTool
#Not import MyTool to avoid collpase
def getTS_strftime (ts, fmt='%Y/%m/%d'):
    d = datetime.fromtimestamp(ts)
    return d.strftime(fmt)

if __name__=="__main__":
   parser = argparse.ArgumentParser(description='Collect CPU and other resources (mem, io) utilization data')
   parser.add_argument('-y', '--years',    type=int, default=3,  help='years of history used to make day prediction')
   parser.add_argument('-w', '--weeks',    type=int, default=52, help='weeks of history used to make hour prediction')
   parser.add_argument('-c', '--clusters', nargs="+", default=['slurm'],   help='clusters')
   parser.add_argument('-f', '--use_file', default=False, action='store_true', help='use data from saved file')

   args = parser.parse_args()
   print(args)

   # sav old files by day
   clusters = args.clusters + ['slurm_plus']
   if not args.use_file: 
       #slurm_plus = slurm + slurm_cluster_mod (before slurm part)
       print("--- plusFiles ---")
       SlurmDBQuery.plusFiles()

       for cluster in clusters:
          print("--- Cluseter {} cpuAllocDF ---".format(cluster))
          for day_or_hour in ['day','hour']:
              fname1 = "{}/{}_{}_{}".format(CSV_DIR, cluster, day_or_hour, "cpuAllocDF.csv")
              fname2 = "{}/{}_{}_{}".format(CSV_DIR, cluster, day_or_hour, "cpuAllocDF_{}.csv")
              start,end,df = SlurmDBQuery.savCPUAlloc (cluster, day_or_hour, fname1)  # cannot put into prophet.py 
              print("\tAll: {}-{}".format(start, end)) 
              SlurmDBQuery.savAccountCPUAlloc(cluster, day_or_hour, fname2, df)  

       # generate pickle file
       print("--- Query influx")
       InfluxQueryClient.daily()

   # generate summary usage file
   #SlurmDBQuery.sum_assoc_usage_day('slurm_plus')
   SlurmDBQuery.sum_job_step       ('slurm')

   # generate new prophet and figure
   myProphet.daily()
