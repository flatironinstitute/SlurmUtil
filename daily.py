import time
t1=time.time()
from querySlurmDB import CSV_DIR, SlurmDBQuery
from prophet      import MyProphet
from queryInflux  import InfluxQueryClient

if __name__=="__main__":
   SlurmDBQuery.savCPUAlloc       (CSV_DIR +"cpuAllocDF.csv",    years=2)
   SlurmDBQuery.savAccountCPUAlloc(CSV_DIR +"cpuAllocDF_{}.csv", years=2)  # write to cpuAllocDF_cca.csv, ....

   inst = MyProphet()  # default is using 2 years's history
   inst.clusterUsage_forecast (CSV_DIR + "cpuAllocDF.csv")
   inst.accountUsage_forecast (CSV_DIR + "cpuAllocDF_{}.csv")

   # generate pickle file
   app  = InfluxQueryClient()
   app.savNodeHistory      ()  # default is 7 days
   app.savJobRequestHistory()

