# Slurm Utilites: monitoring tools and web interfaces

This project is to monitor a slurm cluster and to provide a set of user interfaces to display the monitoring data.

## Monitoring Framework
On each node of the slurm cluster, a deamon cluster_host_mon.py is running and reporting the monitored data (running processes' user, slurm_job_id, cpu, memory, io ...) to a MQTT server (for example, mon5.flatironinstitute.org).

An InfluxDB server (for example, worker1090.flatironinstitute.org) is set up to store the monitoring data.

A monitoring server (for example, mon7.flatironinstiute.org) is set up to receive, forward and display the monitoring data. We use Phao Python Client to subscribe to the MQTT server and thus receive data from it. We also use PySlurm to retrieve data from slurm server periodically. These incoming data will be 
1) parsed and indexed; 
2) saved to data file (${hostname}_sm.p) and index file (${hostname}_sm.px); 
3) saved to a measurement (for example, slurmdb_2) in InfluxDB
3) sent to the web interface (http://${webserver}:8126/updateSlurmData) to display

## Web Interface
Web server is built using CherryPy. You can see an example of it at http://mon7:8126/. The set of user interfaces includes:

1) http://${webserver}:8126/,
A tabular summary of the slurm worker nodes, jobs and users.

2) http://${webserver}:8126/utilHeatmap,
A heatmap graph of worker nodes' and gpus' utilization.

3) http://${webserver}:8126/pending,
A table of pending jobs and related information.

4) http://${webserver}:8126/sunburst,
A sunburst graph of the slurm accounts, users, jobs and worker nodes.

5) http://${webserver}:8126/usageGraph,
A chart of the file and byte usage of users.

6) http://${webserver}:8126/tymor,
A tabular summary of slurm jobs' load statistics.

7) http://${webserver}:8126/bulletinboard,
A set of tables including running jobs and allocated nodes with low resource utilization, errors reported from different components of the system, and etc.

8) http://${webserver}:8126/report,
Generate reports of the cluster resource usage.

9) http://${webserver}:8126/search,
Search the slurm entities' information.

10) http://${webserver}:8126/settings,
Set the settings to control the display of interfaces.

11) http://${webserver}:8126/forecast,
Forecast the cluster usage in the future.

Through the links embeded in these user inferfaces, you can also see the detailed informaiton and resource usage of a specific worker node, job, user, partition and so on. 

## Shortcut for a test run on a Simons Foundation machine
You can have a test run using the existed python enironment and influxdb server.

### Download the repository from github
```
git clone https://github.com/flatironinstitute/SlurmUtil.git
```

### Start the web server on your node
Check the configuration file at SlurmUtil/config/config.json. Make sure "writeFile" is set to false. 
```
   "fileStorage": {
       "dir":       "/mnt/ceph/users/yliu/mqtMonStreamRecord",
       "writeFile": false
   },
```

```
module add slurm gcc/11.2.0 python3
cd SlurmUtil
./StartSlurmMqtMonitoring_1
```  

# Getting Started
## Prerequisites 
The data sources of our monitoring tool are listed as the following and thus should be available to use.
### Slurm 
Slurm configuration file can be accessed at /etc/slurm/slurm.conf.

### Bright
Birght certificates are stored under ./prometheus.cm/. Bright configuration is in config/config.json under key "bright".

### MQTT and cluster_host_mon.py
MQTT configuration is in config/config.json under key "mqtt".
The host monitoring deamon (cluster_host_mon.py) should be installed on the nodes and report data to MQTT server.

### Install InfluxDB
We save monitoring data in a time-series database: InfluxDB.
For CentOS,
```
wget https://dl.influxdata.com/influxdb/releases/influxdb-1.8.1.x86_64.rpm
sudo yum install influxdb-1.8.1.x86_64.rpm

service influxdb start
```
By default, InfluxDB uses the following network ports:
    TCP port 8086 is used for client-server communication over InfluxDB’s HTTP API
    TCP port 8088 is used for the RPC service for backup and restore
All port mappings can be modified through the configuration file, which is located at /etc/influxdb/influxdb.conf for default installations.

## Environment setup
### Python and etc
You can use module to add needed packages and libraries in SF environment.
```
module add slurm gcc/11.2.0 python/3.10
```

### Create and activate a python virutal environment:
```
cd <dir>
python -m venv --system-site-packages env_slurm25_p310
source ./env_slurm25_p310/bin/activate
```

#### Install python packages
Inside the virutal environment
```
pip install -r requirements.txt
```

[]: # pip install pystan==2.19.1.1 --no-cache
[]: pip install prophet --no-cache
[]: Note: The installation of fbprophet may need to pip uninstall numpy; pip install numpy; to solve error of import pandas 

### Install pyslurm
#### Download pyslurm source
Check release information at https://github.com/PySlurm/pyslurm/releases.
```
wget https://github.com/PySlurm/pyslurm/archive/refs/tags/v22.5.1.tar.gz
tar -xzvf v22.5.1.tar.gz
```

Or,
```
git clone https://github.com/PySlurm/pyslurm.git
```

#### Modify pyslurm source (20.11.8):
Modify pyslurm/pyslurm.pyx
```
2139             #Yanbin: add state_reason_desc
2140             if self._record.state_desc:
2141                 Job_dict['state_reason'] = self._record.state_desc.decode("UTF-8").replace(" ", "_")
2142             Job_dict['state_reason'] = slurm.stringOrNone(
2143                     slurm.slurm_job_reason_string(
2144                         <slurm.job_state_reason>self._record.state_reason
2145                     ), ''
2146                 )

2192             #Yanbin: add gres_detail
2193             gres_detail = []
2194             for x in range(min(self._record.num_nodes, self._record.gres_detail_cnt)):
2195                 gres_detail.append(slurm.stringOrNone(self._record.gres_detail_str[x],''))
2196             Job_dict[u'gres_detail'] = gres_detail
```

[]: # #### Modify setup.py ####
[]: # Make pyslurm work with slurm v21.08
[]: # ```
[]: # ...
[]: # SLURM_VERSION = "21.08"
[]: # ...
[]: #         try:
[]: #             slurm_inc_ver = self.read_inc_version(
[]: #                 "{0}/slurm/slurm_version.h".format(self.slurm_inc)
[]: #             )
[]: #         except IOError:
[]: #             slurm_inc_ver = self.read_inc_version("{0}/slurm_version.h".format(self.slurm_inc))
[]: # ...
[]: # ```


#### Build and Install pyslurm:
Inside the python virtual environment
```
cd <pyslurm_source_dir>
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include build
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include install
```

```
MQTT server running on mon7.flatironinstitute.org
```

```
Influxdb running on influxdb000
```

## Others:
### Installing

A step by step series of examples that tell you have to get a development env running.

Clone the repository to your local machine
```
git clone https://github.com/flatironinstitute/SlurmUtil.git
```
(git pull to retrieve the update)

## Execute

Here is how to start the system on your local machine.

### StartSlurmMqtMonitoring 

Customerize ${CmSlurmRoot}, ${pData}, ${WebPort}, python virtual environment in the script, list the web server update interface in mqt_urls, and run
```
StartSlurmMqtMonitoring
```
It starts web server at http://localhost:${WebPort} and two deamons that 1) both subscribe to MQTT 2) one update the informaton of the web server, one update influxdb (WILL MERGE TWO DEAMONS LATER)

The script starts 3 python processes, such as 
```
python3 /mnt/home/yliu/projects/slurm/utils/smcpgraph-html-sun.py 8126 /mnt/ceph/users/yliu/tmp/mqtMonTest
python3 /mnt/home/yliu/projects/slurm/utils/mqtMon2Influx.py
python3 /mnt/home/yliu/projects/slurm/utils/mqtMonStream.py /mnt/ceph/users/yliu/tmp/mqtMonTest mqt_urls
```
## Debug and Restart

Check log files for errors. Log files are saved in smcpsun_${cm}_mqt_$(date +%Y%m%d_%T).log, mms_${cm}_$(date +%Y%m%d_%T).log and ifx_${cm}_$(date +%Y%m%d_%T).log.

If missed, the python process will be automatically restarted every 60 seconds.

In case, you need to restart
```
sudo service influxdb start
cd /mnt/home/yliu/projects/slurm/utils
. ./StartSlurmMqtMonitoring
```

# set up cron job
Run daily.sh every day to update data.
```
crontab -e
00 07 * * * . /mnt/home/yliu/projects/slurm/utils/daily.sh >  /mnt/home/yliu/projects/slurm/utils/daily_$(date +%Y-%m-%d).log 2>&1
```
install fbprophet
pip install pandas
pip install fbprophet
pip --use-feature=2020-resolver install python-dev-tools

