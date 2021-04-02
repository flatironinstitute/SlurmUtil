# Slurm Utilites: monitoring tools and web interfaces

This project is to monitor a slurm cluster and to provide a set of user interfaces to display the monitoring data.

## Monitoring
On each node of the slurm cluster, a deamon cluster_host_mon.py is running and reporting the monitored data (running processes' user, slurm_job_id, cpu, memory, io ...) to a MQTT server (for example, mon5.flatironinstitute.org).

A InfluxDB server (for example, worker1090.flatironinstitute.org) is set up to save data.

In the monitoring server, we use Phao Python Client to subscribe to the MQTT server and receive data from it. We also use PySlurm to retrieve data from slurm server periodically. Theese incoming data will be 
1) parsed and indexed; 
2) saved to data file (${hostname}_sm.p) and index file (${hostname}_sm.px); 
3) saved to a measurement (for example, slurmdb_1) in InfluxDB
3) sent to the web interface (http://${webserver}:8126/updateSlurmData) to refresh the displayed data

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
module add slurm gcc/10.1.0 python3
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
We save history data in InfluxDB.
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

### Python and etc
You can use module to add needed packages and libraries in SF environment.
```
module add slurm gcc/10.1.0 python3
Currently Loaded Modulefiles:
 1) slurm/20.02.5   2) gcc/10.1.0   3) python3/3.7.3  
```

## Set up a python virutal environment:
```
cd <dir>
python3 -m venv env_slurm20_python37
source ./env_slurm20_python37/bin/activate
```

#### Install pyslurm
Download pyslurm 18.08.source from https://pypi.org/project/pyslurm/#history. Untar the downloaded zip file into <pyslurm_source_dir>.

Modify pyslurm source (18.08.0):
In pyslurm/pyslurm.pyx, changed line 1884 to:
```
        self._ShowFlags = slurm.SHOW_DETAIL | slurm.SHOW_DETAIL2 | slurm.SHOW_ALL
```
Other changes:
```
2320a2321,2326
>             gres_detail = []
>             for x in range(min(self._record.num_nodes, self._record.gres_detail_cnt)):
>                 gres_detail.append(slurm.stringOrNone(self._record.gres_detail_str[x],''))
>                                    
>             Job_dict[u'gres_detail'] = gres_detail
> 
5365a5372
>                 JOBS_info[u'state_str'] = slurm.slurm_job_state_string(job.state)
modify pyslurm to add state_reason_desc 02/27/2020
2274                 Job_dict[u'state_reason_desc'] = self._record.state_desc.decode("UTF-8").replace(" ", "_")
2199                 Job_dict[u'pack_job_id_set'] = slurm.stringOrNone(self._record.pack_job_id_set, '')
```

Build and Install pyslurm:
```
pip install Cython
cd <pyslurm_source_dir>
python setup.py build --slurm=/cm/shared/apps/slurm/curr
python setup.py install
```
Note: need to  
module rm python3
source env_slurm20_python37/bin/activate
python setup.py install
Note: setup.py change slurm version

#### Install fbprophet
```
pip install -I pystan==2.18 --no-cache
pip install plotly
pip install fbprophet --no-cache
```
Note: The installation of fbprophet need to pip install -I pystan==2.18 first.
The installation of fbprophet may need to pip uninstall numpy; pip install numpy; to solve error of import pandas 

#install other packages
```
pip install cherrypy
pip install paho-mqtt
pip install influxdb
pip install python-ldap
pip install seaborn
```

Python virtual environment with packages:
```
Package                       Version   
----------------------------- ----------
backports.functools-lru-cache 1.5       
certifi                       2018.11.29
chardet                       3.0.4     
cheroot                       6.5.2     
CherryPy                      18.1.0    
cycler                        0.10.0    
Cython                        0.29.2    
fbprophet                     0.3.post2 
idna                          2.8       
influxdb                      5.2.1     
jaraco.functools              1.20      
kiwisolver                    1.0.1     
matplotlib                    3.0.2     
more-itertools                4.3.0     
numpy                         1.15.4    
paho-mqtt                     1.4.0     
pandas                        0.23.4    
pip                           18.1      
portend                       2.3       
pyparsing                     2.3.0     
pyslurm                       17.11.0.14
pystan                        2.18.0.0  
python-dateutil               2.7.5     
pytz                          2018.7    
requests                      2.21.0    
setuptools                    40.6.3    
six                           1.12.0    
tempora                       1.14      
urllib3                       1.24.1    
wheel                         0.32.3    
zc.lockfile                   1.4       
```


## Others:
### Rebuild pyslurm
```
python setup.py build
python setup.py install
. env_slurm18/bin/activate
pip install -e ../pyslurm
```

```
MQTT server running on mon5.flatironinstitute.org
```

```
Influxdb running on localhost
```

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

