# Flatiron Monitoring and Alarming Utilities

The front-end web interface provides users with real-time and historical monitoring information. 
The interface is powered by a backend server that collects data from various sources, 
including our own monitoring agents, slurm database, Bright server, etc., from both local (rusty) 
and remote (popeye) clusters. 

The server efficiently saves the real-time data into time-series databases and backup files, 
while also offering tools to streamline the administrator's tasks, such as sending alerts 
during system health issues, providing possible solutions, predicting future workload, and more. 
The server is designed for high performance, utilizing multiple processes and memory data 
caches to enhance efficiency.

An instance of the web interface is accessible at http://mon7:8126.

# Getting Started
## Prerequisites 
The following are the listed data sources for our monitoring tool, which should be accessible for use.
### Slurm 
Slurm commands should be able to run on the same node.

### Bright
Birght monitoring interface should be accessible.
The "bright" key can be found in the config/config.json file, which contains the configuration settings for Bright.

### MQTT and cluster_host_mon.py
For data to be reported to the MQTT server, the cluster_host_mon.py host monitoring daemon needs to be installed on all nodes.
The "mqtt" key can be found in the config/config.json file, which contains the configuration settings for MQTT.

### InfluxDB
We save our monitoring data in a time-series database: InfluxDB.

#### Installation
For CentOS,
```
wget https://dl.influxdata.com/influxdb/releases/influxdb-1.8.1.x86_64.rpm
sudo yum install influxdb-1.8.1.x86_64.rpm

service influxdb start
```

#### Configuration
By default, InfluxDB uses the following network ports:
```
    TCP port 8086 is used for client-server communication over InfluxDB’s HTTP API. And,
    TCP port 8088 is used for the RPC service for backup and restore.
```
The configuration file for default installations can be found at /etc/influxdb/influxdb.conf, and it allows for the modification of all port mappings.

The "influxdb" key can be found in the config/config.json file, which contains the configuration settings to connect to InfluxDB.

## Environment setup
### Slurm, Python and gcc
The module can be used to install necessary packages and libraries such as 
```
module add slurm gcc/11.2.0 python/3.10
```

### Python virutal environment:
Create and install packages within a Python virtual environment.
```
cd <dir>
python -m venv --system-site-packages env_slurm22_p310
source ./env_slurm25_p310/bin/activate
pip install -r requirements.txt
```

<!---
[//]: # pip install pystan==2.19.1.1 --no-cache
[//]: # pip install prophet --no-cache
[//]: # Note: The installation of fbprophet may need to pip uninstall numpy; pip install numpy; to solve error of import pandas 
-->

#### Install pyslurm
Inside the python virtual environment, we should install pyslurm.

##### Download source code
```
wget https://github.com/PySlurm/pyslurm/archive/refs/tags/v22.5.1.tar.gz
tar -xzvf v22.5.1.tar.gz
```
You may check release information at https://github.com/PySlurm/pyslurm/releases.

Or,
```
git clone https://github.com/PySlurm/pyslurm.git
```

#### Modify the source code:
Modify pyslurm/pyslurm.pyx to add job attributes 'state_reason' and 'gres_detail'.
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

<!---
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
-->

##### Build and Install pyslurm:
Inside the Python virtual environment
```
cd <pyslurm_source_dir>
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include build
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include install
```

## Run the Slurm Monitoring Tool:

### Installation

Clone the repository to your local machine
```
git clone https://github.com/flatironinstitute/SlurmUtil.git
```

### Configuration

You may configure the monitoring tools using both the shell script "StartSlurmMqtMonitoring_mon7" and the configuration 
file "config/config.json".

### Start the Slurm Monitoring Tool:

Run
```
StartSlurmMqtMonitoring_mon7
```

The script starts programs defined in "cmds" such as
```
declare -a cmds=("python ${ScriptDir}/sm_app.py" "python ${ScriptDir}/mqttMonStream.py" "python ${ScriptDir}/mqttMon2Influx.py" "ssh -i /mnt/home/yliu/.ssh/id_sdsc -N -R 8126:localhost:8126 popeye-login2.sdsc.edu" "python ${ScriptDir}/brightRelay.py")
```

"python ${ScriptDir}/sm_app.py" starts a web server at http://localhost:${port}, where "port" is configured in "config/config.json".
 
"python ${ScriptDir}/mqttMonStream.py" starts a MQTT client that receieves montoring data form a MQTT server, which is configured in "config/config.json".

"python ${ScriptDir}/mqttMon2Influx.py"

web server at http://localhost:${WebPort} and two deamons that 1) both subscribe to MQTT 2) one update the informaton of the web server, one update influxdb (WILL MERGE TWO DEAMONS LATER)

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

