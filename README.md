# Flatiron Scientific Computing Monitoring and Alarming Utilities

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
Slurm commands should be executable on the same node.

### Bright
Birght monitoring interface should be accessible.
The "bright" key can be found in the config/config.json file, which contains the configuration settings for Bright.

### MQTT and cluster_host_mon.py
For data to be reported to the MQTT server, the cluster_host_mon.py host monitoring daemon needs to be installed on all nodes.
The "mqtt" key can be found in the config/config.json file, which contains the configuration settings for MQTT.

### InfluxDB
Our monitoring data is stored in a time-series database called InfluxDB, with its configuration settings specified in the config/config.json file under the 'influxdb' key.

#### Installation
For Linux,
```
wget https://dl.influxdata.com/influxdb/releases/influxdb-1.8.1.x86_64.rpm
sudo yum install influxdb-1.8.1.x86_64.rpm
```

#### Configuration
By default, InfluxDB uses the following network ports:
```
    TCP port 8086 is used for client-server communication over InfluxDB’s HTTP API. And,
    TCP port 8088 is used for the RPC service for backup and restore.
```
The configuration file is located at /etc/influxdb/influxdb.conf, and it can be customized 
to specify the port and directories where the data is saved.

#### Start
```
service influxdb start
```

#### Backup
On the InfluxDB server, we can set up a monthly cron job to run the backup command
```
0 2 1 * * /usr/bin/influxd backup -portable /mnt/home/yliu/ceph/influxdb/backup
```

#### Restart
In some situations, you may need to perform a re-installation and re-configuration 
of InfluxDB. Once that is done, you can restore the data using the following command:

```
influxd restore -portable /mnt/home/yliu/ceph/influxdb/backup
```
After the data has been successfully restored, you can then proceed to restart InfluxDB.

##Environment setup
### Required Modules 
The module facilitates the installation of required packages and libraries, including
```
module add slurm gcc/11.2.0 python/3.10
```

### Python Virutal Environment:
Create and install Python packages within a Python virtual environment.
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

#### Pyslurm
Once the Python virtual environment is set up, we install pyslurm within it.

##### Download the source code
```
wget https://github.com/PySlurm/pyslurm/archive/refs/tags/v22.5.1.tar.gz
tar -xzvf v22.5.1.tar.gz
```
Or,
```
git clone https://github.com/PySlurm/pyslurm.git
```
For the latest release information, please visit the following URL: https://github.com/PySlurm/pyslurm/releases.


#### Modify the source code:
Modify pyslurm/pyslurm.pyx to include additional job attributes, namely 'state_reason' and 'gres_detail'.
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
```
cd <pyslurm_source_dir>
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include build
python setup.py --slurm-lib=$SLURM_ROOT/lib64 --slurm-inc=$SLURM_ROOT/include install
```

## Run the Monitoring and Alarming Utilities:

### Installation

Clone the repository to your local machine
```
git clone https://github.com/flatironinstitute/SlurmUtil.git
```

### Configuration
The configuration can be done via both the shell script "StartSlurmMqtMonitoring_mon7" 
and the configuration file "config/config.json"

### Execution

Run
```
StartSlurmMqtMonitoring_mon7
```

The script starts programs defined in "cmds" such as
```
declare -a cmds=("python ${ScriptDir}/sm_app.py" "python ${ScriptDir}/mqttMonStream.py" "python ${ScriptDir}/mqttMon2Influx.py" "ssh -i /mnt/home/yliu/.ssh/id_sdsc -N -R 8126:localhost:8126 popeye-login2.sdsc.edu" "python ${ScriptDir}/brightRelay.py")
```

The command "python ${ScriptDir}/sm_app.py" starts a web server at http://localhost:${port}, where "port" is configured in "config/config.json".
 
The command "python ${ScriptDir}/mqttMonStream.py" launches an MQTT client that receives monitoring data from an MQTT server, sends the data to web servers, and saves it in files. 
The configuration of it can be found under the "mqtt" key in the "config/config.json" configuration file."

The command "python ${ScriptDir}/mqttMon2Influx.py" launches another MQTT client that receives monitoring data from an MQTT server, 
formats the data and sends it to a InfluxDB server. 
The configuration of it can be found under the "influxdb" key in the "config/config.json" configuration file."

The command "python ${ScriptDir}/brightRelay.py" executes a proxy server that queries and caches data from a bright server.

The command "ssh -i /mnt/home/yliu/.ssh/id_sdsc -N -R 8126:localhost:8126 popeye-login2.sdsc.edu" establishes an SSH forwarding channel that enables the receipt of data from a remote cluster (popeye) where an instance of mqttMonStream.py is running.

The script and configuration file can be customized to initiate a subset of the processes mentioned earlier. For example, on popeye, "cmds" is defined as:
```
declare -a cmds=("python ${ScriptDir}/mqttMonStream.py -c config/config_popeye.json" "python ${ScriptDir}/mqttMon2Influx.py -c config/config_
popeye.json")
```

Log files are generated and serve as a valuable resource for troubleshooting in case any issues arise.

### Restart

If you need to restart, make sure to clean up any remaining processes before re-running the same command.
```
. ./StartSlurmMqtMonitoring_mon7
```

# set up cron job
Run daily.sh every day to update data.
```
crontab -e
00 07 * * * . /mnt/home/yliu/projects/slurm/utils/daily.sh >  /mnt/home/yliu/projects/slurm/utils/daily_$(date +%Y-%m-%d).log 2>&1
```
<!---
install fbprophet
pip install pandas
pip install fbprophet
pip --use-feature=2020-resolver install python-dev-tools
--->

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

