# Slurm Utilites: monitoring tools and web interfaces

This project is to monitor a slurm cluster and to provide a set of user interfaces to display the monitoring data.

## Monitoring
### Pre-requirement
On each node of the slurm cluster, a cluster_host_mon.py is running and reporting the monitored data (running processes' user, slurm_job_id, cpu, memory, io ...) to a MQTT server.
### Monitor Data
We use Phao Python Client to subscribe to the MQTT server (mon5.flatironinstitute.org) and receive data from it. The incoming data will be 
1) parsed and indexed; 
2) saved to data file (${hostname}_sm.p) and index file (${hostname}_sm.px); 
3) saved to InfluxDB (slurmdb1)
3) updated to web interface (http://${webserver}:8126/updateSlurmData) to refresh the displayed data

We also use PySlurm to retrieve data from slurm server periodically and save the data to InfluxDB.

## Web Interface
Web server is programmed using CherryPy. You can see an example of it at http://mon6:8126/

You can see different user interfaces at
1) http://${webserver}:8126/,
A tabular summary of the slurm worker nodes, jobs and users.

2) http://${webserver}:8126/sunburst,
A sunburst graph of the slurm partitions, users, jobs and worker nodes.

3) http://${webserver}:8126/usageGraph,
A chart showing the file usage of slurm users.

4) http://${webserver}:8126/tymor,
A tabular summary of slurm jobs

4) http://${webserver}:8126/tymor2,
A tabular summary of slurm jobs

You can also see the detailed informaiton and resource usage of a specific worker node or a running job. Some examples are:
http://scclin011:8126/nodeDetails?node=worker1001
http://scclin011:8126/nodeGraph?node=worker1001
http://scclin011:8126/jobDetails?jobid=93585
http://scclin011:8126/jobGraph?jobid=93585

## Getting Started

### Prerequisites and Environment setup
Use module to add needed packages and libraries.
```
module add slurm gcc/10.1.0 python3
[yliu@scclin011 utils]$ module list
Currently Loaded Modulefiles:
 1) slurm/18.08.8   2) gcc/10.1.0   3) python3/3.7.3  
```
Slurm is installed and slurm configuration file is at /etc/slurm/slurm.conf

####install pyslurm
#####download pyslurm, untar,
https://pypi.org/project/pyslurm/18.8.1.1/#history
cd <pyslurm_source_dir>
#####modify setup.py to set slurm directories
SLURM_DIR = ""
SLURM_LIB = ""
SLURM_INC = ""

Set up a python virutal environment:
```
cd <dir>
python3 -m venv env_slurm18_python37
source ./env_slurm18_python37/bin/activate
pip install Cython
cd <pyslurm_dir>
python setup.py build install

#install other packages
pip install cherrypy
pip install paho-mqtt
pip install influxdb
pip install fbprophet
pip install seaborn
```
The installation of fbprophet includes pystan, pandas, matplotlib, ...
May need to pip uninstall numpy; pip install numpy; to solve error of import pandas 

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

modify pyslurm installation:
In pyslurm/pyslurm.pyx, changed line 1901 to:
        self._ShowFlags = slurm.SHOW_DETAIL | slurm.SHOW_DETAIL2 | slurm.SHOW_ALL
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



rebuild pyslurm
python setup.py build
python setup.py install
. env_slurm18/bin/activate
pip install -e ../pyslurm

```
MQTT server running on mon5.flatironinstitute.org
```

```
Influxdb running on localhost
```

### Installing

A step by step series of examples that tell you have to get a development env running.

```
Clone the repository to your local machine
```

Install Influxdb

```
wget https://dl.influxdata.com/influxdb/releases/influxdb-1.5.3.x86_64.rpm
sudo yum install influxdb-1.5.3.x86_64.rpm
service influxdb start
```

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


install fbprophet
pip --use-feature=2020-resolver install python-dev-tools

