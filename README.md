# Slurm Utilites: monitoring tools and web interfaces

This project is to keep monitoring tools and web interfaces for slurm jobs.

## Monitoring
Monitoring is achieved by subscribing to MQTT server (mon5.flatironinstitute.org) using Eclipse Phao python client. 

The incoming messages will be 
1) parsed and indexed; 
2) saved to local files at ${pData}/${hostname}_sm.p (data file) and ${hostname}_sm.px (index file); 
3) updated to web interface (http://${webserver}:8126/updateSlurmData) to refresh the data

Considering performance, we save incoming messages in InfluxDB (measurement slurmdb) and retrieve information from the measurement.

## Web Interface
Web server is hosted by CherryPy at http://${webserver}:8126/. You can see an example of it at http://scclin011:8126/

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

These instructions will get you a copy of the project up and running on your local machine.  See deployment for notes on how to deploy the project on a live system.

### Prerequisites

```
slurm 17.02.2
/etc/slurm/slurm.conf
```

```
Python 3.6
```

Create python virutal environment:
```

#install pyslurm
#download pyslurm, untar,
https://pypi.org/project/pyslurm/18.8.1.1/#history
cd <pyslurm_source_dir>
#modify setup.py
SLURM_DIR = ""
SLURM_LIB = ""
SLURM_INC = ""
python setup.py build

cd <dir>
virtualenv env_slurm18
source ./env_slurm18/bin/activate
pip install Cython
python setup.py install

#install other packages
pip install pandas
pip install cherrypy
pip install pystan
pip install fbprophet
pip install influxdb
pip install paho-mqtt
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

rebuild pyslurm
python setup.py build

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


