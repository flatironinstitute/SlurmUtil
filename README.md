# Slurm Utilites: monitoring tools and web interfaces

This project is to keep monitoring tools and web interfaces for slurm jobs.

## Monitoring
Monitoring is achieved by subscribing to MQTT server (mon5.flatironinstitute.org) using Eclipse Phao python client. 

The incoming messages will be 
1) parsed and indexed; 
2) saved to local files at ${pData}/${hostname}_sm.p (data file) and ${hostname}_sm.px (index file); 
3) updated to web interface (http://${webserver}:8126/updateSlurmData) to refresh the data

## Web interface
Web server is hosted by CherryPy at http://${webserver}:8126/

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

Python virtual environment with packages:
```
cheroot (6.0.0)
CherryPy (14.0.0)
Cython (0.27.3)
dnspython (1.15.0)
more-itertools (4.1.0)
numpy (1.14.2)
paho-mqtt (1.3.1)
pandas (0.22.0)
pip (9.0.1)
portend (2.2)
pyslurm (17.2.0)
python-dateutil (2.7.0)
python-etcd (0.4.5)
pytz (2018.3)
setuptools (38.5.2)
six (1.11.0)
tempora (1.11)
urllib3 (1.22)
wheel (0.30.0)
```

```
MQTT server running on mon5.flatironinstitute.org
```

### Installing

A step by step series of examples that tell you have to get a development env running.

```
Clone the repository to your local machine
```

## Execute

Here is how to start the system on your local machine.

###StartSlurmMqtMonitoring 
Customerize ${CmSlurmRoot}, ${pData}, ${WebPort}, python virtual environment in the script, list the web server update interface in mqt_urls, and run
```
StartSlurmMqtMonitoring
```
It starts web server at http://localhost:${WebPort} and a deamon that shall 1) subscribe to MQTT 2) update the informaton of the web server.

The log files are saved in smcpsun_${cm}_mqt_$(date +%Y%m%d_%T).log and mms_${cm}_$(date +%Y%m%d_%T).log.


