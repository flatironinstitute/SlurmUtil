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
1) http://${webserver}:8126/
A tabular summary of the slurm worker nodes, jobs and users.

2) http://${webserver}:8126/sunburst
A sunburst graph of the slurm partitions, users, jobs and worker nodes.

3) http://${webserver}:8126/usageGraph
A chart showing the file usage of slurm users.

4) http://${webserver}:8126/tymor
A tabular summary of slurm jobs

4) http://${webserver}:8126/tymor2
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
Python virtual environment
Pyslurm
```

### Installing

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Execute

##StartSlurmMqtMonitoring 
Run
```
StartSlurmMqtMonitoring
```
It starts web server at http://localhost:8126 and a deamon and 1) subscribe to mqt 2) update the informaton of the server.


