=============
Cluster usage
=============

The HumanBase pipeline is designed to be run on the high-performance computing (HPC) cluster available at the Flatiron
Institute, where it was developed. In principle, it is possible to run the pipeline in some other cluster environment
or on a single workstation, but this (especially the latter) is not advised. The pipeline generates about ~10 Tb of
data, downloads hundreds of thousands of files, and takes days to run to completion in an HPC setting. This section of
the manual will cover what you need to know to make it run specifically on the Flatiron HPC cluster, which uses Slurm
as its management tool.

This guide will assume that you have already set up both PostgreSQL and Redis to run somewhere accessible by the
project. Typically, if you are running this on a workstation, that is a good place to run your Redis server, but
you can run it anywhere that can be reached from within the cluster network.

-------------------
Acquiring resources
-------------------

The first thing we'll want to is to get some nodes. The node configuration varies on a per-task basis, which means that
as a practical matter you will likely not be able to just fire-and-forget the pipeline and come back in a week to find
that it's done all of its work. The parameters that are appropriate to each task are discussed in the documentation of
the specific tasks, but most of the time you'll want to grab about 8 or 10 Rome nodes, each of which has 1 Tb of RAM
and 128 cores. You can do this by running the following sequence of commands in a bash shell on the cluster:

.. code-block:: bash

    > module load slurm
    > salloc -N 8 -C rome -t 01-00:00:00 -p ccb

The first line here loads the slurm module on the cluster and makes the **salloc** command available, and the
second line requests the allocation. Once the allocation is successful, you will be granted 8 Rome nodes on the CCB
partition for a duration of 1 day. You can increase the duration to up to 7 days; there are maximum usage limits which
are detailed in the cluster wiki.

How do you know what resources you need? Roughly speaking the biggest bottleneck to throughput is memory usage.
Many of the tasks perform intensive computations that produce intermediate objects which consume memory. Effort has
been taken to minimize memory usage but nevertheless, the matrices involved in the computations are quite large
(20,000 by 20,000 genes, times 8 bytes per floating point value), so they will consume a large amount of RAM. If you
exceed the maximum RAM consumption allowed to you on a given node, the out-of-memory (OOM) manager will simply kill
your process and you will be left with a failed task run. Thus, for whatever task you are running, you will want to
be sure that you will not, at any point, exceed the maximum allowed memory usage of the node. Through calculation and
trial and error, reasonable defaults have been configured for the various tasks that use the cluster, but if you are
introducing new tasks, you will likely need to experiment to find out which parameters are optimal for your application.

---------------
Setting up Dask
---------------

Any of the tasks that require **dask** will need to have the Dask scheduler set up. The scheduler can be started by
entering the directory where the project is installed and loading the virtual environment, and then simply running

.. code-block:: bash

    > dask-scheduler

on the command line. This should start up the scheduler and give you some information about the address of the scheduler
and what port it's listening on. Leave the scheduler running (whether in a tmux session or in a terminal if you're
working on the cluster workstation directly) and open a second window or tmux screen. You will now want to do the
following:

.. code-block:: bash

    > export PYTHONPATH=/absolute/path/to/your/pipeline/root:$PYTHONPATH
    > srun dask-worker tcp://<ip-of-scheduler> --nthreads <N> --nprocs <P> --nanny --name humanbase-worker --death-timeout 60 --protocol tcp://

Angle-bracketted terms indicate parameters that will need to be set by the user. The first line here is one that you
only need to run once; it sets the ``PYTHONPATH`` environment (assuming you are using bash; if not, modify for your
shell) to the root of the project. This is needed because otherwise the workers will not know how to import the
modules they need to actually do the work and you will get errors.

The **srun** command will run the **dask-worker** on every node that has been allocated to you. Here, the **nthreads**
and **nprocs** options indicate how many worker processes should run on each node, and how many threads each of those
processes can use. Thus, for the Rome nodes acquired above, a configuration where *N* = 16 and *P* = 8 means that 8
processes will use 16 threads each, for a total of 128 cores, which observant readers will note is equal to the number
of cores on each node. If your allocation is different, you will need to adjust your arguments to **dask-worker**
accordingly.

You should also modify the ``hb.ini`` config file to set the address of the Dask scheduler. Now any tasks which are
distributed to Dask workers should run normally. You can use the Dask dashboard to monitor the progress of these tasks.

.. warning::

    The address of the dask scheduler must be listed in absolute terms as the IP of the workstation or node
    which is running the scheduler process. That means that you cannot give ``localhost`` as the address, since
    this will direct the workers to look for the scheduler on the nodes on which they themselves are running,
    and not on the node where it's actually running.


----------------------------
Alternatives to manual setup
----------------------------

If you don't want to go through the hassle of setting up the cluster, you can use the ``set_up_cluster`` function
from ``src.config.scheduler`` with the appropriate parameters. If you choose to go this route, make sure you do not
have a scheduler process already running.


-----------------
Setting up Celery
-----------------

In addition to using Dask, the pipeline also uses the Celery distributed task manager for some tasks. This is more
appropriate for certain tasks than Dask, and in fact, all of the Dask usage in this project can be replaced with
Celery usage for the most part. This has not been done because the need for Celery over Dask became obvious halfway
through the project when a good deal of code that was already Dask-dependent had been written. Therefore, the project
uses both frameworks, but it would in general be a good idea to work on replacing Dask, which is better suited for
complex computational graphs than it is to situations where you have to do the same operation on a large number of
datasets.

Celery is configured to use Redis as a broker, so if Redis is set up, you can just fire up the workers as follows:

.. code-block:: bash

    > export PYTHONPATH=/absolute/path/to/your/pipeline/root:$PYTHONPATH
    > srun --propagate=NONE celery -A src.config.celery:app worker --concurrency=1

As before, we set up the ``PYTHONPATH`` environment variable so that workers will be able to import the modules.
We then use **srun** to run a celery worker process on each node. The **-A** argument indicates the import path of the
Celery app object which is defined in the code, and the **--concurrency** argument controls how many workers should
run on each node. Unlike with Dask, you cannot control explicitly the number of threads used by the workers.

+++++++++++++++++++++++++
Celery worker concurrency
+++++++++++++++++++++++++

The ideal concurrency of the celery worker is different for different tasks. As mentioned above, the goal is to
maximize throughput while not exceeding the allowed memory limits; this means that we want to have as many workers as
possible, but not so many that their individual tasks cause the whole node to run out of memory. By default, Celery
will create workers with the same concurrency as the number of cores. Thus, on the Rome nodes described above,
starting the workers without the **--concurrency** argument would result in up to 128 workers per node being available
to run tasks. There are times when this is what you want, and times when it is not. Here are the configurations that
I have found work for the tasks that rely on Celery:

.. table::

    ========================== ===========
    Task name                  concurrency
    ========================== ===========
    compute_mutual_information 128
    align_with_gene_list       32
    compute_aligned_posteriors 1
    learn_svm_weights          24

These paramters only make sense for the Rome nodes; if your allocation is different, you will have to find a different
concurrency that works for your case. If tasks are being killed for running out of memory, reduce the concurrency.
