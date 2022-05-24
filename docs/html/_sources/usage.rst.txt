========
Usage
========

The HumanBase pipeline is intended as a stand-alone project, and therefore it is not recommended that you import
any of its modules from some other project. Doing so will likely result in some other outcome than the one you
probably desire. Instead, the pipeline is designed to be run from the command-line using the ``yenta`` task-runner.
You can read more about ``yenta`` at its `project documentation <https://yenta.readthedocs.io/en/latest/>`_
page, but most likely you will be using the `run` subcommand most frequently. You could, in theory, run the entire
pipeline in a single operation simply executing

.. code-block:: bash

    > yenta run

inside the virtual environment of the project. However, there are good reasons to not do this, or at least not do it
unless you are really confident about your setup. To understand this, it helps to understand the various functions of
the pipeline

-----------------------
Step 1: Data Collection
-----------------------

The first step that the pipeline code must do is to collect the data. This is not a trivial operation, particularly
when it requires interacting with remote APIs. The downloading of the data is handled by a library called
`ketl <https://github.com/grapesmoker/ketl>`_ which provides a way to set up download targets and cache downloads
appropriately. You are encouraged to read the documentation for ``ketl`` but the gist of it is that all interactions
with external data sources pass through two phases: the first phase, initiated by the ``setup()`` function call
of the API object, creates a set of database objects that represent both the files that are intended to be downloaded
and the files that are expected to emerge from the downloaded files; these latter objects can either just be the
downloaded files themselves or they can be files contained in downloaded archives. The second step is the actual
download of the files into the appropriate locations and extracting them.

These operations are carried out in the tasks that are prefixed by ``_etl``, e.g. ``biogrid_etl`` and so on. In
addition to handling the data acquisition, these tasks are also responsible for the preprocessing of data via the
transformer mechanism, which will be explained in the next section. Be aware that the download process can take a long
time if being run for the first time. However, any subsequent downloads will check for the presence of target files and
only download those files which are absent, making incremental updates much faster than running *de novo*.

---------------------
Step 2: Preprocessing
---------------------

Before the data can be used within HumanBase, it needs to be transformed into a common form, in this case a gene
network. Some datasets also need to be written to database tables in order to provide mappings between gene identifiers
that will be used in downstream tasks. This is accomplished within the formalism of the ``ketl`` library via the
concept of transformers, which are generic building blocks that take in a set of files and produce a Pandas dataframe.
The specific content of the transformation is described in the appropriate function documentation; for the purposes
of this guide, it suffices to understand that this functionality is also handled by the various ``_etl`` tasks,
and can take a fairly long amount of time, depending on which dataset is being transformed.

--------------------
Interlude: RefineBio
--------------------

Most of the ETL tasks will run relatively quickly, taking only a few minutes to complete their operations. The one
exception to this rule is the data harvesting task that is responsible for the bulk of the actual data volume, which
is the ``refine_bio_etl`` task. This task crawls the API of the `RefineBio <https://refine.bio/>`_ website
and collects both the set of files to be downloaded and the metadata related to those files, which describes the
organisms in the samples, the sequencing platform used, etc. There are about 10,000 microarray datasets within
RefineBio for *homo sapiens* so that should give you some idea of the size of the workload, although not all datasets
will ultimately end up being used in the analysis.

RefineBio requires special attention because it interacts with a live API that could, conceivably, change. Although
the API is quite mature and is unlikely to change completely, there are lacunae in the data (e.g. not all fields
of all objects will be filled) and it is being actively developed. Users of the pipeline should be aware that they
might need to actively debug the interface to RefineBio.

--------------------------------
Step 3: Analysis
--------------------------------

The full extent of the analysis performed by the HumanBase pipeline is outside the scope of this specific guide and
is detailed elsewhere in this document. There are a number of interdependent moving parts within the project, many of
which require the use of distributed task frameworks such as Celery and Dask to leverage processing on high-performance
computing clusters. These tasks also make use of a library called `grani <https://github.com/FunctionLab/grani>`_,
which implements a number of algorithms that were previously contained in a suite of command-line utilities called
Sleipnir.

The end product of the full pipeline is a set of probabilities for a given gene to be expressed in a given biological
context. This file is a simple table that can be imported into the HumanBase backend that powers the website.


---------------------------------
Incremental operations with Yenta
---------------------------------

You will likely want to run some tasks individually so you can check their output before moving on to downstream tasks.
Yenta provides you with a way to do that, and also to rerun tasks. Suppose I have a task **foo** that I wish to run again,
and also I don't want any of the downstream tasks of **foo** to run:

.. code-block:: bash

    > yenta run --force-rerun foo --up-to foo

This will force yenta to rerun the **foo** task (or run it for the first time if it hasn't been run before) and only
run the pipeline up to and including the **foo** task itself, which of course will merely cause only **foo** to run.
If you leave off the **--up-to** option, all the downstream tasks will run after foo completes, which may or may not
be the desired result. For more information on how to use yenta, check out the documentation linked above.
