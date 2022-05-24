============
Installation
============

As of the present time, the pipeline is not ``pip-`` installable (if this changes, the responsible party should
update this document). Assuming you have the proper credentials, you can obtain the code of the project by doing

.. code-block:: bash

    > git clone https://github.com/FunctionLab/humanbase-pipeline

Once the repository has been cloned, ``cd`` into it, create a virtual environment, enter it, and install the
prerequisites via

.. code-block:: bash

    > pip install -r requirements.txt

This should hopefully proceed without incident. Once you are done, you can start using the pipeline. Ha ha! Just kidding!
Before you can actually use the pipeline you'll need to read a little about how the Yenta taskrunner works.


--------------------------------
Setting up external dependencies
--------------------------------

In addition to depending on libraries that are installable via ``pip``, the pipeline depends on a number of external
pieces of software that will need to be installed independently, and the project configured to point to them. The two
pieces of software you will absolutely need are:

1. PostgreSQL - for managing file caches and gene mappings
2. Redis - for distributing tasks across a cluster using Celery

This guide will assume that you have managed to install these dependencies in whatever manner is appropriate for
your system and that both PostgreSQL and Redis are running somewhere in the background and are reachable by the
project code.


----------------------
Generating ketl tables
----------------------

The HumanBase pipeline uses a library called `ketl <https://github.com/grapesmoker/ketl>`_ to handle the downloading,
preprocessing, and storage of the raw data. This library will be installed as part of the requirements, but before you
can use it, you will need to create the tables that it relies on. You can do this by running

.. code-block:: bash

    > ketl create-tables

By default, ketl will create tables in a local SQLite database called ``ketl.db``. This is most likely not what you want,
so to configure which database ketl should use, you will either need to supply a command-line option like so:

.. code-block:: bash

    > ketl create-tables --db-dsn postgresql://humanbase:humanbase@localhost/humanbase

or create a ``ketl.ini`` file in the root directory of the project and place the following contents into it:

.. code-block:: ini

    [ketl]
    DB_DSN = postgresql://humanbase:humanbase@localhost/humanbase

Obviously you should modify the DSN string if you are using some other configuration.


-------------
Configuration
-------------

The code expects to find a file called ``hb.toml`` by default, unless that value is overridden by the ``HB_CONFIG``
environment variable. That files is parsed to obtain both general configuration parameters for the project as well as
task-specific configuration parameters. A sample file, ``hb.toml.sample`` is provided with the project and contains
sensible default settings, as well as some commentary on what should be changed. You should use that file as a template
to make ``hb.toml`` with the appropriate changes; the resulting config file should not be checked into the repository.


--------------------------
Generating database models
--------------------------

The HumanBase pipeline uses a large number of database models to track the mappings and other data products that are
used by the project. The migrations of the tables that use these products is handled by the ``alembic`` tool, which is
installed as part of the requirements. To create the database tables, find the ``alembic.ini`` file, and locate the
line that starts off as

.. code-block:: bash

    sqlalchemy.url = postgresql://humanbase:humanbase@localhost/humanbase

Change the value of this parameter to the same thing that you set in the ``ketl.ini`` file. This will tell alembic where
to put your data tables. Now, you can run.

.. code-block:: bash

    > alembic upgrade head

This should create the relevant database tables