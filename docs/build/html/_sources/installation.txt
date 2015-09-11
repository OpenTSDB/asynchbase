Installation
============

AsyncHBase may be compiled from source or fetched from Maven as a pre-compiled Jar. Releases can be found on `Github <https://github.com/OpenTSDB/asynchbase/releases>`_ or `Maven <http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22asynchbase%22>`_. Snapshots can be found on `Sonatype.org <https://oss.sonatype.org/content/repositories/snapshots/org/hbase/asynchbase/1.7.0-SNAPSHOT/>`_.

Runtime Requirements
^^^^^^^^^^^^^^^^^^^^

The pre-compiled Jars simply require Java 1.6 or later to run. 

Building
^^^^^^^^

Compiling From Source
---------------------

Complication requirements include:

* A Linux system
* Java Development Kit 1.6 or later
* Autotools
* Make
* Python
* Git
* An Internet connection
* Maven (Optional)

Download the latest version using ``git clone`` command or download a release from the site or Github. Then cd into the repo's directory and ``git checkout <branch>`` the proper branch you want to build from. Then just run ``make``.

::

 git clone git://github.com/OpenTSDB/asynchbase.git
 cd asynchbase
 make
 ls build

If compilation was successful, you should have a jar file in ``./build`` that you can include in your project. Jars that AsyncHBase depends on are downloaded to the ``third_party/`` sub directories under the main repo directory.

.. NOTE:: 

  Yes, we're aware that using Make for a java project is rather odd. But it allows for some nifty tricks that aren't as easily performed using a standard Java build tool. That being said, if you prefer Maven, simply execute ``make pom.xml`` from the repo directory and continue as you normally would. E.g. ``mvn clean package`` and you'll wind up with the jar and associated files in ``target/``.

**Testing**

Simply running ``make`` will build the jar but won't run various tests. For unit tests, run ``make check``. This will download jars required for testing and run all of the unit tests in ``test``. 

For integration testing, first start up an HBase instance on the local host with default ports (e.g. Zookeeper on port 2181) and without authentication. Then export the path to HBase's home directory. This is needed as the integration tests use the shell to create and truncate tables. E.g. ``export HBASE_HOME=/usr/local/hbase``. Then run ``make integration``. 

.. WARNING:: 

  The integration tests will create and/or truncate data in various tables starting with ``test`` such as ``test``, ``test1``, etc. Make sure you don't have any important information in your local HBase instance.

Source Layout
-------------

There are two main branches in the GIT repo. The ``master`` branch is the latest stable release along with any bug fixes that have been committed between releases. The ``next`` branch is the next major or minor version of AsyncHBase with new features and development. When ``next`` is stable, it will be merged into ``master``. Additional branches may be present and are used for testing or developing specific features.

Upgrading
^^^^^^^^^

We strive to make sure AsyncHBase is always backwards compatible. To upgrade to a new version, simply replace your old Jar with the new one. Be sure to test it first before going into production.

If you are upgrading HBase, make sure to try AsyncHBase with the new HBase in a development environment to make sure all RPCs are still compatible.
