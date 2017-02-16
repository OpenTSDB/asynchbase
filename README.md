# Asynchronous HBase

This is an alternative Java library to use HBase in applications that require
a fully asynchronous, non-blocking, thread-safe, high-performance HBase API.

This HBase client differs significantly from HBase's client (HTable).
Switching to it is not easy as it requires to rewrite all the code that was
interacting with any HBase API.  This pays off in applications that are
asynchronous by nature or that want to use several threads to interact
efficiently with HBase.

Documentation can be found under http://opentsdb.github.io/asynchbase/

Please read the Javadoc starting from the HBaseClient class.  This class
replaces all your HTable instances.  Unlike HTable, you should have only
one instance of HBaseClient in your application, regardless of the number
of tables or threads you want to use.  The Javadoc also spells out rules
you have to follow in order to use the API properly in a multi-threaded
application.

The [GitHub repo branches](https://github.com/OpenTSDB/asynchbase) are laid 
out as follows:

* [`maintenance`](https://github.com/OpenTSDB/opentsdb/tree/maintenance): This
  was the last stable version of AsyncHBase and should only have bug fix PRs
  created against it. Bugs should also be patched in master and next.

* [`master`](https://github.com/OpenTSDB/opentsdb/tree/master): This is the
  current stable version of AsyncHBase and should only have bug fix PRs created
  against it. Bug should also be patched in the next branch.

* [`next`](https://github.com/OpenTSDB/opentsdb/tree/next): This is the
  development version of AsyncHBase and all new features or API changes should
  be written against this.
