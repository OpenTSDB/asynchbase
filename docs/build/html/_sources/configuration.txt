Configuration
-------------

AsyncHBase 1.7 has a new configuration class ``Config`` that enables tweaking many parameters of the client. For backwards compatibility, the existing ``HBaseClient`` constructors will instantiate a new config object with default values. However new constructors will accept a config object.

To create a config with default values, instantiate it via ``Config config = new Config();``. Or to load properties from a file, call ``Config config = new Config(<path_to_file>);`` where the file is a standard Java properties file with entries like ``hbase.rpc.timeout = 60000`` separated by new lines.

To modify the (default or parsed from file) value of a configuration entry, simply call ``config.overrideConfig(<property>, <value>);``. Make sure to modify values *before* passing the config to the ``HBaseClient`` constructor. Modifying values of the config after instantiating the client will not cause an effect as many values are read from the config and stored in the client's memory, never to be read again.

Properties
^^^^^^^^^^

The following is a table of configuration options for AsyncHBase. Some of the values share names in common with HBase's configs but may behave slightly differently. Be sure to read the notes associated with the config before proceeding.

.. csv-table::
   :header: "Property", "Type", "Description", "Default"
   :widths: 20, 5, 65, 10

   "hbase.client.retries.number", "Integer", "The number of times an RPC will be retried when a recoverable exception occurs, e.g. a region moving or splitting. Non-recoverable exceptions will fail the RPC immediately.", "10"
   "hbase.increments.buffer_size", "Integer", "How many unique counters to maintain in memory when using ``BufferedIncrement`` RPCs. Once this limit is reached, new buffered increments are immediately sent to the region server.", "65,535"
   "hbase.increments.durable", "Boolean", "Whether or not to write to the WAL for every atomic increment request.", "true"
   "hbase.ipc.client.connection.idle_timeout", "Integer", "How long, in seconds, to wait before closing an idle connection to a region server. Idle means no writes or reads to the socket.", "300"
   "hbase.ipc.client.socket.receiveBufferSize", "Integer", "The size of the region server socket receive buffer in bytes.", "System dependent"
   "hbase.ipc.client.socket.sendBufferSize", "Integer", "The size of the region server socket send buffer in bytes.", "System dependent"
   "hbase.ipc.client.socket.timeout.connect", "Integer", "How long, in milliseconds, to wait for a socket connection attempt to a region server.", "5000"
   "hbase.ipc.client.tcpkeepalive", "Boolean", "Whether or not to enable keep-alives on the TCP socket to the region servers.", "true"
   "hbase.ipc.client.tcpnodelay", "Boolean", "Whether or not to bypass TCP spooling and send packets immediately to the region servers.", "true"
   "hbase.kerberos.regionserver.principal", "String", "A name or template to use as the principal for kerberos authentication. Instances of the string ``_HOST`` will be replaced with the host name of the client.", ""
   "hbase.nsre.high_watermark", "Integer", "The maximum number of RPCs allowed in the NSRE queue for a region before new RPCs are failed with please throttle exceptions.", "10000"
   "hbase.nsre.low_watermark", "Integer", "The number of RPCs, per region server, that must be present in the NSRE queue before warnings start appearing in the log file.", "1000"
   "hbase.region_client.check_channel_write_status", "Boolean", "Whether or not to check the write status of the region server connection's socket before attempting to send the RPC. If a region server is slow, the client may buffer a large amount of data in memory while waiting for the server to consume data. This can lead to out of memory issues or a very large heap. When this is enabled and the buffer is full, RPCs will fail with please throttle exceptions.", "false"
   "hbase.region_client.inflight_limit", "Integer", "The maximum number of RPCs sent to the connected region server and waiting a response. A large number of inflight RPCs can lead to out of memory issues. A value of 0 will disable this limit. If the limit is reached, RPCs will fail with please throttle exceptions.", "0"
   "hbase.region_client.pending_limit", "Integer", "The maximum number of RPCs queued while waiting for a region server connection to complete. If a region server is slow to respond to a connection request, or authentication takes a long time, then a large queue of RPCs may build up in memory. A value of 0 will disable this limit. If the limit is reached, RPCs will fail with please throttle exceptions.", "0"
   "hbase.regionserver.kerberos.password", "String", "NOT RECOMMENDED OR TESTED: A password for the region server when authenticating via kerberos.", ""
   "hbase.rpcs.batch.size", "Integer", "The number of individual RPCs that can be buffered before being sent to the region server. If this limit is reached before ``hbase.rpcs.buffered_flush_interval`` then the batch is sent out immediately and a new batch is started.", "1024"
   "hbase.rpcs.buffered_flush_interval", "Integer", "How often, in milliseconds, to flush buffered RPCs (Puts, Appends, etc) to the region servers.", "1000"
   "hbase.rpc.protection", "String", "Whether or not to encrypt RPCs while they are traversing the network. May be ``authentication`` (no encryption), ``integrity``, (no encryption) or ``privacy`` (encrypted). Requires authentication to be enabled."
   "hbase.rpc.timeout", "Integer", "How long, in milliseconds, to wait for a response to an RPC from a region server before failing the RPC with a ``RpcTimedOutException``. This value can be overridden on a per-RPC basis. A value of 0 will not allow RPCs to timeout.", "0"
   "hbase.sasl.clientconfig", "String", "The section in a JAAS configuration file to use when authenticating against a region server.", "Client"
   "hbase.security.auth.94", "Boolean", "Whether or not the client is connection to an HBase 0.94 cluster with authentication.", "false"
   "hbase.security.auth.enable", "Boolean", "Whether or not to enable authentication when connecting to HBase. Please see ___TODO___ for more information."
   "hbase.security.authentication", "String", "The type of authentication required for the HBase cluster. May be ``kerberos`` or ``simple``.", ""
   "hbase.security.simple.username", "String", "The user name for authenticating against an HBase cluster with simple authentication. (Not recommended)", ""
   "hbase.timer.tick", "Integer", "How often, in milliseconds, to execute the flush and timeout timers, looking for new tasks. This value should not need modification.", "20"
   "hbase.timer.ticks_per_wheel", "Integer", "How many hash buckets are used for scheduling tasks in the flush and timeout timers. This value should not need modification.", "512"
   "hbase.workers.size", "Integer", "The number of worker threads to instantiate for region client connections.", "2 * CPU cores"
   "hbase.zookeeper.getroot.retry_delay", "Integer", "How long, in milliseconds, to wait between attempts fetching the root region from Zookeeper if a call fails.", "1000"
   "hbase.zookeeper.quorum", "String", "A comma-separated list of ZooKeeper hosts to connect to, with or without port specifiers.
   E.g. ``192.168.1.1:2181,192.168.1.2:2181``", "localhost"
   "hbase.zookeeper.session.timeout", "Integer", "How long, in milliseconds, to maintain a Zookeeper connection. After the session times out, the connection is closed and a new one will be opened the next time AsyncHBase needs root.", "5000"
   "hbase.zookeeper.znode.parent", "String", "Path under which the znode for the -ROOT- region is located", "/hbase"

.. NOTE::

  For authentication with Kerberos, a JAAS file must be created and passed to the JVM using AsyncHBase in the system parameter ``java.security.auth.login.config``.

Data Types
^^^^^^^^^^

Some configuration values require special consideration:

* Booleans - The following literals will parse to ``True``:

  * ``1``
  * ``true``
  * ``yes``
  
  Any other values will result in a ``False``. Parsing is case insensitive
  
* Strings - Strings, even those with spaces, do not require quotation marks, but some considerations apply:

  * Special characters must be escaped with a backslash include: ``#``, ``!``, ``=``, and ``:``
    E.g.::
    
      my.property = Hello World\!
      
  * Unicode characters must be escaped with their hexadecimal representation, e.g.::
  
      my.property = \u0009
