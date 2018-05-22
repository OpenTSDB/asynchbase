Authentication
==============

HBase supports Kerberos and simple client authentication to the cluster. It also supports optional encryption of network payloads. 

The recommended means of authenticating with HBase is through Kerberos. There are numberous guides available to help set up Kerberos with HBase and if you use a distibution from a vendor, they may have scripts to help you with the configuration. 

For the AsyncHBase, some parameters must be set, passed in via the ``Config`` object.

* ``hbase.security.auth.enable`` must be set to ``true`` in order for authentication to work.
* ``hbase.security.authentication`` must be set to ``kerberos``.
* ``hbase.sasl.clientconfig`` must be set to an entry in a JAAS config file as documented below.
* ``hbase.kerberos.regionserver.principal`` must be set. Instances of ``_HOST`` will be replaced with a host name. E.g. a setting may look like ``tsd.storage.kerberos.principal=myhbaseuser/_HOST@MY.HADOOP.DOMAIN``
* Optionally, ``hbase.security.authentication`` may be set to ``protected`` if you require RPC encryption (the payload over the network will be encrypted). By default the payload is not encrypted.
* It *may* be possible to set ``hbase.regionserver.kerberos.password`` with the password but we haven't tested that yet. Instead it's preferred to use a *keytab* file that contains the kerberos user credentials.

An additional JVM flag must be passed to the application running the AsyncHBase client. The ``java.security.auth.login.config`` parameter is required with a value pointing to a JAAS config file as in:

::

	Client {
	  com.sun.security.auth.module.Krb5LoginModule required
	  useKeyTab=true
	  useTicketCache=false
	  keyTab="/path/to/keytab.keytab"
	  principal="myhbaseuser@MY.HADOOP.DOMAIN";
	};

Make sure that the file specified in the ``keyTab`` parameter is present on the host.

Additionally, if the Zookeeper cluster is not secured with Kerberos, pass in ``zookeeper.sasl.client=false`` as a JVM argument.