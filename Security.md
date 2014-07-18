This patch add support for security enabled 0.94. It currently has only one mechanism: kerberos. So this can't be used in MapReduce jobs or the like. We've internally been using a hacky implementation of this patch as part of opentsdb and have had no issues so far.

Most of the logic is in SecurityHelper which does the handshaking, wrapping, etc.

There were some configs that needed to be set. Since we don't have a config object everything is passed via properties:

````
org.hbase.async.security.94
hbase.security.authentication=<MECHANISM>
hbase.kerberos.regionserver.principal=<REGIONSERVER PRINCIPAL>
hbase.rpc.protection=[authentication|integrity|privacy]
hbase.sasl.clientconfig=<JAAS Profile Name>
java.security.auth.login.config=<Path to JAAS conf>
````
Basic properties that need to be set are:

````
-Dorg.hbase.async.security.94 -Dhbase.security.authentication=kerberos
-Dhbase.kerberos.regionserver.principal=hbase/_HOST@MYREALM.COM
-Dhbase.sasl.clientconfig=Client -Djava.security.auth.login.config=/path/to/jaas.conf
````
A JAAS profile looks like this:

For non-headless (ie using kinit):

````
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=false
  useTicketCache=true;
};
````
For headless users or services:

````
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  useTicketCache=false
  keyTab="/path/to/hbase.keytab"
  principal="hbase/rs1.host.com@MYREALM.COM";
};
````
Note the value passed in hbase.sasl.clientconfig should match the profile name defined in the jaas conf.
