These are the protobuf definition files used by AsyncHBase.
They were copied from HBase (see under hbase-protocol/src/main/protobuf).

The following changes were made to those files:
  - the package name was changed to "org.hbase.async.protobuf.generated"
  - the `java_outer_classname' were normalized so that things would be
    consistently named (file Foo.proto generates FooPB.java).
  - code is optimize_for LITE_RUNTIME instead of SPEED to reduce code bloat.
  - java_generate_equals_and_hash and java_generic_services were switched
    to false.  The former isn't useful and the latter is incompatible with
    the LITE_RUNTIME.

The files in this directory are subject to the license in LICENSE.txt.
