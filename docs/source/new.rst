What's New
==========

1.7
---

* AppendRequests - Behaves the same way as HTable append RPCs, appending a byte array to the existing column.
* Secure HBase Support - The client can now connect to HBase clusters that require Kerberos or simple authentication.
* RPC Timeouts - HTable will fail an RPC if it doesn't respond within 60 seconds by default. AsyncHBase now supports timeouts with *per RPC overrides*! By default, timeouts are disabled to provide backwards compatibility.
* Region Information - A new API allows for scanning the meta table for all of the regions associated with a user table, returning the list of region names and servers that are hosting them.
* Region Client Stats - Another new API provides statistics about each region client for useful debugging.
* New Config Object - Many configuration parameters are now accessible and can be loaded from a Java properties style file.
* GetRequest Filters/Timestamps - GetRequests now support filters and timestamps for greater flexibility.
* Region Client Timeouts - Region clients will now close gracefully if data isn't sent or received for some period of time.
* New filters from HTable including:
  
  * FirstKeyOnlyFilter
  * ColumnPaginationFilter
  * KeyOnlyFilter
  * FuzzyRowFilter
