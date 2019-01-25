# Presto Query History Extension

This module provides an implementation for query history extension, relying on a RDBMS to persist completed queries.

## How to use it

To enable this extension implementation for query history, you should:

* Add the jar of the extension implementation (`com.facebook.presto:presto-history-extension`) into the presto coordinator classpath.
* Add the jar of the appropriate sql jdbc (e.g. `org.mariadb.jdbc:mariadb-java-client`) into the presto coordinator classpath.
* Create a properties file in presto coordinator's the file system (e.g. `${PRESTO_ROOT}/etc/extension/history-extension.properties`)
* In the properties file, add the extension implementation property `com.facebook.presto.server.extension.query.history.QueryHistoryStore.impl` in the property file (e.g. `com.facebook.presto.server.extension.query.history.QueryHistoryStore.impl = com.facebook.presto.server.extension.query.history.QueryHistorySQLStore`). This is the implementation that we will use for the history extension
* In the properties file, add all the necessary jdbc properties under the namespace `sql.`, especially, `sql.jdbcUrl`, that's used to determine both jdbc connection and jdbc driver (e.g. `sql.jdbcUrl = jdbc:mariadb:://localhost:3306/DB?user=root&password=myPassword`)
* In the properties file, define appropriate `presto.cluster` which will be used to filter the origin of the query
* Add the jvm property `extension.query.history.config` to point to the properties file before (re)starting presto coordinator (e.g. `-Dextension.query.history.config = ${PRESTO_ROOT}/etc/extension/history-extension.properties`)

## Behind the scenes

`QueryTracker` relies on the property `extension.query.history.config` to discover the properties file. In the properties file, it discover which implementation to use for `QueryHistoryStore`. In the case of the implementation `com.facebook.presto.server.extension.query.history.QueryHistorySQLStore`, it uses the section `sql.` to find all jdbc connection configurations.

If the property `extension.query.history.config` is not defined or it failed to create the extension implementation (either the properties file is not found, or any error during the instantiation such as `ClassNotFoundException` or `SQLException`), no extension will be used.

## Sample of the extension properties file

```
# cat history-extension.properties
com.facebook.presto.server.extension.query.history.QueryHistoryStore.impl = com.facebook.presto.server.extension.query.history.QueryHistorySQLStore
sql.jdbcUrl = jdbc:mariadb:://localhost:3306/DB?user=root&password=myPassword
presto.cluster=preprod-pa4
```