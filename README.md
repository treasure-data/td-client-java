[![CircleCI](https://circleci.com/gh/treasure-data/td-client-java/tree/master.svg?style=svg&circle-token=cf534b7f736cc552c29e905ed0fec0ef5a29fa47)](https://circleci.com/gh/treasure-data/td-client-java/tree/master)

# td-client for java

A java client for accessing [Treasure Data API](http://docs.treasuredata.com/articles/rest-api).
With this client, you can:
 - submit Hive/Presto queries to Treasure Data
 - check the status of jobs (queries)
 - retrieve query results
 - check the information of databases and tables

Since td-client-java 0.8.0, it requires Java 1.8 or higher. For Java7, use td-client-java-0.7.x.

td-client-java is licensed under Apache License Version 2.0.

## Download

You can download a jar file (td-client-java-(version)-shade.jar) from here: http://central.maven.org/maven2/com/treasuredata/client/td-client

For the information of the older versions, see <https://github.com/treasure-data/td-client-java/tree/0.7.x>.

- [Release Notes](CHANGES.txt)

### For Maven Users

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.treasuredata.client/td-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.treasuredata.client/td-client/)
[![Javadoc](https://javadoc-emblem.rhcloud.com/doc/com.treasuredata.client/td-client/badge.svg)](http://www.javadoc.io/doc/com.treasuredata.client/td-client)

Use the following dependency settings:

```
<dependency>
  <groupId>com.treasuredata.client</groupId>
  <artifactId>td-client</artifactId>
  <version>(version)</version>
</dependency>

<!-- If you are not using any slf4 logger binder, add the following dependency, too. -->
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.2.3</version>
</dependency>
```

#### Standalone jar
td-client-java provides a standalone jar, which include all of the dependencies into a single jar file:

```
<dependency>
  <groupId>com.treasuredata.client</groupId>
  <artifactId>td-client</artifactId>
  <version>(version)</version>
  <classifier>shade</classifier>
</dependency>
```

## Usage

To use td-client-java, you need to set your API key in the following file:

**$HOME/.td/td.conf**

```
[account]
  user = (your TD account e-mail address)
  apikey = (your API key)
```

You can retrieve your API key from [My profile](https://console.treasuredata.com/users/current) page.

It is also possible to use `TD_API_KEY` environment variable. Add the following configuration to your shell configuration `.bash_profile`, `.zprofile`, etc.

```
export TD_API_KEY = (your API key)
```

For Windows, add `TD_API_KEY` environment variable in the user preference panel.

### Proxy Server

If you need to access Web through proxy, add the following configuration to `$HOME/.td/td.conf` file:

```
[account]
  user = (your TD account e-mail address)
  apikey = (your API key)
  td.client.proxy.host = (optional: proxy host name)
  td.client.proxy.port = (optional: proxy port number)
  td.client.proxy.user = (optional: proxy user name)
  td.client.proxy.password = (optional: proxy password)
```

### Example Code

[Example.java](src/test/java/com/treasuredata/client/Example.java)

```java
import com.treasuredata.client.*;
import com.google.common.base.Function;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
...

// Create a new TD client by using configurations in $HOME/.td/td.conf
TDClient client = TDClient.newClient();
try {

// Retrieve database and table names
List<TDDatabase> databaseNames = client.listDatabases();
for(TDDatabase db : databaseNames) {
   System.out.println("database: " + db.getName());
   for(TDTable table : client.listTables(db.getName())) {
      System.out.println(" table: " + table);
   }
}

// Submit a new Presto query (for Hive, use TDJobReqult.newHiveQuery)
String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "select count(1) from www_access"));

// Wait until the query finishes
ExponentialBackOff backoff = new ExponentialBackOff();
TDJobSummary job = client.jobStatus(jobId);
while(!job.getStatus().isFinished()) {
  Thread.sleep(backoff.nextWaitTimeMillis());
  job = client.jobStatus(jobId);
}

// Read the detailed job information
TDJob jobInfo = client.jobInfo(jobId);
System.out.println("log:\n" + jobInfo.getCmdOut());
System.out.println("error log:\n" + jobInfo.getStdErr());

// Read the job results in msgpack.gz format
client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Object>() {
  @Override
  public Object apply(InputStream input) {
  try {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
    while(unpacker.hasNext()) {
       // Each row of the query result is array type value (e.g., [1, "name", ...])
       ArrayValue array = unpacker.unpackValue().asArrayValue();
       int id = array.get(0).asIntegerValue().toInt();
    }
  }
});

...

}
finally {
  // Never forget to close the TDClient. 
  client.close();
}
```

### Bulk upload

```java
// Create a new TD client by using configurations in $HOME/.td/td.conf
TDClient client = TDClient.newClient();

File f = new File("./sess/part01.msgpack.gz");

TDBulkImportSession session = client.createBulkImportSession("session_name", "database_name", "table_name");
client.uploadBulkImportPart(session.getName(), "session_part01", f);
```

### Data Connector Bulk Loading

```java
// Create a new TD client by using configurations in $HOME/.td/td.conf
TDClient client = TDClient.newClient();

client.startBulkLoadSession("session_name");
```

### Configuring TDClient

To configure TDClient, use `TDClient.newBuilder()`:

```java
TDClient client = TDClient
    .newBuilder()
    .setApiKey("(your api key)")
    .setEndpoint("api.ybi.idcfcloud.net")   // For using a non-default endpoint
    .build()
```

It is also possible to set the configuration with a `Properties` object:

```java
Properties prop = new Properties();
// Set your own properties
prop.setProperty("td.client.retry.limit", "10");
...

// This overrides the default configuration parameters with the given Properties
TDClient client = TDClient.newBuilder().setProperties(prop).build();
```

## List of Configuration Parameters

|key              | default value | description |
|-----------------|---------------|-------------|
|`apikey`  |               | API key to access Treasure Data. You can also set this via TD_API_KEY environment variable.  |
|`user`    |               | Account e-mail address (unnecessary if `apikey` is set) |
|`password`|               | Account password (unnecessary if `apikey` is set) |
|`td.client.proxy.host` |         | (optional) Proxy host e.g., "myproxy.com"  |
|`td.client.proxy.port` |         | (optional) Proxy port e.g., "80" |
|`td.client.proxy.user` |         | (optional) Proxy user |
|`td.client.proxy.password` |     | (optional) Proxy password  |
|`td.client.usessl` | true | (optional) Use SSL encryption |
|`td.client.retry.limit` | 7 | (optinoal) The maximum number of API request retry |
|`td.client.retry.initial-interval` | 500 | (optional) backoff retry interval = (interval) * (multiplier) ^ (retry count) |
|`td.client.retry.max-interval` | 60000 | (optional) max retry interval |
|`td.client.retry.multiplier` | 2.0 | (optional) retry interval multiplier |
|`td.client.connect-timeout` | 15000 | (optional) connection timeout before reaching the API |
|`td.client.read-timeout` | 60000 | (optional) timeout when no data is coming from API |
|`td.client.connection-pool-size` | 64 | (optional) Connection pool size|
|`td.client.endpoint` | `api.treasuredata.com` | (optional) TD REST API endpoint name |
|`td.client.port` | 80 for non-SSL, 443 for SSL connection | (optional) TD API port number |


The precedence of the configuration parameters are as follows:

1. Properties object passed to `TDClient.newBuilder().setProperties(Properties p)`
1. Parameters written in `$HOME/.td/td.conf`
1. System properties (passed with `-D` option when launching JVM)
1. Environment variable (only for TD_API_KEY parameter)

You can override the default configuration parameters given by the environment
variables with System properties, and then by `$HOME/.td/td.conf` file or `Properties` object.

## For Developers

### Build from the source code

```
$ git clone https://github.com/treasure-data/td-client-java.git
$ cd td-client-java
$ mvn package
```

This creates jar files within `target` folder.

### How to deploy to the Central repository

```
# update pom.xml, README.md and CHANGES.txt
$ mvn deploy -DperformRelease=true
$ sbt "sonatypeReleaseAll com.treasuredata"
```

See also https://github.com/xerial/sbt-sonatype#publishing-maven-projects to use `sbt sonatypeReleaseAll` command.
