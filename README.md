# td-client for java

A java client for accessing [Treasure Data API](http://docs.treasuredata.com/articles/rest-api).
With this client, you can:
 - Make queries to Treasure Data
 - Check the status of jobs (queries)
 - Retrieve query results
 - Check the information of databases and tables

td-client-java is built for Java 1.7 or higher, and licensed under Apache License Version 2.0.

## Download

You can download a jar file (td-client-java-(version)-jar-with-dependencies.jar) from here: http://central.maven.org/maven2/com/treasuredata/client/td-jdbc

For the information of the older versions, see <https://github.com/treasure-data/td-client-java/tree/0.5.x>.

### For Maven Users

Use the following dependency setting:

```
<dependency>
  <groupId>com.treasuredata.client</groupId>
  <artifactId>td-client</artifactId>
  <version>${client.version}</version>
</dependency>
```

### For Scala Users

Add the following sbt settings:

```
libraryDependencies ++= Seq("com.treasuredata.client" % "td-client" % "${client.version}")
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

```java
import com.treasuredata.client.TDClient;
import com.treasuredata.client.ExponentialBackOff;
import com.google.common.base.Function;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
...

// Create a new TD client by using configurations in $HOME/.td/td.conf
TDClient client = new TDClient();

// Retrieve database and table names
List<String> databaseNames = client.listDatabases();
for(String db : databaseNames) {
   System.out.println("database: " + db);
   for(TDTable table : client.listTables(db)) {
      System.out.println(" table: " + table);
   }
}

// Submit a new Presto query
String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_dataset", "select count(1) from www_accesses"));

// Wait until the query finishes
ExponentialBackOff backoff = new ExponentialBackOff();
TDJobSummary job = client.jobStatus(jobId);
while(!job.getStatus().isFinished()) {
  Thread.sleep(backOff.nextWaitTimeMillis());
  job = client.jobStatus(jobId);
}

// Read the detailed job information
TDJob jobInfo = client.jobInfo(jobId)
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
       int id = array.get(0).asIntegerValue().toInt
    }
  }
});

```

### Bulk upload

```java
// Create a new TD client by using configurations in $HOME/.td/td.conf
TDClient client = new TDClient();

File f = new File("./sess/part01.msgpack.gz");

TDBulkImportSession session = client.createBulkImportSession("session_name", "database_name", "table_name");
client.uploadBulkImportPart(session.getName(), "session_part01", f);
```

### Custom Configuration

You can set configuration parameters by using ``$HOME/.td/td.conf`` file and `Properties` object:

```java
Properties prop = TDClientConfig.readTDConf(); // Set the default values by reading $HOME/.td/td.conf file
// Set your own properties
prop.setProperty("key", "value");
...

// This uses configuration parameters in the Properties object as the default values.
// You can overwrite this by using System properties
TDClietnConfig config = TDClientConfig.newConfig(p)
TDClient client = new TDClient(config);
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
|`td.client.usessl` | false | (optional) Use SSL encryption |
|`td.client.retry.limit` | 7 | (optinoal) The maximum number of API request retry |
|`td.client.retry.initial-interval` | 500 | (optional) backoff retry interval = (interval) * (multiplier) ^ (retry count) |
|`td.client.retry.max-interval` | 60000 | (optional) max retry interval |
|`td.client.retry.multiplier` | 2.0 | (optional) retry interval multiplier |
|`td.client.connect-timeout` | 15000 | (optional) connection timeout before reaching the API |
|`td.client.idle-timeout` | 60000 | (optional) idle connection timeout when no data is coming from API |
|`td.client.connection-pool-size` | 64 | (optional) Connection pool size|
|`td.client.endpoint` | `api.treasuredata.com` | (optional) TD REST API endpoint name |
|`td.client.port` | 80 for non-SSL, 443 for SSL connection | (optional) TD API port number |


You can overwrite the configurations by using environment variables or System properties. The precedence of the configuration parmeters are as follows:

1. Environment variable (only for TD_API_KEY parameter)
1. System properties (passed with `-D` option when launching JVM)
1. Properties object passed to TDClientConfig.newConfig(Properties p)


## For Developers

### Build from the source code

```
$ git clone https://github.com/treasure-data/td-client-java.git
$ cd td-client-java
$ mvn package
```

This creates jar files within `target` folder.



