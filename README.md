# Treasure Data Client for Java

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, analyzing these logs easily and
reliably is a challenging task.

Treasure Data Cloud solves the problem by having: easy installation, small
footprint, plugins reliable buffering, log forwarding, the log analyzing, etc.

  * Treasure Data website: [http://treasure-data.com/](http://treasure-data.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

**td-client-java** is a Java library, to access Treasure Data Cloud from Java application.

## Requirements

Java >= 1.7

## Install

### Install with all-in-one jar file

You can download all-in-one jar file for Treasure Data Logger.

    $ wget http://central.maven.org/maven2/com/treasuredata/td-client/${client.version}/td-client-${client.version}-jar-with-dependencies.jar

To use Treasure Data Cloud for Java, set the above jar file to your classpath.

### Install from Maven repository

Treasure Data Client for Java is released on Treasure Data's Maven repository.
You can configure your pom.xml as follows to use it:

    <dependencies>
      ...
      <dependency>
        <groupId>com.treasuredata.client</groupId>
        <artifactId>td-client</artifactId>
        <version>${client.version}</version>
      </dependency>
      ...
    </dependencies>

### Install with SBT (Scala Build Tool)

To install td-client From SBT (a build tool for Scala), please add the following lines to your build.sbt.

    /* in build.sbt */
    // Repositories
    resolvers ++= Seq(
      "fluent-logger Maven Repository" at "http://fluentd.org/maven2/"
    )
    // Dependencies
    libraryDependencies ++= Seq(
      "com.treasuredata" % "td-client" % "${client.version}"
    )

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-client-java.git
    $ cd td-client-java
    $ mvn package -Dmaven.test.skip=true

You will get the td-client jar file in td-client-java/target
directory.  File name will be td-client-${client.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

**Replace ${client.version} with the current version of Treasure Data Cloud for Java.**
**The current version is 0.5.0.**

## Configuration

Please configure your treasure-data.properties file using the properties listed below:

### API key

Please configure your treasure-data.properties file using the commands shown below:

    td.api.key=<your API key>

The same information can be provided with the `TREASURE_DATA_API_KEY` environment
variable, e.g.:

    TREASURE_DATA_API_KEY="<your API key>"

The environment variable takes precedence over the property specified above.

### Endpoint

The endpoint is specified with the `td.api.server.*` properties:

    td.api.server.scheme=https://
    td.api.server.host=api.treasuredata.com
    td.api.server.port=443

The default `td-client-java` endpoint is `https://api.treasuredata.com:443` and
by default the HTTPS protocol is used. If you want to use http instead of https,
you can configure your properties file like following.

    td.api.server.scheme=http://
    td.api.server.host=api.treasuredata.com
    td.api.server.port=80

The same information can be provided with the `TD_API_SERVER` environment
variable, e.g.:

    TD_API_SERVER="https://api.treasuredata.com:443"

The environment variable takes precedence over the properties specified above.

### Proxy

If you configure http proxy, please add the following lines to your treasure-data.properties.

    http.proxyHost=<your proxy server's host>
    http.proxyPort=<your proxy server's port>


## Quickstart

### List Databases and Tables

Below is an example of listing databases and tables.

    import java.io.IOException;
    import java.util.List;
    import java.util.Properties;

    import com.treasure_data.client.ClientException;
    import com.treasure_data.client.TreasureDataClient;
    import com.treasure_data.model.DatabaseSummary;
    import com.treasure_data.model.TableSummary;

    public class Main {
        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() throws ClientException {
            TreasureDataClient client = new TreasureDataClient();

            List<DatabaseSummary> databases = client.listDatabases();
            for (DatabaseSummary database : databases) {
                String databaseName = database.getName();
                List<TableSummary> tables = client.listTables(databaseName);
                for (TableSummary table : tables) {
                    System.out.println(databaseName);
                    System.out.println(table.getName());
                    System.out.println(table.getCount());
                }
            }
        }
    }

### Issue Queries

Below is an example of issuing a query from a Java program. The query API is asynchronous, and you can wait for the query to complete by polling the job periodically.

    import java.io.IOException;
    import java.util.Properties;

    import org.msgpack.unpacker.Unpacker;
    import org.msgpack.unpacker.UnpackerIterator;

    import com.treasure_data.client.ClientException;
    import com.treasure_data.client.TreasureDataClient;
    import com.treasure_data.model.Database;
    import com.treasure_data.model.Job;
    import com.treasure_data.model.JobResult;
    import com.treasure_data.model.JobSummary;

    public class Main {
        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() throws ClientException {
            TreasureDataClient client = new TreasureDataClient();

            Job job = new Job(new Database("testdb"), "SELECT COUNT(1) FROM www_access");
            client.submitJob(job);
            client.submitJob(job);
            String jobID = job.getJobID();
            System.out.println(jobID);

            while (true) {
                JobSummary.Status stat = client.showJobStatus(job);
                if (stat == JobSummary.Status.SUCCESS) {
                    break;
                } else if (stat == JobSummary.Status.ERROR) {
                    String msg = String.format("Job '%s' failed: got Job status 'error'", jobID);
                    JobSummary js = client.showJob(job);
                    if (js.getDebug() != null) {
                        System.out.println("cmdout:");
                        System.out.println(js.getDebug().getCmdout());
                        System.out.println("stderr:");
                        System.out.println(js.getDebug().getStderr());
                    }
                    throw new ClientException(msg);
                } else if (stat == JobSummary.Status.KILLED) {
                    String msg = String.format("Job '%s' failed: got Job status 'killed'", jobID);
                    throw new ClientException(msg);
                }

                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    // do something
                }
            }

            JobResult jobResult = client.getJobResult(job);
            Unpacker unpacker = jobResult.getResult(); // Unpacker class is MessagePack's deserializer
            UnpackerIterator iter = unpacker.iterator();
            while (iter.hasNext()) {
                ArrayValue row = iter.next().asArrayValue();
                for (Value elm : row) {
                    System.out.print(elm + ",");
                }
                System.out.println();
            }
        }
    }

JobResult is model class for query result. The object has Unpacker object. The Unpacker and the iterator, UnpackerIterator object, allow users to download query result via internet and use the raw data.

### List and Get the Status of Jobs

Below is an example of listing and get the status of jobs.

    import java.io.IOException;
    import java.util.List;
    import java.util.Properties;

    import com.treasure_data.client.ClientException;
    import com.treasure_data.client.TreasureDataClient;
    import com.treasure_data.model.JobSummary;

    public class Main {
        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() throws ClientException {
            TreasureDataClient client = new TreasureDataClient();

            List<JobSummary> jobs = client.listJobs(0, 127);
            for (JobSummary job : jobs) {
                System.out.println(job.getJobID());
                System.out.println(job.getStatus());
            }
        }
    }

### Show Table Schema and Update the Schema to Table

    import java.io.IOException;
    import java.util.Arrays;
    import java.util.Properties;

    import com.treasure_data.client.ClientException;
    import com.treasure_data.client.TreasureDataClient;
    import com.treasure_data.model.TableSchema;

    public class Main {
        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() throws ClientException {
            TreasureDataClient client = new TreasureDataClient();

            // show current schema
            TableSchema schema = client.showTableSchema("testdb", "testtbl");
            System.out.println(schema.getPairsOfColsAndTypes());

            // set schema
            client.setTableSchema("testdb", "testtbl", Arrays.asList("id:string", "age:int", "name:string"));

            // remove schema
            client.removeTableSchema("testdb", "testtbl", Arrays.asList("age", "name"));
        }
    }

### Bulk-Upload Data on Bulk Import Session

    import java.io.BufferedInputStream;
    import java.io.File;
    import java.io.FileInputStream;
    import java.io.InputStream;

    import com.treasure_data.auth.TreasureDataCredentials;
    import com.treasure_data.client.TreasureDataClient;
    import com.treasure_data.client.bulkimport.BulkImportClient;
    import com.treasure_data.model.bulkimport.Session;

    public class Main {
        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() throws ClientException {
            TreasureDataClient client = new TreasureDataClient();
            BulkImportClient biclient = new BulkImportClient(client);

            String name = "session_name";
            String database = "database_name";
            String table = "table_name";
            String partID = "session_part01";

            File f = new File("./sess/part01.msgpack.gz");
            InputStream in = new BufferedInputStream(new FileInputStream(f));

            Session session = new Session(name, database, table);
            biclient.uploadPart(session, partID, in, (int) f.length());
        }
    }


## License

Apache License, Version 2.0
