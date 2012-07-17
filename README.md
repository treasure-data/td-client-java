# Java Client Library for Treasure Data Cloud

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

Java >= 1.6

## Install

### Install with all-in-one jar file

You can download all-in-one jar file for Treasure Data Logger.

    $ wget http://maven.treasure-data.com/com/treasure_data/td-client/${client.version}/td-client-${client.version}-jar-with-dependencies.jar

To use Treasure Data Cloud for Java, set the above jar file to your classpath.

### Install from Maven repository

Treasure Data Logger for Java is released on Treasure Data's Maven repository.
You can configure your pom.xml as follows to use it:

    <dependencies>
      ...
      <dependency>
        <groupId>com.treasure_data</groupId>
        <artifactId>td-client</artifactId>
        <version>${client.version}</version>
      </dependency>
      ...
    </dependencies>

    <repositories>
      <repository>
        <id>treasure-data.com</id>
        <name>Treasure Data's Maven Repository</name>
        <url>http://maven.treasure-data.com/</url>
      </repository>
      <repository>
        <id>fluentd.org</id>
        <name>Fluentd's Maven Repository</name>
        <url>http://fluentd.org/maven2</url>
      </repository>
    </repositories>
    
### Install with SBT (Build tool Scala)

To install td-client From SBT (a build tool for Scala), please add the following lines to your build.sbt.

    /* in build.sbt */
    // Repositories
    resolvers ++= Seq(
      "td-client     Maven Repository" at "http://maven.treasure-data.com/",
      "fluent-logger Maven Repository" at "http://fluentd.org/maven2/"
    )
    // Dependencies
    libraryDependencies ++= Seq(
      "com.treasure_data" % "td-client" % "${client.version}"
    )

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-client-java.git
    $ cd td-client-java
    $ mvn package

You will get the td-client jar file in td-client-java/target 
directory.  File name will be td-client-${client.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

**Replace ${client.version} with the current version of Treasure Data Cloud for Java.**
**The current version is 0.1.2.**

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

please configure your treasure-data.properties file using the commands shown below:

    td.api.key=<your API key>
    td.api.server.host=api.treasure-data.com
    td.api.server.port=80

### Issue Queries

Below is an example of issuing a query from a Java program. The query API is asynchronous, and you can wait for the query to complete by polling the job periodically.

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
            System.out.println(job.getJobID());
    
            while (true) {
                JobSummary jobSummary = client.showJob(job);
                System.out.println(jobSummary.getStatus());
                if (jobSummary.getStatus() == JobSummary.Status.SUCCESS) {
                    break;
                }
    
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                }
            }
    
            JobResult jobResult = client.getJobResult(job);
            Unpacker unpacker = jobResult.getResult();
            UnpackerIterator iter = unpacker.iterator();
            while (iter.hasNext()) {
                System.out.println(iter.next());
            }
        }
    }

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

## License

Apache License, Version 2.0
