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

    $ wget http://treasure-data.com/maven2/com/treasure_data/td-client/${client.version}/td-client-${client.version}-jar-with-dependencies.jar

To use Treasure Data Cloud for Java, set the above jar file to your classpath.

### Install from Maven2 repository

Treasure Data Logger for Java is released on Treasure Data's Maven2 repository.
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
        <name>Treasure Data's Maven2 Repository</name>
        <url>http://treasure-data.com/maven2</url>
      </repository>
      <repository>
        <id>fluentd.org</id>
        <name>Fluentd's Maven2 Repository</name>
        <url>http://fluentd.org/maven2</url>
      </repository>
    </repositories>
    
### Install with SBT (Build tool Scala)

To install td-client From SBT (a build tool for Scala), please add the following lines to your build.sbt.

    /* in build.sbt */
    // Repositories
    resolvers ++= Seq(
      "td-client     Maven2 Repository" at "http://treasure-data.com/maven2/",
      "fluent-logger Maven2 Repository" at "http://fluentd.org/maven2/"
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
**The current version is 0.1.0.**

## Quickstart

### Small example with a client library for Treasure Data Cloud

The following program is a small example of td-client.

under construction...

## License

Apache License, Version 2.0
