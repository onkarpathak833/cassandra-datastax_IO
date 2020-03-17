# cassandra-datastax_IO
Repository to write and read from Apache Cassandra using Datastax driver

This is a gradle project.
You can import the project into IDE or build a project using gradle task 'customFatJar'
Run Command -> gradle customFatJar
Generated Jar will be executable jar.

Main Class : com.example.cassandra.CassandraApplication

Program takes two arguments :

1. No. of records you want to insert into Localhost Cassandra server (To insert into remote Cassandra server, needs IP address change in code)

2. The Absolute path of XML file whose content will be written to Cassandra Table

Program creates a Keyspace 'ISOM' in Cassandra if not exists
Program creates Table 'ISOM_DOCUMENT' in Keyspace 'ISOM' with Schema :

ISOM_DOCUMENT (
DOCUMENT_ID TEXT,
DOCUMENT_TEXT TEXT,
PRIMARY KEY (DOCUMENT_ID)
);

Run application with two program arguments and observe records inserted in Cassandra with Total time taken to insert those records.
