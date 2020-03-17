package com.example.cassandra.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;

public class CassandraDefinitionManager {

    public static CqlSession session = null;
    public static CqlSession createCassandraSession() {

        if(session==null) {


            //Add Cassandra Cluster nodes as and when required
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("localhost", 9042))
//                    .addContactPoint(new InetSocketAddress("35.225.239.203", 9042))
//                    .addContactPoint(new InetSocketAddress("34.70.9.248",9042))
//                    .withLocalDatacenter("us-central1")
                    .withLocalDatacenter("datacenter1")
                    .build();

            return  session;
        }
        return  session;
    }


    public static void createCassandraKeyspace(CqlSession session) {
        String createKeySpaceQuery =  "CREATE KEYSPACE IF NOT EXISTS ISOM\n" +
                "    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};";

        System.out.println("keyspace value : "+session.getKeyspace());

        session.executeAsync(createKeySpaceQuery);
    }


    public static void executeCassandraDDL(CqlSession session, String query) {
        SimpleStatement statement = new SimpleStatementBuilder(query).setTimeout(Duration.ofMinutes(5L)).build();
        session.execute(statement);
    }


}
