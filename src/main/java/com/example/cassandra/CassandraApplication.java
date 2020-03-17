package com.example.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.example.cassandra.dao.CassandraDMLExecutor;
import com.example.cassandra.dao.CassandraDefinitionManager;
import com.example.cassandra.util.InsertQueryGenerator;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.*;

public class CassandraApplication {


    public static CqlSession session = null;


    public static void main ( String[] args ) {

        String noOfRowsToInsert = args[0];
        String filePathToReadContent = args[1];
        int  params  = Integer.valueOf ( noOfRowsToInsert );
        System.out.println ( params );

        //needed when you connect to the Cassandra on Cloud VM
        GoogleCredentials credentials = null;
        try {
//            credentials = loadGoogleCredentials();
        }
        catch (Exception e) {
            e.printStackTrace ( );
        }

        //Create CqlSession using Datastax driver. CqlSession is the entry point to Cassandra
        session = CassandraDefinitionManager.createCassandraSession ( );

        //Create KeySpace in Cassandra if not exists
        CassandraDefinitionManager.createCassandraKeyspace ( session );


        //Create Cassandra Table -- DDL
        String createTableQuery = "CREATE TABLE IF NOT EXISTS ISOM.ISOM_DOCUMENT (" +
                                  "document_id text," +
                                  "document_text text," +
                                  "PRIMARY KEY (document_id)" +
                                  ")";
        CassandraDefinitionManager.executeCassandraDDL ( session , createTableQuery );


        try {

            //Generate no. of records to insert into Cassandra based on 'params' and file path to read file into string
            List < String > queryStrings = InsertQueryGenerator.generateRandomQueries ( params , filePathToReadContent );

            //This is start time to insert all records
            long   startTime    = System.currentTimeMillis ( );
            System.out.println ( " Start Time for Insert : " + startTime + "for no. of Records : " + queryStrings.size ( ) );


            // run all insert queries in Java8 parallel streams
            queryStrings.parallelStream ( ).forEach ( query -> {

                try {
                    CassandraDMLExecutor.executeDMLQuery ( query , session );
                }
                catch (Exception e) {
                    e.printStackTrace ( );
                }

            } );


            //End time once insert is done. This is not async call
            long endTime = System.currentTimeMillis ( );
            System.out.println ( "End Time for Insert : " + endTime );
            double totalTime = (double) (endTime - startTime) / 1000;
            System.out.println ( "Total Time in seconds for inserts : " + totalTime );

        }
        catch (Exception e) {
            e.printStackTrace ( );
        }


        // Sample Select Query to check no. of records inserted from above query
        // Replace this query with your SELECT Query
        String selectTableQuery = "SELECT count(*) from ISOM.ISOM_DOCUMENT";
        try {
            ResultSet resultSet = CassandraDMLExecutor.executeSelectQuery ( session , selectTableQuery );

            System.out.println ( "Total Row Counts : " + resultSet.one ( ).getFormattedContents ( ) );
        }
        catch (Exception e) {
            e.printStackTrace ( );
        }

        //End CqlSession once done.
        session.close ( );

    }


}
