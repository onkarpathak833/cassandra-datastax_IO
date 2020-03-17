package com.example.cassandra.util;

import com.jcabi.xml.XML;
import com.jcabi.xml.XMLDocument;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InsertQueryGenerator {

    public static List < String > queryStrings = new ArrayList <> ( );

    //Read XML file into Java String
    private static String readXmlContent ( String filePath ) throws FileNotFoundException {
        File file = new File ( filePath );
        XML xml = new XMLDocument ( file );
        String xmlString = xml.toString ( );
        return xmlString;
    }

    //Generate INSERT query for required no. of rows from argument, keep xml string content same
    public static List < String > generateRandomQueries ( int params , String filePath ) throws FileNotFoundException {

        String xmlString = readXmlContent ( filePath );
        Random random = new Random ( );
        for ( int i = 0 ; i < params ; i++ ) {
            long randomLong = random.nextLong ( );
            String deliveryAgreementId = String.valueOf ( randomLong );
            String insertQuery = "INSERT INTO ISOM.ISOM_DOCUMENT (document_id, document_text) values (" + "'" + deliveryAgreementId + "'" + "," + "'" + xmlString + "'" + ")";
            queryStrings.add ( insertQuery );
        }

        return queryStrings;

    }

}
