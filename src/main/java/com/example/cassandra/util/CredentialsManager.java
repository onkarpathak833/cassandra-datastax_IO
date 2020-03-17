package com.example.cassandra.util;

import com.google.auth.oauth2.GoogleCredentials;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CredentialsManager {

    //Load Google Service Account from Service Account JSON key
    public static GoogleCredentials loadGoogleCredentials() throws IOException {
        InputStream serviceAccount = new FileInputStream ("/Users/onkar/Documents/Firestore_ServiceAccountKey.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
        return  credentials;
    }


}
