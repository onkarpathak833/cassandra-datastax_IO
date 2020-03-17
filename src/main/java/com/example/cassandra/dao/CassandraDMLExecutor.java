package com.example.cassandra.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

public class CassandraDMLExecutor {

    public static void executeDMLQuery ( String query, CqlSession session) {
        session.execute(query);
    }

    public static ResultSet executeSelectQuery(CqlSession session, String selectTableQuery) {
        SimpleStatement statement = new SimpleStatementBuilder (selectTableQuery).build();
        ResultSet resultset = session.execute(statement);
        return resultset;
    }
}
