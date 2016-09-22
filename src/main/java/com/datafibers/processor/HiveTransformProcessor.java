package com.datafibers.processor;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/* This is sample transform config
    {
        "topic.for.result":"stock",
        "trans.sql":"SELECT STREAM symbol, name FROM finance"
        "hive.table.for.result":"result1"
    }
*/

public class HiveTransformProcessor {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "vagrant", "");
        Statement stmt = con.createStatement();
        String tableName = "result_table";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table if not exists " + tableName + " as select count(*) from airline");
    }
}

