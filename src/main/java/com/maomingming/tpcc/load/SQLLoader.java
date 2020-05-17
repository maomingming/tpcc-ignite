package com.maomingming.tpcc.load;

import com.maomingming.tpcc.record.Customer;
import com.maomingming.tpcc.record.Record;
import com.maomingming.tpcc.util.Constant;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Collections;

public class SQLLoader implements Loader{

    Connection conn;

    String Customer = "CREATE TABLE CUSTOMER (" +
        "C_ID INT NOT NULL, " +
        "C_D_ID INT NOT NULL," +
        "C_W_ID INT NOT NULL, " +
        "C_FIRST VARCHAR(16), " +
        "C_MIDDLE CHAR(2), " +
        "C_LAST VARCHAR(16), " +
        "C_STREET_1 VARCHAR(20), " +
        "C_STREET_2 VARCHAR(20), " +
        "C_CITY VARCHAR(20), " +
        "C_STATE CHAR(2), " +
        "C_ZIP CHAR(9), " +
        "C_PHONE CHAR(16), " +
        "C_SINCE DATETIME, " +
        "C_CREDIT CHAR(2), " +
        "C_CREDIT_LIM DECIMAL(12,2), " +
        "C_DISCOUNT DECIMAL(5,4), " +
        "C_BALANCE DECIMAL(12,2), " +
        "C_YTD_PAYMENT DECIMAL(12,2), " +
        "C_PAYMENT_CNT INT, " +
        "C_DELIVERY_CNT INT, " +
        "C_DATA VARCHAR(500)," +
        "PRIMARY KEY(C_W_ID, C_D_ID, C_ID) " +
    ") ";
    
    public void loadBegin() throws Exception{
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(Customer);
        }
    }

    public void load(String tableName, Record r) {
        System.out.println(((com.maomingming.tpcc.record.Customer)r).c_discount);
        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);
            Field[] fields = recordClass.getFields();
            String sql = "INSERT INTO " + tableName + " VALUES (" + String.join(", ", Collections.nCopies(fields.length, "?")) + ")";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < fields.length; ++i) {
                    stmt.setObject(i+1, fields[i].get(r));
                }
                stmt.executeUpdate();
            }
        } catch (ClassNotFoundException | SQLException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
    public void loadFinish() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
