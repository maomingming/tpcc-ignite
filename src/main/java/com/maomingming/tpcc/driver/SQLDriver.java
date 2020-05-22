package com.maomingming.tpcc.driver;

import com.maomingming.tpcc.TransactionRetryException;
import com.maomingming.tpcc.param.*;
import com.maomingming.tpcc.record.Record;
import com.maomingming.tpcc.util.Constant;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

public class SQLDriver implements Driver {

    Connection conn;
    String warehouse = "CREATE TABLE WAREHOUSE (" +
            "W_ID SMALLINT NOT NULL," +
            "W_NAME VARCHAR(10)," +
            "W_STREET_1 VARCHAR(20)," +
            "W_STREET_2 VARCHAR(20)," +
            "W_CITY VARCHAR(20)," +
            "W_STATE CHAR(2)," +
            "W_ZIP CHAR(9)," +
            "W_TAX DECIMAL(4,4)," +
            "W_YTD DECIMAL(12,2)," +
            "PRIMARY KEY (W_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String district = "CREATE TABLE DISTRICT (" +
            "D_ID TINYINT NOT NULL," +
            "D_W_ID SMALLINT NOT NULL," +
            "D_NAME VARCHAR(10)," +
            "D_STREET_1 VARCHAR(20)," +
            "D_STREET_2 VARCHAR(20)," +
            "D_CITY VARCHAR(20)," +
            "D_STATE CHAR(2)," +
            "D_ZIP CHAR(9)," +
            "D_TAX DECIMAL(4,4)," +
            "D_YTD DECIMAL(12,2)," +
            "D_NEXT_O_ID INT," +
            "PRIMARY KEY (D_W_ID, D_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String customer = "CREATE TABLE CUSTOMER (" +
            "C_ID INT NOT NULL," +
            "C_D_ID TINYINT NOT NULL," +
            "C_W_ID SMALLINT NOT NULL," +
            "C_FIRST VARCHAR(16)," +
            "C_MIDDLE CHAR(2)," +
            "C_LAST VARCHAR(16)," +
            "C_STREET_1 VARCHAR(20)," +
            "C_STREET_2 VARCHAR(20)," +
            "C_CITY VARCHAR(20)," +
            "C_STATE CHAR(2)," +
            "C_ZIP CHAR(9)," +
            "C_PHONE CHAR(16)," +
            "C_SINCE DATETIME," +
            "C_CREDIT CHAR(2)," +
            "C_CREDIT_LIM DECIMAL(12,2)," +
            "C_DISCOUNT DECIMAL(4,4)," +
            "C_BALANCE DECIMAL(12,2)," +
            "C_YTD_PAYMENT DECIMAL(12,2)," +
            "C_PAYMENT_CNT SMALLINT," +
            "C_DELIVERY_CNT SMALLINT," +
            "C_DATA VARCHAR(500)," +
            "PRIMARY KEY(C_W_ID, C_D_ID, C_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String history = "CREATE TABLE HISTORY (" +
            "H_ID INT," +
            "H_C_ID INT," +
            "H_C_D_ID TINYINT," +
            "H_C_W_ID SMALLINT," +
            "H_D_ID TINYINT," +
            "H_W_ID SMALLINT," +
            "H_DATE DATETIME," +
            "H_AMOUNT DECIMAL(6,2)," +
            "H_DATA VARCHAR(24)," +
            "PRIMARY KEY(H_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String newOrder = "CREATE TABLE NEW_ORDER (" +
            "NO_O_ID INT NOT NULL," +
            "NO_D_ID TINYINT NOT NULL," +
            "NO_W_ID SMALLINT NOT NULL," +
            "NO_DATA CHAR(4)," +
            "PRIMARY KEY(NO_W_ID, NO_D_ID, NO_O_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String order = "CREATE TABLE \"ORDER\" (" +
            "O_ID INT NOT NULL," +
            "O_D_ID TINYINT NOT NULL," +
            "O_W_ID SMALLINT NOT NULL," +
            "O_C_ID INT," +
            "O_ENTRY_D DATETIME," +
            "O_CARRIER_ID TINYINT," +
            "O_OL_CNT TINYINT," +
            "O_ALL_LOCAL TINYINT," +
            "PRIMARY KEY(O_W_ID, O_D_ID, O_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String orderLine = "CREATE TABLE ORDER_LINE (" +
            "OL_O_ID INT NOT NULL," +
            "OL_D_ID TINYINT NOT NULL," +
            "OL_W_ID SMALLINT NOT NULL," +
            "OL_NUMBER TINYINT NOT NULL," +
            "OL_I_ID INT," +
            "OL_SUPPLY_W_ID SMALLINT," +
            "OL_DELIVERY_D DATETIME," +
            "OL_QUANTITY TINYINT," +
            "OL_AMOUNT DECIMAL(6,2)," +
            "OL_DIST_INFO CHAR(24)," +
            "PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String item = "CREATE TABLE ITEM (" +
            "I_ID INT NOT NULL," +
            "I_IM_ID INT," +
            "I_NAME VARCHAR(24)," +
            "I_PRICE DECIMAL(5,2)," +
            "I_DATA VARCHAR(50)," +
            "PRIMARY KEY(I_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    String stock = "CREATE TABLE STOCK (" +
            "S_I_ID INT NOT NULL," +
            "S_W_ID SMALLINT NOT NULL," +
            "S_QUANTITY SMALLINT," +
            "S_DIST_01 CHAR(24)," +
            "S_DIST_02 CHAR(24)," +
            "S_DIST_03 CHAR(24)," +
            "S_DIST_04 CHAR(24)," +
            "S_DIST_05 CHAR(24)," +
            "S_DIST_06 CHAR(24)," +
            "S_DIST_07 CHAR(24)," +
            "S_DIST_08 CHAR(24)," +
            "S_DIST_09 CHAR(24)," +
            "S_DIST_10 CHAR(24)," +
            "S_YTD INT," +
            "S_ORDER_CNT SMALLINT," +
            "S_REMOTE_CNT SMALLINT," +
            "S_DATA VARCHAR(50)," +
            "PRIMARY KEY(S_W_ID, S_I_ID)" +
            ") WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    public void loadStart() throws Exception{
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(warehouse);
            stmt.executeUpdate(district);
            stmt.executeUpdate(customer);
            stmt.executeUpdate(history);
            stmt.executeUpdate(newOrder);
            stmt.executeUpdate(order);
            stmt.executeUpdate(orderLine);
            stmt.executeUpdate(item);
            stmt.executeUpdate(stock);
        }
    }

    public void load(String tableName, Record r) {
        insert(tableName, r);
    }

    public void loadFinish() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void runtimeStart() throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    public void txStart() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void txCommit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void txRollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Record findOne(String tableName,
                          Query query,
                          Projection projection) {
        StringBuilder sql = new StringBuilder("select ");
        sql.append(String.join(", ", projection.selectColumn)).append(" from \"").append(tableName).append("\" ");
        sql.append(getWhereString(query));
        if (projection.sortBy != null) {
            sql.append(" order by ").append(projection.sortBy);
            if (projection.asc.equals("DESC"))
                sql.append(" desc ");
        }
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            Integer index = 1;
            prepareWhere(stmt, index, query);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next())
                return null;
            if (projection.loc.equals("FIRST"))
                return rsToRecord(rs, projection);
            ArrayList<Record> res = new ArrayList<>();
            res.add(rsToRecord(rs, projection));
            while (rs.next()) {
                res.add(rsToRecord(rs, projection));
            }
            return res.get(res.size() / 2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    String getWhereString(Query query) {
        Stream<String> whereStream = Stream.empty();
        if (query.equal != null) {
            Stream<String> equalStream = query.equal.keySet().stream().map(k -> k + " = ?");
            whereStream = Stream.concat(whereStream, equalStream);
        }
        if (query.in != null) {
            Stream<String> inStream = query.in.entrySet().stream().map(e -> e.getKey() + " in (" +
                    String.join(", ", Collections.nCopies(e.getValue().size(), "?")) + ")");
            whereStream = Stream.concat(whereStream, inStream);
        }
        if (query.lessThan != null) {
            Stream<String> lessThanStream = query.lessThan.keySet().stream().map(k -> k + " < ?");
            whereStream = Stream.concat(whereStream, lessThanStream);
        }

        String[] whereExpr = whereStream.toArray(String[]::new);
        return " where " + String.join(" and ", whereExpr);
    }

    void prepareWhere(PreparedStatement stmt, Integer index, Query query) throws SQLException {
        if (query.equal != null)
            for (Object obj : query.equal.values())
                stmt.setObject(index++, obj);
        if (query.in != null)
            for (Set<Integer> set : query.in.values())
                for (Object obj : set)
                    stmt.setObject(index++, obj);
        if (query.lessThan != null)
            for (Object obj : query.lessThan.values())
                stmt.setObject(index++, obj);
    }

    Record rsToRecord(ResultSet rs, Projection projection) {
        Record r = null;
        try {
            r = (Record) projection.recordClass.newInstance();
            for (Map.Entry<String, Field> e : projection.colMap.entrySet()) {
                e.getValue().set(r, rs.getObject(e.getKey()));
            }
        } catch (InstantiationException | IllegalAccessException | SQLException e) {
            e.printStackTrace();
        }
        return r;
    }


    public List<Record> find(String tableName,
                             Query query,
                             Projection projection) {
        String sql = "select " + String.join(", ", projection.selectColumn) + " from \"" + tableName + "\" " +
                getWhereString(query);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            Integer index = 1;
            prepareWhere(stmt, index, query);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Record> res = new ArrayList<>();
            while (rs.next()) {
                res.add(rsToRecord(rs, projection));
            }
            return res;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void update(String tableName,
                       Query query,
                       Update update) throws TransactionRetryException {
        StringBuilder sql = new StringBuilder("update \"" + tableName + "\" set ");
        Stream<String> setStream = Stream.empty();
        if (update.intIncrement != null) {
            Stream<String> incStream = update.intIncrement.keySet().stream().map(k -> k + " = " + k + " + ?");
            setStream = Stream.concat(setStream, incStream);
        }
        if (update.decimalIncrement != null) {
            Stream<String> incStream = update.decimalIncrement.keySet().stream().map(k -> k + " = " + k + " + ?");
            setStream = Stream.concat(setStream, incStream);
        }
        if (update.replace != null) {
            Stream<String> replaceStream = update.replace.keySet().stream().map(k -> k + " = ?");
            setStream = Stream.concat(setStream, replaceStream);
        }

        String[] setExpr = setStream.toArray(String[]::new);
        sql.append(String.join(", ", setExpr));

        sql.append(getWhereString(query));

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            Integer index = 1;
            if (update.intIncrement != null)
                for (Object obj : update.intIncrement.values())
                    stmt.setObject(index++, obj);
            if (update.decimalIncrement != null)
                for (Object obj : update.decimalIncrement.values())
                    stmt.setObject(index++, obj);
            if (update.replace != null)
                for (Object obj : update.replace.values())
                    stmt.setObject(index++, obj);
            prepareWhere(stmt, index, query);
            stmt.executeUpdate();
        } catch (SQLException e) {
//            System.out.println(e.getSQLState());
            if ("40001".equals(e.getSQLState())) {
                // retry the transaction
                txRollback();
                throw new TransactionRetryException();
            } else {
                e.printStackTrace();
            }
        }
    }

    public void insert(String tableName, Record r) {
        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);
            Field[] fields = recordClass.getFields();
            String sql = "INSERT INTO \"" + tableName + "\" VALUES (" + String.join(", ", Collections.nCopies(fields.length, "?")) + ")";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < fields.length; ++i) {
                    stmt.setObject(i + 1, fields[i].get(r));
                }
                stmt.executeUpdate();
            }
        } catch (ClassNotFoundException | SQLException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void delete(String tableName,
                       Query query) throws TransactionRetryException {
        String sql = "delete from  \"" + tableName + "\" " + getWhereString(query);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            Integer index = 1;
            prepareWhere(stmt, index, query);
            stmt.executeUpdate();
        } catch (SQLException e) {
            if ("40001".equals(e.getSQLState())) {
                // retry the transaction
                txRollback();
                throw new TransactionRetryException();
            } else {
                e.printStackTrace();
            }
        }
    }

    public Object aggregation(String tableName,
                              Query query,
                              Aggregation aggregation) {
        StringBuilder sql = new StringBuilder("select ");
        if (aggregation.aggregationType.equals("SUM") && aggregation.dataType.equals("DECIMAL")) {
            sql.append("sum( ").append(aggregation.column).append(")");
        }
        if (aggregation.aggregationType.equals("COUNT")) {
            sql.append("count(*)");
        }
        sql.append(" from \"").append(tableName).append("\" ");

        sql.append(getWhereString(query));

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            Integer index = 1;
            prepareWhere(stmt, index, query);
            ResultSet rs = stmt.executeQuery();
            if (rs.next())
                return rs.getObject(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void runtimeFinish() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
