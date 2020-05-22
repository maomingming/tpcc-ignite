package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.TransactionRetryException;
import com.maomingming.tpcc.param.*;
import com.maomingming.tpcc.record.Record;
import com.maomingming.tpcc.util.Constant;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

public class SQLExecutor implements Executor {

    Connection conn;

    public SQLExecutor() throws Exception {
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
                e.printStackTrace();
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

    public void executeFinish() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
