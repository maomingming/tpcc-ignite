package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.Record;
import com.maomingming.tpcc.util.Constant;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
                          List<String> selectColumn,
                          Map<String, Object> key) {
        List<Record> rs =  find(tableName, selectColumn, key, null, null, null);
        if (rs == null || rs.size() == 0)
            return null;
        return rs.get(0);
    }

    public List<Record> find(String tableName,
                             List<String> selectColumn,
                             Map<String, Object> key,
                             Map<String, Object[]> keys,
                             Map<String, Object> equalFilter,
                             String sortBy) {
        StringBuilder sql = new StringBuilder("select ");
        sql.append(String.join(", ", Collections.nCopies(selectColumn.size(), "?"))).append(" from \"").append(tableName).append("\" ");
        boolean first = true;
        Stream<String> whereStream = Stream.empty();
        if (key != null) {
            Stream<String> keyStream = key.keySet().stream().map(kk -> "(" + kk + " = ?)");
            whereStream = Stream.concat(whereStream, keyStream);
        }
        if (keys != null) {
            Stream<String> keysStream = keys.entrySet().stream().map(kse -> "(" + kse.getKey() + " in (" +
                    String.join(", ", Collections.nCopies(kse.getValue().length, "?")) + ")");
            whereStream = Stream.concat(whereStream, keysStream);
        }
        if (equalFilter != null) {
            Stream<String> equalStream = equalFilter.keySet().stream().map(ek -> "(" + ek + " = ?)");
            whereStream = Stream.concat(whereStream, equalStream);
        }

        String[] whereExpr = whereStream.toArray(String[]::new);
        if (whereExpr.length > 0)
            sql.append("where ").append(String.join(" and ", whereExpr));

        if (sortBy != null)
            sql.append("order by ").append(sortBy);

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            int index = 1;
            if (key != null)
                for (Object obj : key.values())
                    stmt.setObject(index++, obj);
            if (keys != null)
                for (Object[] objects : keys.values())
                    for (Object obj : objects)
                        stmt.setObject(index++, obj);
            if (equalFilter != null)
                for (Object obj : equalFilter.values())
                    stmt.setObject(index++, obj);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Record> res = new ArrayList<>();
            String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
            Class<?> recordClass = Class.forName(recordName);
            while (rs.next()) {
                Record r = (Record) recordClass.newInstance();
                for (String col : selectColumn) {
                    Field field = recordClass.getField(col);
                    field.set(r, rs.getObject(col));
                }
                res.add(r);
            }
            return res;
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void update(String tableName, List<String> selectColumn, Record r) {
        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);

            StringBuilder sql = new StringBuilder("update \"" + tableName + "\" set ");
            String[] setExpr = selectColumn.stream().map(x -> x + " = ?").toArray(String[]::new);
            sql.append(String.join(", ", setExpr));

            sql.append(" where ");
            Map<String, Object> keyMap = r.getKeyMap();
            String[] whereExpr = keyMap.keySet().stream().map(x -> x + " = ?").toArray(String[]::new);
            sql.append(String.join(" and ", whereExpr));

            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                int index = 1;
                for (String col : selectColumn) {
                    Field field = recordClass.getField(col);
                    stmt.setObject(index++, field.get(r));
                }
                for (Object obj : keyMap.values())
                    stmt.setObject(index++, obj);
                stmt.executeUpdate();
            }
        } catch (ClassNotFoundException | SQLException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
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

    public void executeFinish() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
