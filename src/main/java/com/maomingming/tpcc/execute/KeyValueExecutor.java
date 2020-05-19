package com.maomingming.tpcc.execute;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;
import com.maomingming.tpcc.util.Constant;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyValueExecutor implements Executor {
    Ignite ignite;
    HashMap<String, IgniteCache<String, Record>> caches = new HashMap<>();

    Transaction currentTxn;

    public KeyValueExecutor() {
        Ignition.setClientMode(true);
        IgniteConfiguration conf = new IgniteConfiguration();
        conf.setIgniteInstanceName("CLIENT");
        this.ignite = Ignition.getOrStart(conf);
        for (String table : Constant.TABLES) {
            caches.put(table, this.ignite.getOrCreateCache(table));
        }
    }

    public void txStart() {
        currentTxn = ignite.transactions().txStart();
    }

    public void txCommit() {
        currentTxn.commit();
        currentTxn.close();
    }

    public void txRollback() {
        currentTxn.rollback();
        currentTxn.close();
    }

    public Record findOne(String tableName,
                          List<String> selectColumn,
                          Map<String, Object> key) {
        List<Record> rs =  find(tableName, selectColumn, key, null, null, null);
        if (rs == null || rs.size() == 0)
            return null;
        return rs.get(0);
    }

    public void insert(String tableName, Record r) {
        caches.get(tableName).put(r.getKey(), r);
    }
    public void update(String tableName, List<String> selectColumn, Record r) {
        caches.get(tableName).put(r.getKey(), r);
    }

    @SuppressWarnings("unchecked")
    public List<Record> find(String tableName,
                             List<String> selectColumn,
                             Map<String, Object> key,
                             Map<String, Object[]> keys,
                             Map<String, Object> equalFilter,
                             String sortBy) {

        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        IgniteCache<String, Record> cache = caches.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);
            Method keyMethod = recordClass.getMethod("getKeys", Map.class, Map.class);
            Set<String> stringKeys = (Set<String>) keyMethod.invoke(null, key, keys);
            Stream<Record> res = cache.getAll(stringKeys).values().stream();
            if (equalFilter != null) {
                for (Map.Entry<String, Object> e : equalFilter.entrySet()) {
                    Field field = recordClass.getField(e.getKey());
                    res = res.filter(record -> {
                        try {
                            return Objects.equals(field.get(record), e.getValue());
                        } catch (IllegalAccessException ex) {
                            ex.printStackTrace();
                            return false;
                        }
                    });
                }
            }
            if (sortBy != null) {
                Field field = recordClass.getField(sortBy);
                res = res.sorted((record1, record2) -> {
                    try {
                        return ((Comparable<Object>) field.get(record1)).compareTo(field.get(record2));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            return res.collect(Collectors.toList());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public void executeFinish() {
        for (String table : Constant.TABLES) {
            this.caches.get(table).close();
        }
        //todo: 所有线程结束再将其关闭
        this.ignite.close();
    }
}
