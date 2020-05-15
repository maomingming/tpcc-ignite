package com.maomingming.tpcc.execute;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.record.Record;
import org.apache.ignite.IgniteCache;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query {

    static Map<String, String> tableToRecord = ImmutableMap.<String, String>builder()
            .put("CUSTOMER", "CustRecord")
            .put("DISTRICT", "DistRecord")
            .build();

    static public Record findOne(String tableName,
                                 IgniteCache<String, ? extends Record> cache,
                                 Map<String, Object> key) {
        return find(tableName, cache, key, null, null, null).get(0);
    }

    @SuppressWarnings("unchecked")
    static public List<? extends Record> find(String tableName,
                            IgniteCache<String, ? extends Record> cache,
                            Map<String, Object> key,
                            Map<String, Object[]> keys,
                            Map<String, Object> equalFilter,
                            String sortBy) {

        String recordName = "com.maomingming.tpcc.record." + tableToRecord.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);
            Method keyMethod = recordClass.getMethod("getKeys", Map.class, Map.class);
            Set<String> stringKeys = (Set<String>)keyMethod.invoke(null, key, keys);
            Stream<? extends Record> res = cache.getAll(stringKeys).values().stream();
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
                        return ((Comparable<Object>)field.get(record1)).compareTo(field.get(record2));
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
}
