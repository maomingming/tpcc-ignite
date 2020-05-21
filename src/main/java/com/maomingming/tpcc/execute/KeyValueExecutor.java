package com.maomingming.tpcc.execute;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.TransactionRetryException;
import com.maomingming.tpcc.param.Projection;
import com.maomingming.tpcc.param.Query;
import com.maomingming.tpcc.param.Update;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;
import com.maomingming.tpcc.util.Constant;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.transactions.Transaction;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyValueExecutor {
    Ignite ignite;
    HashMap<String, IgniteCache<String, Record>> caches = new HashMap<>();

    Transaction currentTxn;

    public KeyValueExecutor() {
        Ignition.setClientMode(true);
        IgniteConfiguration conf = new IgniteConfiguration();
        conf.setIgniteInstanceName("CLIENT");
        conf.setPeerClassLoadingEnabled(true);
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

    @SuppressWarnings("unchecked")
    public Record findOne(String tableName,
                          Query query,
                          Projection projection) {
        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        IgniteCache<String, Record> cache = caches.get(tableName);
        try {
            Class<?> recordClass = Class.forName(recordName);

            // try to get by key
            Method keyMethod = recordClass.getMethod("getKey", Map.class);
            Object keyString = keyMethod.invoke(null, query.equal);
            if (keyString != null)
                return cache.get((String) keyString);

            IgniteCache<String, BinaryObject> binaryCache = cache.withKeepBinary();
            IgniteBiPredicate<String, BinaryObject> filter = getPredicate(query);
            List<BinaryObject> binaryRes = binaryCache.query(new ScanQuery<>(filter), Cache.Entry::getValue).getAll();

            if (projection.sortBy != null) {
                binaryRes.sort((o1, o2) -> {
                    if (projection.asc.equals("ASC"))
                        return ((Comparable<Object>) o1.field(projection.sortBy)).compareTo(o2.field(projection.sortBy));
                    else
                        return ((Comparable<Object>) o2.field(projection.sortBy)).compareTo(o1.field(projection.sortBy));
                });
            }
            if (binaryRes.isEmpty())
                return null;
            int index=0;
            if (projection.loc.equals("MID"))
                index = binaryRes.size()/2;
            return binaryToRecord(binaryRes.get(index), projection);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void update(String tableName,
                       Query query,
                       Update update) throws TransactionRetryException{
        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
        IgniteCache<String, BinaryObject> cache = caches.get(tableName).withKeepBinary();
        try {
            Class<?> recordClass = Class.forName(recordName);
            Method keyMethod = recordClass.getMethod("getKey", Map.class);
            String keyString = (String) keyMethod.invoke(null, query.equal);
            try {
                cache.invoke(keyString, (CacheEntryProcessor<String, BinaryObject, Object>) (entry, objects) -> {
                    BinaryObjectBuilder builder = entry.getValue().toBuilder();
                    if (update.intIncrement != null) {
                        for (Map.Entry<String, Integer> e : update.intIncrement.entrySet()) {
                            builder.setField(e.getKey(), (int) builder.getField(e.getKey()) + e.getValue());
                        }
                    }
                    if (update.decimalIncrement != null) {
                        for (Map.Entry<String, BigDecimal> e : update.decimalIncrement.entrySet()) {
                            builder.setField(e.getKey(), ((BigDecimal) builder.getField(e.getKey())).add(e.getValue()));
                        }
                    }
                    if (update.replace != null) {
                        for (Map.Entry<String, Object> e : update.replace.entrySet()) {
                            builder.setField(e.getKey(), e.getValue());
                        }
                    }
                    entry.setValue(builder.build());
                    return null;
                });
            } catch (CacheException e) {
                if (currentTxn.isRollbackOnly()) {
                    txRollback();
                    throw new TransactionRetryException();
                }
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

    }


    public void insert(String tableName, Record r) {
        caches.get(tableName).put(r.getKey(), r);
    }

    IgniteBiPredicate<String, BinaryObject> getPredicate(Query query) {
        return (IgniteBiPredicate<String, BinaryObject>) (s, record) -> {
            for (Map.Entry<String, Object> e : query.equal.entrySet()) {
                if (!Objects.equals(record.field(e.getKey()), e.getValue()))
                    return false;
            }
            return true;
        };
    }

    static Record binaryToRecord(BinaryObject binaryObject, Projection projection) {
        Record r = null;
        try {
            r = (Record) projection.recordClass.newInstance();
            for (Map.Entry<String, Field> e : projection.colMap.entrySet()) {
                e.getValue().set(r, binaryObject.field(e.getKey()));
            }
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return r;
    }

    public List<Record> find(String tableName,
                             Query query,
                             Projection projection) {
        IgniteCache<String, BinaryObject> cache = caches.get(tableName).withKeepBinary();
        IgniteBiPredicate<String, BinaryObject> filter = getPredicate(query);
        List<BinaryObject> binaryRes = cache.query(new ScanQuery<>(filter), Cache.Entry::getValue).getAll();
        return binaryRes.stream().map(o -> binaryToRecord(o, projection)).collect(Collectors.toList());
    }
//
//    @SuppressWarnings("unchecked")
//    public List<Record> find(String tableName,
//                             Query query,
//                             Projection projection) {
//
//        String recordName = "com.maomingming.tpcc.record." + Constant.tableToRecord.get(tableName);
//        IgniteCache<String, Record> cache = caches.get(tableName);
//        try {
//            Class<?> recordClass = Class.forName(recordName);
//            IgniteBiPredicate<String, Record> filter = (IgniteBiPredicate<String, Record>) (s, record) -> {
//                for (Map.Entry<String, Object> e : query.equal.entrySet()) {
//                    try {
//                        Field field = recordClass.getField(e.getKey());
//                        if (!Objects.equals(field.get(record), e.getValue()))
//                            return false;
//                    } catch (IllegalAccessException | NoSuchFieldException ex) {
//                        ex.printStackTrace();
//                        return false;
//                    }
//                }
//                return true;
//            };
//            List<Cache.Entry<String, Record>> rawRes = cache.query(new ScanQuery<>(filter)).getAll();
//            Stream<Record> res = rawRes.stream().map(Cache.Entry::getValue);
////            if (sortBy != null) {
////                Field field = recordClass.getField(sortBy);
////                res = res.sorted((record1, record2) -> {
////                    try {
////                        return ((Comparable<Object>) field.get(record1)).compareTo(field.get(record2));
////                    } catch (IllegalAccessException e) {
////                        throw new RuntimeException(e);
////                    }
////                });
////            }
//            return res.collect(Collectors.toList());
//        } catch (ClassNotFoundException | NoSuchFieldException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void executeFinish() {
        for (String table : Constant.TABLES) {
            this.caches.get(table).close();
        }
        //todo: 所有线程结束再将其关闭
        this.ignite.close();
    }
}
