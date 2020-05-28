package com.maomingming.tpcc.driver;

import com.maomingming.tpcc.TransactionRetryException;
import com.maomingming.tpcc.param.*;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.util.Constant;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.transactions.Transaction;

import javax.cache.Cache;
import javax.cache.CacheException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class KeyValueDriver implements Driver {
    Ignite ignite;
    HashMap<String, IgniteCache<String, Record>> caches = new HashMap<>();
    HashMap<String, IgniteDataStreamer<String, Record>> stmrs = new HashMap<>();

    public void loadStart() {
        Ignition.setClientMode(true);
        this.ignite = Ignition.start("config/snapshot.xml");
        for (String table : Constant.TABLES) {
            caches.put(table, this.ignite.getOrCreateCache(table));
            stmrs.put(table, this.ignite.dataStreamer(table));
        }
    }

    public void load(String tableName, Record r) {
        this.stmrs.get(tableName).addData(r.getKey(), r);
    }

    public void loadFinish() {
        for (String table : Constant.TABLES) {
            this.stmrs.get(table).close();
            this.caches.get(table).close();
        }
        this.ignite.close();
    }

    Transaction currentTxn;

    public void runtimeStart() {
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
        try {
            currentTxn.rollback();
            currentTxn.close();
        } catch (IgniteException e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("unchecked")
    public Record findOne(String tableName,
                          Query query,
                          Projection projection) {
        Class<?> recordClass = Constant.tableToRecord.get(tableName);
        IgniteCache<String, Record> cache = caches.get(tableName);
        try {
            // try to get by key
            Method keyMethod = recordClass.getMethod("getKey", Map.class);
            Object keyString = keyMethod.invoke(null, query.equal);
            if (keyString != null)
                return cache.get((String) keyString);

            IgniteCache<String, BinaryObject> binaryCache = cache.withKeepBinary();
            IgniteBiPredicate<String, BinaryObject> filter = getPredicate(query);
            List<Cache.Entry<String, BinaryObject>> entryRes = binaryCache.query(new ScanQuery<>(filter)).getAll();
            List<BinaryObject> binaryRes = entryRes.stream().map(Cache.Entry::getValue).collect(Collectors.toList());

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
            int index = 0;
            if (projection.loc.equals("MID"))
                index = binaryRes.size() / 2;
            return binaryToRecord(binaryRes.get(index), projection);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void update(String tableName,
                       Query query,
                       Update update) throws TransactionRetryException {
        Class<?> recordClass = Constant.tableToRecord.get(tableName);
        IgniteCache<String, BinaryObject> cache = caches.get(tableName).withKeepBinary();
        try {
            Method keyMethod = recordClass.getMethod("getKey", Map.class);
            String keyString = (String) keyMethod.invoke(null, query.equal);
            List<String> keyList;
            if (keyString == null) {
                IgniteCache<String, BinaryObject> binaryCache = cache.withKeepBinary();
                IgniteBiPredicate<String, BinaryObject> filter = getPredicate(query);
                keyList = binaryCache.query(new ScanQuery<>(filter), Cache.Entry::getKey).getAll();
            } else {
                keyList = Collections.singletonList(keyString);
            }
            for (String key : keyList) {
                try {
                    cache.invoke(key, (CacheEntryProcessor<String, BinaryObject, Object>) (entry, objects) -> {
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
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
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
            if (query.in != null) {
                for (Map.Entry<String, Set<Integer>> e : query.in.entrySet()) {
                    if (!e.getValue().contains((Integer) record.field(e.getKey())))
                        return false;
                }
            }
            if (query.lessThan != null) {
                for (Map.Entry<String, Comparable<?>> e : query.lessThan.entrySet()) {
                    if (e.getValue().compareTo(record.field(e.getKey())) <= 0)
                        return false;
                }
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
        List<Cache.Entry<String, BinaryObject>> entryRes = cache.query(new ScanQuery<>(filter)).getAll();
        List<BinaryObject> binaryRes = entryRes.stream().map(Cache.Entry::getValue).collect(Collectors.toList());
        return binaryRes.stream().map(o -> binaryToRecord(o, projection)).collect(Collectors.toList());
    }

    public void delete(String tableName,
                       Query query) throws TransactionRetryException {
        Class<?> recordClass = Constant.tableToRecord.get(tableName);
        IgniteCache<String, Record> cache = caches.get(tableName);
        try {
            // delete by key
            Method keyMethod = recordClass.getMethod("getKey", Map.class);
            String keyString = (String) keyMethod.invoke(null, query.equal);
            try {
                cache.remove(keyString);
            } catch (CacheException e) {
                if (currentTxn.isRollbackOnly()) {
                    txRollback();
                    throw new TransactionRetryException();
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public Object aggregation(String tableName,
                              Query query,
                              Aggregation aggregation) {
        IgniteCache<String, BinaryObject> cache = caches.get(tableName).withKeepBinary();
        IgniteBiPredicate<String, BinaryObject> filter = getPredicate(query);
        List<Cache.Entry<String, BinaryObject>> entryRes = cache.query(new ScanQuery<>(filter)).getAll();
        List<BinaryObject> binaryRes = entryRes.stream().map(Cache.Entry::getValue).collect(Collectors.toList());
        if (aggregation.aggregationType.equals("SUM") && aggregation.dataType.equals("DECIMAL")) {
            BigDecimal sum = BigDecimal.valueOf(0);
            for (BinaryObject o : binaryRes) {
                sum = sum.add(o.field(aggregation.column));
            }
            return sum;
        }
        if (aggregation.aggregationType.equals("COUNT")) {
            return (long) binaryRes.size();
        }
        return null;
    }

    public void runtimeFinish() {
        for (String table : Constant.TABLES) {
            this.caches.get(table).close();
        }
        //todo: 所有线程结束再将其关闭
        this.ignite.close();
    }
}
