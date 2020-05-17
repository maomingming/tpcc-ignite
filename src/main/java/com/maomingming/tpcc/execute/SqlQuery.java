package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.Record;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.util.List;
import java.util.Map;

public class SqlQuery {
    static public List<? extends Record> find(String tableName,
                                              IgniteCache<String, ? extends Record> cache,
                                              Map<String, Object> key,
                                              Map<String, Object[]> keys,
                                              Map<String, Object> equalFilter,
                                              String sortBy) {
        SqlFieldsQuery query = new SqlFieldsQuery("");
        return null;
    }
}
