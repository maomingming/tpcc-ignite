package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.Record;

import java.util.List;
import java.util.Map;

public interface Executor {
    void txStart();

    void txCommit();

    void txRollback();

    Record findOne(String tableName,
                   List<String> selectColumn,
                   Map<String, Object> key);

    List<Record> find(String tableName,
                      List<String> selectColumn,
                      Map<String, Object> key,
                      Map<String, Object[]> keys,
                      Map<String, Object> equalFilter,
                      String sortBy);

    void insert(String tableName, Record r);
    void update(String tableName, List<String> selectColumn, Record r);

    void executeFinish();
}
