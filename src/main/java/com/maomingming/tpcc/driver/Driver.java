package com.maomingming.tpcc.driver;

import com.maomingming.tpcc.TransactionRetryException;
import com.maomingming.tpcc.param.Aggregation;
import com.maomingming.tpcc.param.Projection;
import com.maomingming.tpcc.param.Query;
import com.maomingming.tpcc.param.Update;
import com.maomingming.tpcc.record.Record;

import java.util.List;

public interface Driver {

    void loadStart() throws Exception;
    void load(String tableName, Record r);
    void loadFinish();

    void runtimeStart() throws Exception;
    void runtimeFinish();

    void txStart();

    void txCommit();

    void txRollback();

    Record findOne(String tableName,
                   Query query,
                   Projection projection);

    List<Record> find(String tableName,
                      Query query,
                      Projection projection);

    void insert(String tableName, Record r);

    void update(String tableName,
                Query query,
                Update update) throws TransactionRetryException;

    void delete(String tableName,
                Query query) throws TransactionRetryException;

    Object aggregation(String tableName,
                       Query query,
                       Aggregation aggregation);

}
