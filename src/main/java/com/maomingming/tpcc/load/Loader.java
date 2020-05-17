package com.maomingming.tpcc.load;

import com.maomingming.tpcc.record.Record;

public interface Loader {
    default void loadBegin() throws Exception{}
    void load(String tableName, Record r);
    default void loadFinish(){}
}
