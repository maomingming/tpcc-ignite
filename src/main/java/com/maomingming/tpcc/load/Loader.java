package com.maomingming.tpcc.load;

import com.maomingming.tpcc.record.Record;

public interface Loader {
    void load(String tableName, Record r);
    default void loadFinish(){}
}
