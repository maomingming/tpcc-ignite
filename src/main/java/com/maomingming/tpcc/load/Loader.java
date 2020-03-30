package com.maomingming.tpcc.load;

import com.maomingming.tpcc.record.Record;

public interface Loader {
    String[] TABLES = {"WAREHOUSE", "DISTRICT", "CUSTOMER", "HISTORY",
            "NEW-ORDER", "ORDER", "ORDER-LINE", "ITEM", "STOCK"};
    void load(String tableName, Record r);
    default void loadFinish(){}
}
