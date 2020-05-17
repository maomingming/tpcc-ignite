package com.maomingming.tpcc.util;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Constant {

    public static String[] TABLES = {"WAREHOUSE", "DISTRICT", "CUSTOMER", "HISTORY",
            "NEW_ORDER", "ORDER", "ORDER_LINE", "ITEM", "STOCK"};


    public static Map<String, String> tableToRecord = ImmutableMap.<String, String>builder()
            .put("CUSTOMER", "Customer")
            .put("DISTRICT", "District")
            .build();
}
