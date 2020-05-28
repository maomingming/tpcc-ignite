package com.maomingming.tpcc.util;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.record.*;

import java.util.Map;

public class Constant {

    public static String[] TABLES = {"WAREHOUSE", "DISTRICT", "CUSTOMER", "HISTORY",
            "NEW_ORDER", "ORDER", "ORDER_LINE", "ITEM", "STOCK"};

    public static Map<String, Class<?>> tableToRecord = ImmutableMap.<String, Class<?>>builder()
            .put("WAREHOUSE", Warehouse.class)
            .put("DISTRICT", District.class)
            .put("CUSTOMER", Customer.class)
            .put("HISTORY", History.class)
            .put("NEW_ORDER", NewOrder.class)
            .put("ORDER", Order.class)
            .put("ORDER_LINE", OrderLine.class)
            .put("ITEM", Item.class)
            .put("STOCK", Stock.class)
            .build();
}
