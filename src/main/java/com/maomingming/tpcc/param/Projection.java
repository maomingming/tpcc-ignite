package com.maomingming.tpcc.param;

import com.maomingming.tpcc.util.Constant;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Projection {
    public Class<?> recordClass;
    public List<String> selectColumn;
    public String sortBy;
    public String asc = "ASC";
    public String loc = "FIRST";

    public Map<String, Field> colMap;

    public Projection(String recordName, List<String> selectColumn) {
        recordName = "com.maomingming.tpcc.record." + recordName;
        try {
            recordClass = Class.forName(recordName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        this.selectColumn = selectColumn;
        colMap = selectColumn.stream().collect(Collectors.toMap(s -> s, s -> {
            try {
                return recordClass.getField(s);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }));
    }

    public Projection(String recordName, List<String> selectColumn, String sortBy) {
        this(recordName, selectColumn);
        this.sortBy = sortBy;
    }

    public Projection(String recordName, List<String> selectColumn, String sortBy, String asc, String loc) {
        this(recordName, selectColumn, sortBy);
        assert asc.equals("ASC") || asc.equals("DESC");
        assert loc.equals("FIRST") || loc.equals("MID");
        this.asc = asc;
        this.loc = loc;
    }
}
