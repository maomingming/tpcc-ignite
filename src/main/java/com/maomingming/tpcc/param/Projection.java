package com.maomingming.tpcc.param;

import java.util.List;
import java.util.Map;

public class Projection {
    public List<String> selectColumn;
    public String sortBy;
    public String asc = "ASC";
    public String loc = "FIRST";

    public Projection() {}
    public Projection(List<String> selectColumn) {
        this.selectColumn = selectColumn;
    }
    public Projection(List<String> selectColumn, String sortBy) {
        this.selectColumn = selectColumn;
        this.sortBy = sortBy;
    }

    public Projection(List<String> selectColumn, String sortBy, String asc, String loc) {
        this.selectColumn = selectColumn;
        this.sortBy = sortBy;
        assert asc.equals("ASC") || asc.equals("DESC");
        assert loc.equals("FIRST") || loc.equals("MID");
        this.asc = asc;
        this.loc = loc;
    }
}
