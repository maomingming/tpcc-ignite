package com.maomingming.tpcc.param;

import java.math.BigDecimal;
import java.util.Map;

public class Update {
    public Map<String, Integer> intIncrement;
    public Map<String, BigDecimal> decimalIncrement;
    public Map<String, Object> replace;

    public Update(Map<String, Integer> intIncrement) {
        this.intIncrement = intIncrement;
    }

    public Update(Map<String, Integer> intIncrement, Map<String, BigDecimal> decimalIncrement) {
        this.intIncrement = intIncrement;
        this.decimalIncrement = decimalIncrement;
    }

    public Update(Map<String, Integer> intIncrement, Map<String, BigDecimal> decimalIncrement, Map<String, Object> replace) {
        this.intIncrement = intIncrement;
        this.decimalIncrement = decimalIncrement;
        this.replace = replace;
    }
}
