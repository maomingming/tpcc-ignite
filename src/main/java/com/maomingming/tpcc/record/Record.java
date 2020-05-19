package com.maomingming.tpcc.record;

import java.util.Map;

public interface Record {
    String getKey();
    Map<String, Object> getKeyMap();
}
