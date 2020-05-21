package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class NewOrder implements Record, Serializable {
    public int no_o_id;
    public int no_d_id;
    public int no_w_id;

    public String no_data = "none";

    public NewOrder() {}

    public NewOrder(int o_id, int d_id, int w_id) {
        this.no_o_id = o_id;
        this.no_d_id = d_id;
        this.no_w_id = w_id;
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("no_w_id") && key.containsKey("no_d_id") && key.containsKey("no_o_id"))
            return getKey((int) key.get("no_w_id"), (int) key.get("no_d_id"), (int) key.get("no_o_id"));
        return null;
    }

    public static String getKey(int no_w_id, int no_d_id, int no_o_id) {
        return "NO_W_ID=" + no_w_id + "&NO_D_ID=" + no_d_id + "&NO_O_ID" + no_o_id;
    }

    public String getKey() {
        return getKey(this.no_w_id, this.no_d_id, this.no_o_id);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("no_w_id", no_w_id)
                .put("no_d_id", no_d_id)
                .put("no_o_id", no_o_id)
                .build();
    }
}
