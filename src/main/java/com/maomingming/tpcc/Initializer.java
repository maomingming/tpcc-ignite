package com.maomingming.tpcc;

import com.maomingming.tpcc.load.Loader;
import com.maomingming.tpcc.load.StreamLoader;
import com.maomingming.tpcc.record.ItemRecord;

public class Initializer {

    String loaderType;

    public Initializer(String loaderType) {
        this.loaderType = loaderType;
    }

    public void loadAll() throws Exception{
        loadItem();
    }

    private Loader getLoader(String tableName) throws Exception{
        switch (this.loaderType) {
            case "STREAM_LOADER":
                return new StreamLoader(tableName);
            default:
                throw new Exception("Wrong Loader Type.");
        }
    }

    public void loadItem() throws Exception{
        Loader loader = getLoader("ITEM");
        for (int i = 0; i < 1000; i++) {
            loader.load(new ItemRecord(i));
        }
        loader.loadFinish();
    }
}
