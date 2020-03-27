package com.maomingming.tpcc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import com.maomingming.tpcc.record.Record;

import javax.swing.*;

public class Coordinator {
    public static void main(String[] args) throws Exception{
        Initializer initializer = new Initializer("STREAM_LOADER");
        initializer.loadAll();
//        Ignition.setClientMode(true);
//        try (Ignite ignite=Ignition.start()) {
//            try (IgniteCache<String, Record> cache=ignite.getOrCreateCache("ITEM")) {
//                for (int i=0;i<100;i++) {
//                    System.out.println(cache.get(Integer.toString(i)));
//                    System.out.println(cache.get(Integer.toString(i)).getKey());
//                }
//            }
//        }
    }
}
