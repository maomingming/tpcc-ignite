package com.maomingming.tpcc;

public class Coordinator {
    public static void main(String[] args) throws Exception{
        Populator initializer = new Populator("STREAM_LOADER", 2);
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
