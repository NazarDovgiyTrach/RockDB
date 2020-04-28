package com.key2asset.raclette.persistent.cache;

import org.rocksdb.RocksDB;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RaclettePersistentCacheApplication {

    public static void main(String[] args) {
        RocksDB.loadLibrary();
        SpringApplication.run(RaclettePersistentCacheApplication.class, args);
    }

}
