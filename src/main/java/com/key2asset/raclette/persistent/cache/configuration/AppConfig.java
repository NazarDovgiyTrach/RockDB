package com.key2asset.raclette.persistent.cache.configuration;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Bean
    public Options getOptions() {
        return new Options().setCreateIfMissing(true);
    }

}

