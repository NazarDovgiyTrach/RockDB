package com.key2asset.raclette.persistent.cache.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * It's implementation of {@link KeyValueRepository} which allows perform basic Create, Read, Delete operations using
 * RocksDB library https://github.com/facebook/rocksdb/tree/master/java.
 * To control the behavior of a database it uses configuration from injected Options bean declared in
 * {@link com.key2asset.raclette.persistent.cache.configuration.AppConfig}
 * but there is possibility to change it via {@link #setOptions(Options)}
 *
 *  */
@Repository
public class RocksDBRepositoryImpl implements KeyValueRepository<String, InputStream> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBRepositoryImpl.class);
    private final String dbDir;
    private boolean overwriteExisting;
    private Options options;

    public RocksDBRepositoryImpl(Options options, @Value("${rocksDB.database.path:rocks-db}") String dbDir,
                                 @Value("${rocksDB.overwrite-existing:true}") boolean overwriteExisting) {
        this.options = options;
        this.overwriteExisting = overwriteExisting;
        this.dbDir = dbDir;
    }

    /**
     * This method will store entry into RocksDB within transaction.
     * The isolation level is <b>Read Committed</b>.
     * <br/>If <u>overwriteExisting</u> mode disabled(enable by default) entries will not be stored if passed key exists.
     * */

    public void transactionalSave(String key, InputStream value) {
        try (TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
                TransactionDB transactionDB = TransactionDB.open(options, transactionDBOptions, dbDir);
                Transaction transaction = transactionDB.beginTransaction(new WriteOptions());
                ReadOptions readOptions = new ReadOptions()) {
            if (!overwriteExisting && Objects.nonNull(transaction.get(readOptions, key.getBytes()))) {
                throw new RocksDBException(String.format(
                        "Entry with the key: %s already exists, please choose another or enable 'overwriteExisting' mode ",
                        key));
            }
            transaction.put(key.getBytes(), IOUtils.toByteArray(value));
            transaction.commit();
            LOG.info("Entry with key:{} successfully saved to RocksDB", key);
        } catch (RocksDBException | IOException e) {
            LOG.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }

    @Override
    public void save(String key, InputStream value) {
        try (RocksDB rocksDB = RocksDB.open(options, dbDir)) {
            if (!overwriteExisting && rocksDB.keyMayExist(key.getBytes(), null)) {
                LOG.error(
                        "Entry with the key: {} already exists, please choose another key or enable 'overwriteExisting' mode ",
                        key);
                return;
            }
            rocksDB.put(key.getBytes(), IOUtils.toByteArray(value));
            LOG.info("Entry with key:{} successfully saved to RocksDB", key);
        } catch (RocksDBException | IOException e) {
            LOG.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }

    @Override
    public InputStream find(String key) {
        InputStream result = null;
        try (RocksDB rocksDB = RocksDB.open(options, dbDir)) {
            byte[] bytes = rocksDB.get(key.getBytes());
            if (Objects.nonNull((bytes))) {
                LOG.info("Entry with key: {} found", key);
                result = new ByteArrayInputStream(bytes);
            }
        } catch (RocksDBException e) {
            LOG.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(),
                    e.getMessage());
        }
        return result;
    }

    @Override
    public void delete(String key) {
        try (RocksDB rocksDB = RocksDB.open(options, dbDir)) {
            rocksDB.delete(key.getBytes());
            LOG.info("Entry with key: {} deleted from RocksDB", key);
        } catch (RocksDBException e) {
            LOG.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }

    }

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }
}
