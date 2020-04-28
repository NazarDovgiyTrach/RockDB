package com.key2asset.raclette.persistent.cache.api;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.multipart.MultipartFile;

import com.key2asset.raclette.persistent.cache.store.KeyValueRepository;
@Controller
public class CacheApi {

    private final KeyValueRepository<String, InputStream> rocksDB;

    public CacheApi(KeyValueRepository<String, InputStream> rocksDB) {
        this.rocksDB = rocksDB;
    }

    @PostMapping("raclette/{key}")
    public ResponseEntity save(@PathVariable("key") String key, @RequestBody MultipartFile value)
            throws IOException {
        rocksDB.save(key, value.getInputStream());
        return ResponseEntity.ok().build();
    }

    @GetMapping("raclette/{key}")
    public void find(@PathVariable("key") String key, HttpServletResponse response) throws IOException {
        IOUtils.copyLarge(rocksDB.find(key),response.getOutputStream());
    }

    @DeleteMapping("raclette/{key}")
    public ResponseEntity delete(@PathVariable("key") String key) {
        rocksDB.delete(key);
        return ResponseEntity.ok().build();
    }
}
