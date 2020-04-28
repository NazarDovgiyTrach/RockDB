import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.assertj.core.util.Files;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.rocksdb.Options;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.key2asset.raclette.persistent.cache.configuration.AppConfig;
import com.key2asset.raclette.persistent.cache.store.RocksDBRepositoryImpl;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {AppConfig.class})
public class RocksDBRepositoryImplTest {
    @Autowired
    private Options options;
    private static final File DB_DIR = Files.temporaryFolder();

    @Test
    public void testSave() throws IOException {
        String entry = "Some text information";

        RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(options, DB_DIR.toString(), true);
        rocksDBRepository.save("Key1", new ByteArrayInputStream(entry.getBytes()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

        Assert.assertEquals(entry, new String(outputStream.toByteArray()));
    }

    @Test
    public void testSaveWhenOverwriteExistingModeDisabled() throws IOException {
        String entry = "Some text information";

        //Disabling overwriteExisting mode
        RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(options, DB_DIR.toString(), false);
        rocksDBRepository.save("Key1", new ByteArrayInputStream(entry.getBytes()));

        //Save new entry with existing key
        rocksDBRepository.save("Key1", new ByteArrayInputStream("New text".getBytes()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

        Assert.assertEquals("The previous entry should not be overwritten, because overwriteExisting mode disabled.",
                entry, new String(outputStream.toByteArray()));
    }

    @Test
    public void testDelete() throws IOException {
        String entry = "Some text information";

        RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(options, DB_DIR.toString(), true);
        rocksDBRepository.save("Key1", new ByteArrayInputStream(entry.getBytes()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

        rocksDBRepository.delete("Key1");

        Assert.assertNull(rocksDBRepository.find("Key1"));
    }

}
