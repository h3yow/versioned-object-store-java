package com.github.h3yow.versionedobjectstorejava.tests;

import com.github.h3yow.versionedobjectstorejava.store.BlobStore;
import com.github.h3yow.versionedobjectstorejava.store.Journal;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.Permission;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

/**
 * 测试共用构造工具：每个测试使用独立临时目录，存储组件全部直接基于磁盘构建。
 */
final class StoreTestSupport {

    static final String OWNER = "alice";
    static final String READER = "bob";
    static final String OUTSIDER = "carol";

    private StoreTestSupport() {
    }

    static TestEnv newEnv(@TempDir Path tmp, long maxObjectBytes) {
        BlobStore blobs = new BlobStore(tmp);
        Journal journal = new Journal(tmp);
        ObjectStore store = new ObjectStore(blobs, journal, maxObjectBytes);
        return new TestEnv(tmp, blobs, journal, store);
    }

    static TestEnv newEnv(@TempDir Path tmp) {
        return newEnv(tmp, Long.MAX_VALUE);
    }

    /** 创建一个 alice=ADMIN、bob=READ 的默认空间。 */
    static String defaultSpace(TestEnv env, String space, long quotaBytes) {
        env.store().createSpace(space, OWNER, quotaBytes, Map.of(READER, Permission.READ));
        return space;
    }

    record TestEnv(Path root, BlobStore blobs, Journal journal, ObjectStore store) {
    }
}
