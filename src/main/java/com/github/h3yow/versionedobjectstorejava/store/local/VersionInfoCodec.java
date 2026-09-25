package com.github.h3yow.versionedobjectstorejava.store.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.h3yow.versionedobjectstorejava.store.model.VersionInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link VersionInfo} 与磁盘 JSON 文件之间的编解码。
 * 仅使用工程已有依赖（spring-boot-starter-web 自带的 Jackson）。
 */
final class VersionInfoCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private VersionInfoCodec() {
    }

    static void write(Path file, VersionInfo info) {
        try {
            MAPPER.writeValue(file.toFile(), info);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write version metadata " + file, e);
        }
    }

    static VersionInfo read(Path file) {
        try {
            return MAPPER.readValue(file.toFile(), VersionInfo.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read version metadata " + file, e);
        }
    }

    static boolean isVersionFile(Path file) {
        String name = file.getFileName().toString();
        return name.startsWith("v-") && name.endsWith(".json") && Files.isRegularFile(file);
    }

    static long versionOf(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(2, name.length() - ".json".length()));
    }
}
