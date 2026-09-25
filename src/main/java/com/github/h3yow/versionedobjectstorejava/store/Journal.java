package com.github.h3yow.versionedobjectstorejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.versionedobjectstorejava.store.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 追加写事件日志：每个存储空间一个 JSON Lines 文件，崩溃后据此重放恢复全部已提交状态。
 *
 * <p>持久化协议：事件先序列化为单行 JSON，以 APPEND 方式写入并 {@link FileChannel#force} 落盘。
 * 每行以 '\n' 结尾；只有完整结尾换行的行才算已提交。重放时若末尾存在不完整行
 * （崩溃瞬间撕裂的半行），直接截断丢弃——该事件对应的写入即视为未完成。
 *
 * <p>全局事件序号单调递增，跨所有空间共享，存于 {@code seq.counter} 文件。
 */
public class Journal {

    private static final Logger log = LoggerFactory.getLogger(Journal.class);

    private final Path journalsDir;
    private final Path seqFile;
    private final ObjectMapper mapper = new ObjectMapper();
    private long seq;

    public Journal(Path rootDir) {
        this.journalsDir = rootDir.resolve("journals");
        this.seqFile = rootDir.resolve("seq.counter");
        try {
            Files.createDirectories(journalsDir);
            if (Files.notExists(seqFile)) {
                Files.writeString(seqFile, "0", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            }
            this.seq = Long.parseLong(Files.readString(seqFile, StandardCharsets.UTF_8).trim());
        } catch (IOException e) {
            throw new IllegalStateException("无法初始化日志目录: " + rootDir, e);
        }
    }

    /**
     * 重放指定空间的全部事件；遇到崩溃留下的截断尾行时截断修复。
     * 同时以日志中出现的最大事件序号校准全局序号。
     */
    public synchronized List<Event> replay(String space) throws IOException {
        Path file = journalFile(space);
        if (Files.notExists(file)) {
            return List.of();
        }
        byte[] raw = Files.readAllBytes(file);
        int validLen = raw.length;
        if (raw.length > 0 && raw[raw.length - 1] != '\n') {
            int lastNl = -1;
            for (int i = raw.length - 1; i >= 0; i--) {
                if (raw[i] == '\n') {
                    lastNl = i;
                    break;
                }
            }
            validLen = lastNl + 1;
            byte[] torn = new byte[raw.length - validLen];
            System.arraycopy(raw, validLen, torn, 0, torn.length);
            log.warn("空间[{}]日志末尾存在未提交的截断行（{}字节），截断丢弃: {}",
                    space, torn.length, new String(torn, StandardCharsets.UTF_8));
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                ch.truncate(validLen);
                ch.force(true);
            }
        }
        List<Event> events = new ArrayList<>();
        for (String line : new String(raw, 0, validLen, StandardCharsets.UTF_8).split("\n", -1)) {
            if (line.isBlank()) {
                continue;
            }
            Event event = mapper.readValue(line, Event.class);
            events.add(event);
            if (event.seq() > seq) {
                seq = event.seq();
            }
        }
        log.info("空间[{}]日志重放完成: 事件数={}, 校准后最大序号={}", space, events.size(), seq);
        return events;
    }

    /**
     * 追加一条已提交事件并 fsync 保证持久性；返回分配的全局序号。
     * 调用方负责传入已带序号的事件（序号通过 {@link #nextSeq()} 提前分配）。
     */
    public synchronized void append(String space, Event event) {
        try {
            Path file = journalFile(space);
            Files.createDirectories(file.getParent());
            byte[] line = (mapper.writeValueAsString(event) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(line));
                ch.force(true);
            }
            log.info("日志事件已提交: space={}, seq={}, event={}", space, event.seq(),
                    event.getClass().getSimpleName());
        } catch (IOException e) {
            throw new UncheckedIOException("日志追加失败: space=" + space, e);
        }
    }

    /** 分配下一个全局序号并持久化计数器（计数器丢失不影响正确性，重放会校准）。 */
    public synchronized long nextSeq() {
        seq++;
        try {
            Files.writeString(seqFile, Long.toString(seq), StandardCharsets.UTF_8,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new UncheckedIOException("序号持久化失败", e);
        }
        return seq;
    }

    public synchronized long currentSeq() {
        return seq;
    }

    /** 列出磁盘上现存的全部空间（按 *.journal 文件名推断，按名排序保证恢复顺序稳定）。 */
    public List<String> listSpaces() throws IOException {
        List<String> names = new ArrayList<>();
        if (!Files.exists(journalsDir)) {
            return names;
        }
        try (var stream = Files.newDirectoryStream(journalsDir, "*.journal")) {
            for (Path p : stream) {
                String fn = p.getFileName().toString();
                names.add(fn.substring(0, fn.length() - ".journal".length()));
            }
        }
        names.sort(String::compareTo);
        return names;
    }

    private Path journalFile(String space) {
        if (space == null || !space.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new StoreException(RejectReason.SPACE_NOT_FOUND, "非法存储空间名: " + space);
        }
        return journalsDir.resolve(space + ".journal");
    }
}
