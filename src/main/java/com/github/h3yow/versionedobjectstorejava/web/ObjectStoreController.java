package com.github.h3yow.versionedobjectstorejava.web;

import com.github.h3yow.versionedobjectstorejava.store.DeleteResult;
import com.github.h3yow.versionedobjectstorejava.store.ObjectStore;
import com.github.h3yow.versionedobjectstorejava.store.ObjectVersion;
import com.github.h3yow.versionedobjectstorejava.store.Permission;
import com.github.h3yow.versionedobjectstorejava.store.PutRequest;
import com.github.h3yow.versionedobjectstorejava.store.PutResult;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 对象存储的最小 REST 入口，用于本地手工验证。
 * 身份通过 {@code X-Principal} 头传递（仅本地演示，无真实凭据）。
 */
@RestController
@RequestMapping("/api/spaces")
public class ObjectStoreController {

    private static final String DEFAULT_EXPECTED_VERSION = "-1";

    private final ObjectStore store;

    public ObjectStoreController(ObjectStore store) {
        this.store = store;
    }

    /** 创建空间。请求体：{"space":"s","ownerPrincipal":"alice","quotaBytes":10000,"grants":{"bob":"READ"}} */
    @PostMapping
    public Map<String, Object> createSpace(@RequestBody Map<String, Object> body) {
        String space = (String) body.get("space");
        String owner = (String) body.get("ownerPrincipal");
        long quota = body.get("quotaBytes") == null ? Long.MAX_VALUE
                : Long.parseLong(body.get("quotaBytes").toString());
        @SuppressWarnings("unchecked")
        Map<String, String> rawGrants = (Map<String, String>) body.getOrDefault("grants", Map.of());
        Map<String, Permission> grants = new java.util.HashMap<>();
        rawGrants.forEach((k, v) -> grants.put(k, Permission.valueOf(v)));
        store.createSpace(space, owner, quota, grants);
        return Map.of("space", space, "created", true);
    }

    /**
     * 版本化写入。必填头 {@code X-Content-Sha256}；可选 {@code X-Expected-Version}（默认 -1）、
     * {@code X-Retention-Millis}、{@code X-Legal-Hold}。
     */
    @PutMapping(path = "/{space}/objects/{objectId}")
    public PutResult put(@PathVariable String space,
                         @PathVariable String objectId,
                         @RequestHeader("X-Principal") String principal,
                         @RequestHeader("X-Content-Sha256") String sha256,
                         @RequestHeader(value = "X-Expected-Version",
                                 defaultValue = DEFAULT_EXPECTED_VERSION) long expectedVersion,
                         @RequestHeader(value = "X-Retention-Millis", defaultValue = "0") long retentionMillis,
                         @RequestHeader(value = "X-Legal-Hold", defaultValue = "false") boolean legalHold,
                         @RequestBody(required = false) byte[] content) {
        return store.put(new PutRequest(space, objectId, principal, content == null ? new byte[0] : content,
                expectedVersion, sha256, retentionMillis, legalHold));
    }

    /** 读取最新版本的原始内容。 */
    @GetMapping(path = "/{space}/objects/{objectId}")
    public ResponseEntity<byte[]> get(@PathVariable String space,
                                      @PathVariable String objectId,
                                      @RequestHeader("X-Principal") String principal) {
        ObjectVersion meta = store.stat(space, objectId, principal);
        byte[] data = store.get(space, objectId, principal);
        return ResponseEntity.ok()
                .headers(versionHeaders(meta))
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(data);
    }

    /** 读取指定历史版本的原始内容。 */
    @GetMapping(path = "/{space}/objects/{objectId}/versions/{version}")
    public ResponseEntity<byte[]> getVersion(@PathVariable String space,
                                             @PathVariable String objectId,
                                             @PathVariable long version,
                                             @RequestHeader("X-Principal") String principal) {
        ObjectVersion meta = store.statVersion(space, objectId, version, principal);
        byte[] data = store.getVersion(space, objectId, version, principal);
        return ResponseEntity.ok()
                .headers(versionHeaders(meta))
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(data);
    }

    /** 查询最新版本元数据。 */
    @GetMapping(path = "/{space}/objects/{objectId}/stat")
    public ObjectVersion stat(@PathVariable String space,
                              @PathVariable String objectId,
                              @RequestHeader("X-Principal") String principal) {
        return store.stat(space, objectId, principal);
    }

    /** 删除指定版本（保留期/合规锁定会被拒绝；无引用后物理回收）。 */
    @DeleteMapping(path = "/{space}/objects/{objectId}/versions/{version}")
    public DeleteResult delete(@PathVariable String space,
                               @PathVariable String objectId,
                               @PathVariable long version,
                               @RequestHeader("X-Principal") String principal) {
        return store.deleteVersion(space, objectId, version, principal);
    }

    private HttpHeaders versionHeaders(ObjectVersion meta) {
        HttpHeaders h = new HttpHeaders();
        h.add("X-Version", Long.toString(meta.version()));
        h.add("X-Content-Sha256", meta.sha256());
        h.add("ETag", "\"" + meta.sha256() + "\"");
        return h;
    }
}
