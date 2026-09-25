package com.github.h3yow.versionedobjectstorejava.web;

import com.github.h3yow.versionedobjectstorejava.store.RejectReason;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把可区分的拒绝原因映射为稳定的 HTTP 错误体 {@code {reason, message}}。
 */
@RestControllerAdvice
public class StoreExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StoreExceptionHandler.class);

    @ExceptionHandler(StoreException.class)
    public ResponseEntity<Map<String, Object>> handle(StoreException ex) {
        HttpStatus status = statusOf(ex.getReason());
        log.warn("HTTP 拒绝响应: reason={}, status={}, message={}", ex.getReason(), status, ex.getMessage());
        return ResponseEntity.status(status)
                .body(Map.of("reason", ex.getReason().name(), "message", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("reason", "BAD_REQUEST", "message", String.valueOf(ex.getMessage())));
    }

    private static HttpStatus statusOf(RejectReason reason) {
        return switch (reason) {
            case ACCESS_DENIED -> HttpStatus.FORBIDDEN;
            case SPACE_NOT_FOUND, OBJECT_NOT_FOUND, VERSION_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case VERSION_CONFLICT -> HttpStatus.CONFLICT;
            case RETENTION_ACTIVE, LEGAL_HOLD_ACTIVE, BLOB_IN_USE -> HttpStatus.LOCKED;
            case EMPTY_CONTENT, CHECKSUM_MISMATCH, OBJECT_TOO_LARGE, QUOTA_EXCEEDED ->
                    HttpStatus.UNPROCESSABLE_ENTITY;
        };
    }
}
