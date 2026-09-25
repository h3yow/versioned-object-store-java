package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 对象存储拒绝操作时抛出的唯一异常类型，携带可区分的 {@link RejectReason}。
 */
public class StoreException extends RuntimeException {

    private final RejectReason reason;

    public StoreException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
