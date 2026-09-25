package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 对象存储的统一异常，携带可区分的 {@link ErrorCode}。
 */
public class StoreException extends RuntimeException {

    private final ErrorCode code;

    public StoreException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
