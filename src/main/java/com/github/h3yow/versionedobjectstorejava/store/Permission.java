package com.github.h3yow.versionedobjectstorejava.store;

/**
 * 主体对存储空间的访问权限。ADMIN 隐式包含 READ/WRITE/DELETE。
 */
public enum Permission {
    READ,
    WRITE,
    DELETE,
    ADMIN
}
