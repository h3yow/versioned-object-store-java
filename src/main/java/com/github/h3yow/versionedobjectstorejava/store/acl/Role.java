package com.github.h3yow.versionedobjectstorejava.store.acl;

/**
 * 访问角色。权限包含关系：ADMIN ⊃ WRITER ⊃ READER。
 */
public enum Role {
    READER, WRITER, ADMIN
}
