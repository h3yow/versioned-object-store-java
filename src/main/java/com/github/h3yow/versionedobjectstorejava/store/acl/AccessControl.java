package com.github.h3yow.versionedobjectstorejava.store.acl;

import com.github.h3yow.versionedobjectstorejava.store.StoreException;

/**
 * 基于本地配置的访问控制：按 (bucket, principal) 授予角色。
 */
public interface AccessControl {

    /** 授予角色（幂等）。 */
    void grant(String bucket, String principal, Role role);

    /** 撤销角色。 */
    void revoke(String bucket, String principal);

    /**
     * 要求 principal 在 bucket 上至少拥有 required 角色，否则抛出
     * {@link StoreException}（ACCESS_DENIED）。
     */
    void require(String bucket, String principal, Role required);
}
