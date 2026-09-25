package com.github.h3yow.versionedobjectstorejava.store.acl;

import com.github.h3yow.versionedobjectstorejava.store.ErrorCode;
import com.github.h3yow.versionedobjectstorejava.store.StoreException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link AccessControl} 的本地内存实现：按 (bucket, principal) 授予角色，
 * 角色满足包含关系 ADMIN ⊃ WRITER ⊃ READER。
 */
public class LocalAccessControl implements AccessControl {

    private final Map<String, Map<String, Role>> grants = new ConcurrentHashMap<>();

    @Override
    public void grant(String bucket, String principal, Role role) {
        grants.computeIfAbsent(bucket, b -> new ConcurrentHashMap<>()).put(principal, role);
    }

    @Override
    public void revoke(String bucket, String principal) {
        Map<String, Role> bucketGrants = grants.get(bucket);
        if (bucketGrants != null) {
            bucketGrants.remove(principal);
        }
    }

    @Override
    public void require(String bucket, String principal, Role required) {
        Map<String, Role> bucketGrants = grants.get(bucket);
        Role actual = bucketGrants == null ? null : bucketGrants.get(principal);
        if (actual == null || actual.ordinal() < required.ordinal()) {
            throw new StoreException(ErrorCode.ACCESS_DENIED,
                    "principal '" + principal + "' lacks role " + required + " on bucket '" + bucket + "'");
        }
    }
}
