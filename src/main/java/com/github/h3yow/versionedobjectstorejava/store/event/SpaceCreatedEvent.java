package com.github.h3yow.versionedobjectstorejava.store.event;

import com.github.h3yow.versionedobjectstorejava.store.Permission;

import java.util.Map;

/**
 * 存储空间创建事件：记录配额与访问授权。
 */
public record SpaceCreatedEvent(
        long seq,
        String space,
        String ownerPrincipal,
        long quotaBytes,
        Map<String, Permission> grants
) implements Event {
}
