package com.github.h3yow.versionedobjectstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 访问控制：基于空间授权表判定，无真实凭据体系。
 *
 * <p>权限层级：ADMIN &gt; DELETE &gt; WRITE &gt; READ；主体不存在或权限不足一律拒绝。
 */
public final class AccessControl {

    private static final Logger log = LoggerFactory.getLogger(AccessControl.class);

    private AccessControl() {
    }

    /**
     * 校验主体对空间是否具备所需权限，否则抛出 {@link StoreException}（ACCESS_DENIED）。
     *
     * @param grants    空间授权表：principal -&gt; permission
     * @param principal 请求主体（X-Principal 头）
     * @param required  所需权限
     */
    public static void checkSpace(Map<String, Permission> grants, String principal, Permission required) {
        Permission granted = grants == null ? null : grants.get(principal);
        if (granted == null || !covers(granted, required)) {
            log.warn("访问拒绝: principal={}, required={}, granted={}", principal, required, granted);
            throw new StoreException(RejectReason.ACCESS_DENIED,
                    "主体 [" + principal + "] 缺少权限 " + required);
        }
        log.debug("访问允许: principal={}, required={}, granted={}", principal, required, granted);
    }

    /**
     * 权限包含关系：ADMIN 覆盖一切，DELETE 隐式包含 WRITE/READ，WRITE 隐式包含 READ。
     */
    public static boolean covers(Permission granted, Permission required) {
        if (granted == null || required == null) {
            return false;
        }
        return rank(granted) >= rank(required);
    }

    private static int rank(Permission p) {
        return switch (p) {
            case READ -> 1;
            case WRITE -> 2;
            case DELETE -> 3;
            case ADMIN -> 4;
        };
    }
}
