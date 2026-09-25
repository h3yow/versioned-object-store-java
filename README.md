# 版本化本地对象存储（Versioned Local Object Store）

仅依赖**本地磁盘**的对象存储核心，基于 Spring Boot 3 / Java 21 实现。不依赖任何外部服务、
真实凭据或付费资源。支持版本化写入与内容去重、并发安全、崩溃恢复、基于空间的访问控制、
保留期与合规锁定删除语义。

---

## 1. 对象版本模型

### 1.1 基本概念

| 概念 | 说明 |
| --- | --- |
| 存储空间 Space | 配额与授权的边界，创建时指定 `quotaBytes` 与授权表 |
| 对象 Object | 空间内以 `objectId` 标识，包含一个严格有序的版本序列 |
| 版本 Version | 一次成功提交的不可变产物，同一对象内版本号从 1 开始**单调递增、连续不跳号** |
| 内容块 Blob | 以内容 SHA-256 寻址的不可变磁盘文件，内容相同即物理同一块（去重） |

每个版本的可追溯元数据（记录在不可变事件中）：

```
space, objectId, version, sha256, sizeBytes, ownerPrincipal,
retentionUntilEpochMillis, legalHold, committedAtEpochMillis, deleted
```

### 1.2 写入与版本条件（`expectedVersion`）

| 取值 | 语义 |
| --- | --- |
| `-1`（默认） | **创建或覆盖最新版本**：不存在则创建 v1，存在则追加为 head+1。未携带版本条件的客户端始终得到该确定、稳定的语义 |
| `0` | 对象必须尚不存在，否则 `VERSION_CONFLICT` |
| `N > 0` | 乐观锁：当前最新版本必须恰好为 N，否则 `VERSION_CONFLICT`（典型 CAS 用法） |

### 1.3 内容去重与容量

- 内容先计算 SHA-256，块文件命名为 `blobs/<前2位>/<其余62位>.bin`；
- 同一空间内同一内容无论被多少对象/版本引用，**磁盘上只有一份块文件**；
- 空间占用按**去重后的唯一内容块**统计，新内容若会使占用超过 `quotaBytes` 则拒绝
  （`QUOTA_EXCEEDED`）；
- 单对象大小受 `objectstore.max-object-bytes` 限制（`OBJECT_TOO_LARGE`）。

### 1.4 可区分的写入拒绝原因

按固定顺序校验，`StoreException#getReason()` 与 HTTP 错误体 `reason` 字段稳定可取：

| RejectReason | 触发条件 | HTTP |
| --- | --- | --- |
| `EMPTY_CONTENT` | 内容为空（null 或 0 字节） | 422 |
| `CHECKSUM_MISMATCH` | `X-Content-Sha256` 与实际 SHA-256 不一致 | 422 |
| `OBJECT_TOO_LARGE` | 超过单对象字节上限 | 422 |
| `QUOTA_EXCEEDED` | 去重后新增内容会超出空间配额 | 422 |
| `VERSION_CONFLICT` | 乐观锁基线不匹配 / 空间重复创建 | 409 |
| `SPACE_NOT_FOUND` / `OBJECT_NOT_FOUND` / `VERSION_NOT_FOUND` | 对应实体不存在或版本已删除 | 404 |
| `ACCESS_DENIED` | 主体无所需权限 | 403 |
| `RETENTION_ACTIVE` / `LEGAL_HOLD_ACTIVE` / `BLOB_IN_USE` | 保留期/法务锁定/引用保护 | 423 |

---

## 2. 并发与崩溃恢复语义

### 2.1 原子提交流程

所有变更在进程内一把全局锁内串行化，保证并发提交不可能产生半提交可见状态：

```
① 输入校验（空/校验和/大小/配额/权限/版本条件）
② 内容 -> tmp/*.tmp，fsync 后原子 MOVE 为 blobs/xx/....bin（块出现即完整）
③ 事件日志 APPEND 一行 JSON 并 force(true) fsync  ← 唯一的"提交点"
④ 更新内存索引（版本号、去重引用计数），提交对读取者原子可见
```

- 版本号在锁内分配，并发 N 次提交的结果必然是连续的 N 个版本，不重不漏；
- 读取提供 `readLatest/readVersion` 原子接口，**元数据与内容在同一临界区内取得**，
  读者永远看到一个完整一致、且与其元数据校验和吻合的对象；
- 相同内容并发提交：恰好一次新建块，其余全部命中去重，共享块的引用计数精确等于版本数。

### 2.2 崩溃恢复（重启即恢复，无需人工介入）

事件日志 `journals/<space>.journal`（JSON Lines，每行一个事件，以 `\n` 结尾）是
**唯一的已提交事实来源**，内存索引完全由它重放重建：

| 崩溃发生在 | 重启后的结果 |
| --- | --- |
| ②之后、③之前（块已落盘但日志未提交） | 提交不可见（head 不变）；`blobs/` 协调扫描发现该块存活引用数为 0，作为**孤儿块物理回收**；`tmp/` 残留临时文件被清理 |
| ③ fsync 之后、④之前（进程猝死） | 日志已提交，重放后版本**完整可见且内容可读**；引用计数以日志为权威重建 |
| 日志最后一行被撕裂（无结尾 `\n` / JSON 不完整） | 重放时识别为未提交的截断尾行并**截断丢弃** |

恢复流程：清理 `tmp/` → 枚举全部空间日志 → 逐空间重放并修复截断尾行、重建对象/版本/
去重计数索引 → 以日志权威计数协调磁盘块（修复计数、回收孤儿块、报告缺失块）。

---

## 3. 访问控制与保留删除规则

### 3.1 访问控制（无真实凭据，仅本地演示）

- 身份通过 `X-Principal` 请求头传递；
- 创建空间时写入授权表 `principal -> 权限`，创建者自动获得 `ADMIN`；
- 权限层级：`ADMIN > DELETE > WRITE > READ`，高级别隐式包含低级别；
- 读对象需要 `READ`，提交新版本需要 `WRITE`，删除版本需要 `DELETE`；
- 任何越权一律 `ACCESS_DENIED`（403），与"不存在"类错误严格可区分。

### 3.2 保留期（Retention）与合规锁定（Legal Hold）

- 提交时可用 `X-Retention-Millis` 设置保留期：截止前删除版本返回 `RETENTION_ACTIVE`（423），
  跨过保留期边界后删除成功；
- 提交时可用 `X-Legal-Hold: true` 设置合规锁定：**锁定不随时间解除**，删除永远返回
  `LEGAL_HOLD_ACTIVE`（423）；
- 两种拒绝原因相互独立、可区分。

### 3.3 删除与内容回收

- 删除是版本级操作，提交一条 `VersionDeleted` 事件；已删除版本元数据仍保留可追溯，
  但读取/再次删除均返回 `VERSION_NOT_FOUND`；
- 内容块只有在**所有空间的任何存活版本都不再引用它**时才会被物理删除；
  仍被引用时拒绝回收（内部判定为 `BLOB_IN_USE`），其余存活版本读取不受影响。

---

## 4. 磁盘布局

```
<objectstore.root>/
├── blobs/
│   └── ab/
│       ├── cdef….bin          不可变内容块（SHA-256 分片寻址）
│       └── cdef….bin.refs     该块的全局存活引用计数（恢复时以日志权威重建）
├── tmp/
│   └── *.tmp                  写入暂存（原子 move，崩溃后清理）
├── journals/
│   └── <space>.journal        每空间一个 JSON Lines 追加日志（提交点）
└── seq.counter                全局事件序号计数器（丢失可由日志重放校准）
```

---

## 5. 本地验证方法

### 5.1 运行单元测试

```bash
mvn test
```

共 14 个测试，覆盖：

- `VersioningAndDedupTest`（4）：单调版本与读取一致性、跨对象去重、四类拒绝原因可区分、
  乐观锁与默认版本语义；
- `ConcurrentCommitTest`（3）：12 写线程 × 10 提交 + 6 并发读者的版本连续性与对象完整性、
  相同内容并发去重、同基线乐观锁仅一个成功；
- `CrashRecoveryTest`（3）：**fork 真实子 JVM** 并在"块落盘后/日志 fsync 后"调用
  `Runtime.halt(1)` 模拟猝死，重启后验证回滚孤儿块 / 恢复已提交版本 / 截断撕裂日志行；
- `AccessControlAndRetentionTest`（3）：越权与各类"不存在"可区分、保留期边界与法务锁定、
  最后引用解除才回收。

测试日志（控制台）打印关键输入、版本标识（版本号/SHA-256）与判定依据，可直接检索
`关键输入`、`关键结果`、`判定` 关键字。

### 5.2 启动服务并用 HTTP 手工验证

```bash
mvn spring-boot:run        # 随机端口（server.port=0），见启动日志，或自行固定端口
```

```bash
BASE=http://localhost:8080

# 1) 创建空间（alice 为属主 ADMIN，bob 只读，dave 可删除，配额 10000 字节）
curl -s -X POST $BASE/api/spaces -H 'Content-Type: application/json' -d '{
  "space":"demo","ownerPrincipal":"alice","quotaBytes":10000,
  "grants":{"bob":"READ","dave":"DELETE"}}'

# 2) 版本化写入（校验和必填；-1 默认语义；带 60 秒保留期示例）
printf 'hello-v1' > /tmp/o1.bin
SHA=$(sha256sum /tmp/o1.bin | cut -d' ' -f1)
curl -s -X PUT $BASE/api/spaces/demo/objects/doc \
  -H "X-Principal: alice" -H "X-Content-Sha256: $SHA" \
  -H "Content-Type: application/octet-stream" \
  -H "X-Expected-Version: -1" -H "X-Retention-Millis: 60000" \
  --data-binary @/tmp/o1.bin
# -> {"version":1,"sha256":"...","deduplicated":false}

# 3) 读取最新版本 / 指定历史版本（响应头 X-Version、X-Content-Sha256）
curl -s $BASE/api/spaces/demo/objects/doc -H 'X-Principal: bob' -D - -o /dev/null
curl -s $BASE/api/spaces/demo/objects/doc/versions/1 -H 'X-Principal: bob'

# 4) 保留期内删除 -> 423 RETENTION_ACTIVE；越权（carol）-> 403 ACCESS_DENIED
curl -s -X DELETE $BASE/api/spaces/demo/objects/doc/versions/1 -H 'X-Principal: dave'
curl -s $BASE/api/spaces/demo/objects/doc -H 'X-Principal: carol'
```

错误响应统一为：`{"reason":"<RejectReason>","message":"..."}`。

### 5.3 关键配置（`src/main/resources/application.properties`）

| 属性 | 默认值 | 含义 |
| --- | --- | --- |
| `objectstore.root` | `./data/objectstore` | 存储根目录 |
| `objectstore.max-object-bytes` | `104857600` | 单对象字节上限 |
