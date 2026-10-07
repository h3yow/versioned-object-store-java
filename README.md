# versioned-object-store-java

仅依赖本地磁盘的版本化对象存储核心（Java 21 / Spring Boot 3.4）。不依赖任何外部服务、
真实凭据或付费资源；除工程已有依赖（Spring Boot 自带的 Jackson）外无新增依赖。

## 对象版本模型

- 对象由 `(bucket, key)` 定位，每次成功提交产生一个**不可变版本**，版本号从 1 开始**单调递增**。
- 每个版本携带可追溯元数据 `VersionInfo`：版本号、内容 SHA-256、大小、提交时间、提交者、
  保留截止时间、合规锁定标志。
- **内容去重**：内容按 SHA-256 寻址存入 `blobs/`。同一内容无论写入多少次、写入多少个
  对象，只在磁盘上占用一份存储；重复内容的写入不消耗配额。
- **默认语义**：`WriteRequest.expectedVersion` 为 `null` 时，写入是确定且稳定的
  **无条件追加**（不检查当前版本，直接签发下一个版本号）；携带时期望值必须等于当前
  HEAD，否则拒绝（乐观并发控制）。

## 批量提交（事务语义）

业务里“一张单据 + 若干附属文件”需要整体提交时，使用
`ObjectStore.putBatch(batchId, bucket, items, principal)`：

- **原子可见性**：一次提交可包含多个对象，每项既可以是新增也可以是覆盖已有对象。
  整批对外是一个整体——**要么全部可见，要么全部不可见**。并发读者在提交过程中
  不会读到只出现一部分对象的中间状态。每个对象仍按原有规则产生不可变版本，
  版本号各自从 1 开始、单调递增；历史版本与版本号顺序不做任何破坏性变更。
- **提交协议（两阶段 + 提交标记）**：所有变更（单对象写入与批量提交）由一把全局
  提交锁串行化，保证两个提交永不交错。批量提交流程为：
  1. **整批校验**（任何落盘之前）：逐项校验非空、校验和、版本条件（`expectedVersion`）、
     写时保留/合规规则、WRITER 权限，并汇总净新增字节做容量核算；
  2. 写入全部净新增内容块（批内相同内容只存一份，与已有内容相同也复用，去重不占配额）；
  3. fsync 落盘 **PREPARED 批次日志** `batches/<batchId>/journal.json`（记录每个对象的
     键、签发版本号、SHA、版本条件等，是恢复与幂等的依据）；
  4. 写每个对象的新版本元数据，并把相关对象的 HEAD 推进到新版本；
  5. 原子创建 **提交标记** `batches/<batchId>/committed`——这是整批唯一的可见点，
     标记落盘的一瞬间整批所有对象同时可见。

  HEAD 推进之后、标记创建之前，读者虽然能在磁盘上看到更新的 HEAD 与新版本元数据，
  但读路径会过滤掉“所属批次标记尚不存在”的版本，因此这些版本完全不可见
  （`get` / `headVersion` / `listVersions` 一致）。
- **容量与去重**：整批只按**净新增**内容一次性核算占用（批内去重 + 与已有内容去重），
  超出配额整批拒绝；拒绝发生在任何落盘之前，**不会残留已经占掉的空间**或半成品数据。
- **整批拒绝原因可区分**：任一项无 WRITER 权限（`ACCESS_DENIED`）、版本条件冲突
  （`VERSION_CONFLICT`）、当前 HEAD 版本处于保留期（`RETENTION_ACTIVE`）或被合规锁定
  （`COMPLIANCE_LOCKED`）、校验和不符（`CHECKSUM_MISMATCH`）、空内容（`EMPTY_CONTENT`）、
  超配额（`QUOTA_EXCEEDED`）、空批次/批内重复键（`BATCH_INVALID`）都会让整批一项不落。
- **写时保留闸门（单对象写入同样适用）**：若对象当前 HEAD 版本仍在保留期内或处于
  合规锁定，则对它的覆盖写入（产生新版本）也会被拒绝（`RETENTION_ACTIVE` /
  `COMPLIANCE_LOCKED`），避免受保护内容被新版本“顶掉”。删除侧规则保持不变。
  新版本仍照常继承当时的对象保留策略。
- **失败与重试（幂等）**：
  - 校验阶段失败：无任何落盘，客户端修正后可用同一 `batchId` 重新提交；
  - 同一 `batchId` 以**完全相同**的请求（bucket、principal、每项的键/内容/版本条件/
    校验和）重试：返回首次提交签发的**稳定一致**结果，**不会重复生成版本、不会重复
    占用空间**；网络抖动、并发重复提交都安全（并发相同重试全部拿到同一结果）；
  - 同一 `batchId` 但请求内容与首次不一致：以 `BATCH_RETRY_CONFLICT` 整批拒绝；
  - `batchId` 由客户端提供，在同一 bucket 内唯一；重试必须复用同一标识。
- **崩溃恢复**：进程强杀或断电后重启（重新构造 `LocalObjectStore`）时：
  - 删除所有 `*.tmp` 临时文件；
  - 对每个已落盘的 PREPARED 日志**前滚**到底（补齐元数据、推进 HEAD、创建提交标记），
    因此到达 PREPARED 的批次重启后必然是“整批成功且完整可读”；
  - 没有对应日志的悬空版本文件（崩溃发生在日志落盘之前）被清理，整批完全不算数；
  - 未提交写入残留的孤儿内容块由 `garbageCollect` 回收。
  - 不存在“半批数据”状态。

### 与单对象写入的兼容关系

- 单对象 `put` 与批量提交共用同一套版本模型、去重存储、配额、权限与保留规则；
  单对象写入版本的 `batchId` 为 `null`，随该对象 HEAD 的原子更新立即可见。
- 新旧版本元数据完全兼容：旧版本 JSON 没有 `batchId` 字段时按 `null`（单对象写入）解读。
- `VersionInfo` 仅新增了可空字段 `batchId`，原有构造器与所有既有行为保持不变。


### 磁盘布局

```
root/
  blobs/<sha256>                                  内容寻址块（不可变，去重）
  buckets/<bucket>/objects/<key>/
    versions/v-<n>.json                           不可变版本元数据（含可选 batchId）
    HEAD                                          已签发版本号（提交点）
    retention.json                                保留策略
  batches/<batchId>/
    journal.json                                  PREPARED 批次日志（各项版本计划与身份）
    committed                                     提交标记（整批原子可见点）
```

key 经百分号编码映射为安全路径段，任意 UTF-8 键均可使用且可逆。

## 并发与恢复语义

- **提交协议**（全局提交锁内执行）：校验 → 写新内容块（tmp + fsync + 原子 rename，并计配额）
  → 写版本元数据（tmp + fsync + 原子 rename 为 `v-<n>.json`）→ 原子更新 `HEAD`
  → fsync 目录。`HEAD` 是单对象写入的**提交点**；批量提交的提交点是 `committed` 标记。
- **无半提交可见状态**：读者只认可版本号 ≤ HEAD 的版本文件；属于某个批次的版本还要求
  该批次的提交标记已存在。元数据与 HEAD 先落盘、标记后发布，因此并发读者永远不会看到
  未完成的单对象版本，也不会看到只出现一部分对象的批次中间状态。
- **并发提交**：单对象写入与批量提交由同一把全局提交锁串行化，已提交版本单调递增且
  无间隙，两个提交永不交错；乐观并发（`expectedVersion`）下同一轮只有一个提交者成功，
  其余得到 `VERSION_CONFLICT`。
- **崩溃恢复**：构造 `LocalObjectStore` 时自动执行恢复——删除所有 `*.tmp` 临时文件、
  将每个已落盘的 PREPARED 批次日志前滚为整批提交、删除无日志且版本号 > HEAD 的悬空
  版本文件（从未提交，不算成功）。已提交状态在重启后完整可见，去重关系保持；
  未完成写入留下的未引用内容块由 `garbageCollect` 回收。

## 访问控制与保留删除规则

- 基于 `(bucket, principal)` 的角色授予：`READER < WRITER < ADMIN`（`LocalAccessControl`）。
  读需要 READER，写/删除/设置保留策略需要 WRITER，创建 bucket 者自动成为其 ADMIN。
- **保留期**：`RetentionPolicy.retentionUntil` 之前删除对应版本被拒绝（`RETENTION_ACTIVE`）；
  过期后允许删除。
- **合规锁定**：`complianceLock=true` 的版本任何删除都被拒绝（`COMPLIANCE_LOCKED`），
  优先于保留期判断。
- **回收规则**：删除版本只删除元数据；内容块仅当不再被**任何**对象的**任何**版本引用时，
  才由 `garbageCollect` 回收并释放配额。

### 可区分的拒绝原因（`StoreException.code()`）

| ErrorCode | 含义 |
|---|---|
| `CHECKSUM_MISMATCH` | 声明的 SHA-256 与实际内容不符（读取时校验失败亦同） |
| `EMPTY_CONTENT` | 空内容写入 |
| `QUOTA_EXCEEDED` | 新内容块超出存储配额（去重命中不计） |
| `ACCESS_DENIED` | 越权访问 |
| `OBJECT_NOT_FOUND` | 对象/版本/bucket 不存在 |
| `VERSION_CONFLICT` | `expectedVersion` 与当前 HEAD 不一致 |
| `RETENTION_ACTIVE` | 保留期内删除，或覆盖保留期内的对象（写时闸门） |
| `COMPLIANCE_LOCKED` | 合规锁定内删除，或覆盖合规锁定对象（写时闸门） |
| `BATCH_INVALID` | 批量请求不合法（空批次、批内重复键、空 batchId） |
| `BATCH_RETRY_CONFLICT` | 同一批量标识重试时，请求与首次提交不一致 |

## 本地验证方法

```bash
mvn test
```

测试覆盖（日志中打印关键输入、版本标识与判定依据）：

- `VersionedWriteTest`：去重（3 次相同内容写入仅 1 个块）、版本单调与不可变、
  默认追加语义、版本冲突、校验和不匹配、空写入、配额超限及去重不占配额。
- `ConcurrencyTest`：16 线程并发提交同一对象（版本唯一、无间隙、无半提交）、
  乐观并发下同一轮仅一人成功、并发读写时读者永远看到完整一致的内容。
- `CrashRecoveryTest`：在“元数据已落盘、HEAD 未更新”的崩溃点注入模拟崩溃，
  重启后未提交版本不可见且被清理；已提交状态与去重结果在重启后保持；
  崩溃后重试的提交语义确定。
- `AccessRetentionGcTest`：越权读/写/删除、对象不存在、保留期内删除被拒、
  保留期过期后删除成功、合规锁定删除被拒、内容块仅在无引用时回收。
- `BatchCommitTest`（批量提交）：整批原子提交且各对象版本单调、提交窗口内读者只见
  旧版本、12 读者并发压测下观测代际不发生回落、批内及与已有内容去重只存一份、
  容量按净新增核算且边界精确、超配额拒绝不留任何痕迹、权限/版本条件/保留期/合规锁定
  导致整批拒绝且原因可区分、空批与批内重复键拒绝、同标识串行与并发重试幂等且返回
  稳定结果、同标识不同请求返回 `BATCH_RETRY_CONFLICT`。
- `BatchCrashRecoveryTest`（批量崩溃恢复，在同一目录重建实例模拟重启）：
  所有 HEAD 已推进但提交标记未发布时崩溃 → 重启前滚为整批成功；提交标记刚发布后
  崩溃 → 重启后完整可见；净新增内容块已落盘但日志未写入时崩溃 → 整批完全不算数、
  同标识重试按全新提交处理。

最小使用示例：

```java
LocalAccessControl acl = new LocalAccessControl();
LocalObjectStore store = new LocalObjectStore(Path.of("/tmp/store"), 1L << 30, acl);
store.createBucket("docs", "alice");           // alice 成为 docs 的 ADMIN
acl.grant("docs", "bob", Role.WRITER);

VersionInfo v1 = store.put("docs", "a.txt",
        WriteRequest.of("hello".getBytes()), "bob");
byte[] data = store.get("docs", "a.txt", "alice");
store.setRetention("docs", "a.txt",
        RetentionPolicy.until(Instant.now().plus(Duration.ofDays(7))), "bob");

// 单据 + 附属文件整体提交；网络抖动时用同一 batchId 原样重试即可
List<BatchItem> items = List.of(
        BatchItem.of("orders/42.json", "{...}".getBytes(StandardCharsets.UTF_8)),
        BatchItem.of("orders/42/attach-1.bin", attachment1),
        BatchItem.of("orders/42/attach-2.bin", attachment2));
BatchResult batch = store.putBatch("order-42-submit-7f3a", "docs", items, "bob");
batch.versions().forEach(v ->
        System.out.println(v.key() + " -> v" + v.version()));
```
