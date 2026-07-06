# Nacos 3.3.0 分布式锁实现原理深度分析

> 基于 Nacos `3.2.1-SNAPSHOT`（develop 分支，含 3.3.0 新增的 Reentrant / NonReentrant / Watchdog / Wait Queue 等特性）源码梳理
>
> 实验性能力：分布式锁当前仍为 experimental 状态，详见 `specs/en/lock/lock-spec.md`

---

## 目录

- [1. 总体架构](#1-总体架构)
- [2. 模块与核心类索引](#2-模块与核心类索引)
- [3. 资源模型](#3-资源模型)
- [4. 锁类型与 SPI 工厂](#4-锁类型与-spi-工厂)
- [5. 服务端核心：AbstractAtomicLock](#5-服务端核心abstractatomiclock)
- [6. 三种锁实现对比](#6-三种锁实现对比)
- [7. LockManager 与内存状态](#7-lockmanager-与内存状态)
- [8. CP（Raft）一致性集成](#8-cpraft-一致性集成)
- [9. 服务层 LockOperationServiceImpl 全流程](#9-服务层-lockoperationserviceimpl-全流程)
- [10. gRPC 入口：LockRequestHandler](#10-grpc-入口lockrequesthandler)
- [11. 等待队列与推送通知机制](#11-等待队列与推送通知机制)
- [12. 连接断开清理](#12-连接断开清理)
- [13. 过期扫描：LockExpireScanner](#13-过期扫描lockexpirescanner)
- [14. 快照持久化：NacosLockSnapshotOperation](#14-快照持久化nacoslocksnapshotoperation)
- [15. 客户端 SDK 实现](#15-客户端-sdk-实现)
- [16. Watchdog 续约机制](#16-watchdog-续约机制)
- [17. 可观测性：Metrics 与切面](#17-可观测性metrics-与切面)
- [18. 关键时序图](#18-关键时序图)
- [19. 线程安全模型](#19-线程安全模型)
- [20. 配置参数](#20-配置参数)
- [21. 实验性限制与待解决问题](#21-实验性限制与待解决问题)
- [22. 核心结论速查](#22-核心结论速查)

---

## 1. 总体架构

Nacos 分布式锁是一个 **CP（Raft）强一致** 的互斥原语，用于在 Nacos 集群上为客户端协调短临界区。整体架构分三层：

```
┌──────────────────────────────────────────────────────────────────┐
│ 客户端 (client 模块 + api 模块)                                  │
│   NacosLockFactory ──> NacosLockService                          │
│      │                     │                                     │
│      │                     ├──> LockGrpcClient (gRPC 请求/推送)   │
│      │                     └──> NacosLockWatchdog (定时续约)      │
│      └──> NacosLock (实现 java.util.concurrent.locks.Lock)       │
│                                                                   │
│   通信协议: gRPC (LockOperationRequest / LockNotificationRequest) │
└──────────────────────────────┬───────────────────────────────────┘
                               │ gRPC (长连接 + 能力协商)
┌──────────────────────────────▼───────────────────────────────────┐
│ 服务端 (lock 模块)                                              │
│   LockRequestHandler (gRPC RequestHandler)                        │
│         │  Aspect: RequestLockAspect (Metrics)                  │
│         ▼                                                         │
│   LockOperationServiceImpl                                       │
│      ├── 实现 LockOperationService (业务接口)                    │
│      └── 继承 RequestProcessor4CP (CP 状态机)                    │
│         │                                                         │
│         ▼  protocol.write(WriteRequest)                          │
│   ┌──────────────────────────────────────────┐                   │
│   │ CP 协议 (JRaft, group=lock_acquire_service_v2) │            │
│   │   onApply() → acquireLock/releaseLock/...   │             │
│   │   loadSnapshotOperate() → NacosLockSnapshotOperation │      │
│   └──────────────────────────────────────────┘                   │
│         │                                                         │
│         ▼  操作内存状态                                            │
│   NacosLockManager                                                │
│      └── ConcurrentHashMap<LockKey, AtomicLockService>           │
│            └── AbstractAtomicLock (owner / reentrantCount /      │
│               expiredTimestamp / waitQueue)                     │
│                                                                   │
│   辅助组件:                                                       │
│     - LockConnectionEventListener (断连清理)                      │
│     - LockExpireScanner (定时过期扫描)                            │
│     - RpcPushService (向 waiter 推送通知)                         │
└──────────────────────────────────────────────────────────────────┘
```

**设计要点**：

1. **强一致**：所有写操作（acquire/release/renew/expire/cancelWait/cleanup）都通过 Raft 提交，集群内状态一致；CP 不可用时直接失败，宁不可用也不冒不一致风险。
2. **内存 + 日志/快照**：锁状态本质是进程内存中的 `ConcurrentHashMap`，持久化依赖 Raft 日志 + `nacos_lock.zip` 快照，**不是** 关系数据库。
3. **实验性**：尚未提供 owner token / fencing token / 完整鉴权，不应作为生产级强一致锁依赖。

---

## 2. 模块与核心类索引

### 2.1 服务端 `lock/` 模块

| 文件 | 作用 |
|------|------|
| `lock/LockManager.java` | 锁管理接口 |
| `lock/NacosLockManager.java` | 默认实现，持有全局 `ConcurrentHashMap` |
| `lock/aspect/RequestLockAspect.java` | gRPC handler 切面，埋点 Metrics |
| `lock/constant/Constants.java` | CP group 名常量 |
| `lock/constant/PropertiesConstant.java` | 配置项与默认值 |
| `lock/core/reentrant/AtomicLockService.java` | 锁服务接口 |
| `lock/core/reentrant/AbstractAtomicLock.java` | 锁实现基类（owner/重入/等待队列/过期） |
| `lock/core/reentrant/mutex/MutexAtomicLock.java` | 兼容旧 `NACOS_LOCK` 的互斥锁 |
| `lock/core/reentrant/mutex/ReentrantAtomicLock.java` | 可重入锁 |
| `lock/core/reentrant/mutex/NonReentrantAtomicLock.java` | 不可重入锁 |
| `lock/factory/LockFactory.java` | SPI 工厂接口 |
| `lock/factory/SimpleLockFactory.java` | `NACOS_LOCK` 工厂 |
| `lock/factory/ReentrantLockFactory.java` | `REENTRANT` 工厂 |
| `lock/factory/NonReentrantLockFactory.java` | `NON_REENTRANT` 工厂 |
| `lock/model/LockInfo.java` | 服务端锁信息 DTO |
| `lock/model/LockKey.java` | 锁标识 (lockType + key) |
| `lock/model/WaitEntry.java` | 等待队列条目 |
| `lock/monitor/LockMetricsMonitor.java` | 指标埋点 |
| `lock/monitor/LockMemoryMonitor.java` | 内存级监控 |
| `lock/persistence/NacosLockSnapshotOperation.java` | Raft 快照保存/加载 |
| `lock/raft/request/MutexLockRequest.java` | Raft 写请求载体 |
| `lock/remote/LockConnectionEventListener.java` | 客户端断连监听 |
| `lock/remote/rpc/handler/LockRequestHandler.java` | gRPC 请求处理 |
| `lock/schedule/LockExpireScanner.java` | 过期锁扫描 |
| `lock/service/LockOperationService.java` | 业务服务接口 |
| `lock/service/impl/LockOperationServiceImpl.java` | 业务服务实现 + CP 状态机 |
| `lock/src/main/resources/META-INF/services/com.alibaba.nacos.lock.factory.LockFactory` | SPI 注册 |

### 2.2 客户端 `api/` + `client/` 模块

| 文件 | 作用 |
|------|------|
| `api/lock/LockService.java` | 客户端锁服务接口 |
| `api/lock/NacosLockFactory.java` | 锁服务工厂（反射创建 `NacosLockService`） |
| `api/lock/common/LockConstants.java` | 锁类型常量 (`NACOS_LOCK`/`REENTRANT`/`NON_REENTRANT`) |
| `api/lock/common/LockNotificationType.java` | 推送通知类型 (`AVAILABLE`/`TIMEOUT`) |
| `api/lock/constant/PropertyConstants.java` | 客户端配置常量 |
| `api/lock/model/LockInstance.java` | 客户端锁实例模型 |
| `api/lock/model/LockResult.java` | 操作结果（成功/重入数/等待位置） |
| `api/lock/remote/AbstractLockRequest.java` | gRPC 请求基类 |
| `api/lock/remote/LockOperationEnum.java` | 操作枚举（6 种） |
| `api/lock/remote/request/LockOperationRequest.java` | 客户端→服务端请求 |
| `api/lock/remote/request/LockNotificationRequest.java` | 服务端→客户端推送 |
| `api/lock/remote/response/LockOperationResponse.java` | 操作响应 |
| `api/lock/remote/response/LockNotificationResponse.java` | 推送响应 |
| `client/lock/NacosLock.java` | JUC `Lock` 实现（`lock`/`tryLock`/`unlock`） |
| `client/lock/NacosLockService.java` | `LockService` 默认实现 |
| `client/lock/NacosLockWatchdog.java` | 续约看门狗 |
| `client/lock/core/NLock.java` | 旧版锁实体（继承 `LockInstance`） |
| `client/lock/core/NLockFactory.java` | 旧版锁工厂 |
| `client/lock/remote/AbstractLockClient.java` | gRPC 客户端基类 |
| `client/lock/remote/LockClient.java` | 客户端接口 |
| `client/lock/remote/grpc/LockGrpcClient.java` | gRPC 客户端实现 |

---

## 3. 资源模型

### 3.1 资源标识：`LockKey`

`lock/model/LockKey.java:28-78`

```java
public class LockKey implements Serializable {
    private String lockType;   // 锁类型，如 "NACOS_LOCK" / "REENTRANT" / "NON_REENTRANT"
    private String key;        // 用户自定义锁名

    // equals/hashCode 基于 (lockType, key) 组合
}
```

- **当前模型全局无 namespace / group / tenant 概念**，仅 `(lockType, key)` 二元组定位一把锁。
- 这与 Config/Naming 的 `namespaceId@group@dataId` 模型不同，是设计取舍（spec 中列为 pending issue）。

### 3.2 服务端锁信息：`LockInfo`

`lock/model/LockInfo.java:28-104`

| 字段 | 含义 |
|------|------|
| `key: LockKey` | 锁标识 |
| `endTime: Long` | 锁到期时间戳（绝对时间，服务端计算） |
| `params: Map` | 扩展参数，内置实现不解析 |
| `owner: String` | 锁持有者标识（客户端 `clientId:threadId` 或 connectionId） |
| `connectionId: String` | 持有者 gRPC 连接 ID（断连清理用） |
| `waitTime: Long` | 等待时长（毫秒），>0 表示可排队 |
| `waiterRetry: boolean` | 是否为等待队列重试请求（FIFO 强制） |

### 3.3 等待条目：`WaitEntry`

`lock/model/WaitEntry.java:30-87`

| 字段 | 含义 |
|------|------|
| `owner` | 等待者标识 |
| `connectionId` | 等待者连接 ID |
| `enqueueTime` | 入队时间 |
| `waitDeadline` | 等待截止时间（0 表示无限等待） |
| `isExpired()` | `waitDeadline > 0 && now >= waitDeadline` |

### 3.4 客户端模型：`LockInstance` / `LockResult`

- `api/lock/model/LockInstance.java`：客户端持有的锁实例，含 `key`、`expiredTime`（lease 时长，由服务端转绝对时间）、`lockType`、`owner`、`waitTime`、`waiterRetry`、`params`。
- `api/lock/model/LockResult.java:30-152`：服务端结构化结果，三种状态：
  - `success(reentrantCount)` —— 成功，含当前重入数
  - `waiting(waitPosition)` —— 已入队，0-based 位置
  - `fail(errorMessage)` —— 失败

---

## 4. 锁类型与 SPI 工厂

### 4.1 三种锁类型常量

`api/lock/common/LockConstants.java:25-32`

```java
public static final String NACOS_LOCK_TYPE       = "NACOS_LOCK";     // 旧版兼容
public static final String REENTRANT_LOCK_TYPE   = "REENTRANT";      // 3.3.0 新增
public static final String NON_REENTRANT_LOCK_TYPE = "NON_REENTRANT"; // 3.3.0 新增
```

### 4.2 `LockFactory` SPI 接口

`lock/factory/LockFactory.java:27-41`

```java
public interface LockFactory {
    String getLockType();                  // 工厂支持的锁类型
    AbstractAtomicLock createLock(String key); // 创建锁实例
}
```

### 4.3 SPI 注册

`lock/src/main/resources/META-INF/services/com.alibaba.nacos.lock.factory.LockFactory`:

```
com.alibaba.nacos.lock.factory.SimpleLockFactory        # -> NACOS_LOCK -> MutexAtomicLock
com.alibaba.nacos.lock.factory.ReentrantLockFactory     # -> REENTRANT -> ReentrantAtomicLock
com.alibaba.nacos.lock.factory.NonReentrantLockFactory   # -> NON_REENTRANT -> NonReentrantAtomicLock
```

### 4.4 工厂加载

`NacosLockManager` 构造时通过 Nacos SPI 加载所有工厂到 `factoryMap`：

```java
// NacosLockManager.java:46-51
public NacosLockManager() {
    Collection<LockFactory> factories = NacosServiceLoader.load(LockFactory.class);
    factoryMap = factories.stream()
        .collect(Collectors.toConcurrentMap(LockFactory::getLockType, f -> f));
}
```

获取锁时按 `lockType` 查找工厂并通过 `computeIfAbsent` 创建：

```java
// NacosLockManager.java:61-64
return atomicLockMap.computeIfAbsent(lockKey, lock -> {
    LockFactory lockFactory = factoryMap.get(lock.getLockType());
    return lockFactory.createLock(lock.getKey());
});
```

**扩展点**：实现自定义 `LockFactory` + `AbstractAtomicLock`，注册到 SPI 文件即可新增锁类型（实验性，spec 警告可能不兼容）。

---

## 5. 服务端核心：AbstractAtomicLock

`lock/core/reentrant/AbstractAtomicLock.java:42-435`

这是所有锁实现的基类，封装了 **owner 跟踪 / 重入计数 / 等待队列 / 自动过期 / 强制释放** 等通用逻辑。

### 5.1 内部状态

```java
// AbstractAtomicLock.java:46-68
private final String key;                       // 锁标识
protected String owner;                          // 当前持有者
protected String connectionId;                  // 持有者连接 ID
protected int reentrantCount;                   // 重入计数
protected long expiredTimestamp;                // 到期绝对时间戳
private final LinkedList<WaitEntry> waitQueue;  // FIFO 等待队列
protected transient ReentrantLock lock;         // 保护内部状态的 JUC 锁（transient，反序列化后需重建）
```

> **关键设计**：每个 `AbstractAtomicLock` 实例内部自带一个 `ReentrantLock`，所有状态读写都先抢这把"实例锁"。这样不同 key 的锁互不阻塞，只在同一把锁内部串行。

### 5.2 tryLock 模板方法

```java
// AbstractAtomicLock.java:396-408
public Boolean tryLock(LockInfo lockInfo) {
    lock.lock();
    try {
        if (lockInfo == null) return false;
        autoExpire();                  // 先尝试自动过期清理
        return doTryLock(lockInfo);    // 委派子类实现具体获取逻辑
    } finally {
        lock.unlock();
    }
}
```

`doTryLock` 是抽象方法，由 `ReentrantAtomicLock` / `NonReentrantAtomicLock` / `MutexAtomicLock` 各自实现。

### 5.3 unLock 模板方法（含 owner 校验旁路）

```java
// AbstractAtomicLock.java:421-434
public Boolean unLock(LockInfo lockInfo) {
    lock.lock();
    try {
        if (lockInfo == null) return false;
        // owner 非空时强制校验；为 null 时绕过校验（系统级释放用）
        if (lockInfo.getOwner() != null && !lockInfo.getOwner().equals(owner)) {
            return false;
        }
        return doUnLock(lockInfo);
    } finally {
        lock.unlock();
    }
}
```

> **注意**：`owner == null` 时跳过校验，是给 `forceRelease`、过期扫描、断连清理等系统级操作留的口子，**不是** 给客户端绕过 owner 校验用的（客户端 owner 在 `LockRequestHandler:72-74` 已默认填 connectionId）。

### 5.4 自动过期

```java
// AbstractAtomicLock.java:319-334
public Boolean autoExpire() {
    lock.lock();
    try {
        if (expiredTimestamp > 0 && System.currentTimeMillis() > expiredTimestamp) {
            owner = null;
            reentrantCount = 0;
            expiredTimestamp = 0;
            connectionId = null;
            return true;
        }
        return false;
    } finally { lock.unlock(); }
}
```

- **惰性检查**：在 `tryLock` / `tryLockAsQueueHead` / `releaseLock` / `expireLock` 等路径中调用，不主动定时器触发。
- **主动扫描**：`LockExpireScanner` 定时遍历并提交 Raft `EXPIRE` 请求（详见 §13）。

### 5.5 renew（续约）

```java
// AbstractAtomicLock.java:346-361
public Boolean renew(LockInfo lockInfo) {
    lock.lock();
    try {
        if (lockInfo == null || lockInfo.getOwner() == null) return false;
        if (!lockInfo.getOwner().equals(owner)) return false;  // 仅 owner 可续
        expiredTimestamp = lockInfo.getEndTime();
        return true;
    } finally { lock.unlock(); }
}
```

### 5.6 forceRelease（强制释放）

```java
// AbstractAtomicLock.java:363-378
public Boolean forceRelease() {
    lock.lock();
    try {
        if (owner == null) return false;
        owner = null; reentrantCount = 0; expiredTimestamp = 0; connectionId = null;
        return true;
    } finally { lock.unlock(); }
}
```

被 `releaseLocksByConnection`（断连清理）和 `expireLock`（过期清理，通过 autoExpire 实现）调用。

### 5.7 等待队列操作

详见 §11。

---

## 6. 三种锁实现对比

### 6.1 `ReentrantAtomicLock`（推荐）

`lock/core/reentrant/mutex/ReentrantAtomicLock.java:34-75`

```java
@Override
protected Boolean doTryLock(LockInfo lockInfo) {
    String requestOwner = lockInfo.getOwner();
    if (owner == null) {                          // 空闲，获取
        owner = requestOwner;
        connectionId = lockInfo.getConnectionId();
        reentrantCount = 1;
        expiredTimestamp = lockInfo.getEndTime();
        return true;
    }
    if (requestOwner.equals(owner)) {             // 同 owner，重入
        reentrantCount++;
        connectionId = lockInfo.getConnectionId();
        expiredTimestamp = lockInfo.getEndTime();
        return true;
    }
    return false;                                  // 他人持有，失败
}

@Override
protected Boolean doUnLock(LockInfo lockInfo) {
    if (reentrantCount <= 0) return false;
    reentrantCount--;
    if (reentrantCount == 0) {                     // 完全释放
        owner = null; connectionId = null; expiredTimestamp = 0;
    }
    return true;
}
```

- 同 owner 可多次获取，`reentrantCount` 递增
- 释放时递减，归零才真正释放
- 续约时刷新 `expiredTimestamp`

### 6.2 `NonReentrantAtomicLock`

`lock/core/reentrant/mutex/NonReentrantAtomicLock.java:34-66`

```java
@Override
protected Boolean doTryLock(LockInfo lockInfo) {
    if (owner == null) {
        owner = lockInfo.getOwner();
        connectionId = lockInfo.getConnectionId();
        reentrantCount = 1;
        expiredTimestamp = lockInfo.getEndTime();
        return true;
    }
    return false;  // 任何第二次获取都失败，哪怕是同一 owner
}
```

- 严格不可重入，**同一 owner 二次获取立即失败**
- 用于早期发现重入 bug
- 客户端 `NacosLock` 还会在本地做 `checkReentrantGuard` 防止自死锁（`client/lock/NacosLock.java:130-136`）

### 6.3 `MutexAtomicLock`（旧版兼容）

`lock/core/reentrant/mutex/MutexAtomicLock.java:35-118`

- 旧版用 `AtomicInteger state` (EMPTY=0 / FULL=1) 跟踪状态
- 3.3.0 改造为继承 `AbstractAtomicLock`，但保留 `state` 字段以兼容旧快照反序列化
- `migrateFromLegacy()` 在快照加载后把旧 `state` 转换为新 `owner/reentrantCount` 模型
- **不强制 owner 校验**（`doUnLock` 不检查 owner，任何客户端可释放）—— 这是旧版实验性行为，新代码应避免使用 `NACOS_LOCK` 类型

### 6.4 对比表

| 特性 | MutexAtomicLock (NACOS_LOCK) | ReentrantAtomicLock (REENTRANT) | NonReentrantAtomicLock (NON_REENTRANT) |
|------|------------------------------|----------------------------------|------------------------------------------|
| 重入 | 否 | **是** | 否 |
| owner 校验 (释放) | 不强制 | **强制** | 强制 |
| 续约 (renew) | 支持 | 支持 | 支持 |
| 等待队列 | 支持 | 支持 | 支持 |
| 推荐场景 | 旧版兼容 | 通用分布式锁 | 防重入 bug |
| 引入版本 | 3.0 | 3.3.0 | 3.3.0 |

---

## 7. LockManager 与内存状态

### 7.1 接口

`lock/LockManager.java`

```java
public interface LockManager {
    AtomicLockService getMutexLock(LockKey lockKey);          // 获取或创建锁
    Map<LockKey, AtomicLockService> showLocks();               // 只读视图
    AtomicLockService removeMutexLock(LockKey lockKey);        // 移除空锁
}
```

### 7.2 实现

`lock/NacosLockManager.java:38-92`

```java
@Service
public class NacosLockManager implements LockManager {
    private final Map<String, LockFactory> factoryMap;         // SPI 工厂
    private final ConcurrentHashMap<LockKey, AtomicLockService> atomicLockMap =  // 全局锁表
        new ConcurrentHashMap<>();

    @Override
    public AtomicLockService getMutexLock(LockKey lockKey) {
        // 校验 lockType/key 非空，且 factoryMap 已注册
        return atomicLockMap.computeIfAbsent(lockKey, lock -> {
            LockFactory lockFactory = factoryMap.get(lock.getLockType());
            return lockFactory.createLock(lock.getKey());
        });
    }

    public ConcurrentHashMap<LockKey, AtomicLockService> getRawLockMap() {
        return atomicLockMap;  // 供快照操作直接访问可变 map
    }
}
```

**关键点**：
- `computeIfAbsent` 保证同一 key 只创建一个锁实例（线程安全）
- `showLocks()` 返回 `Collections.unmodifiableMap`，调用方只能读不能写
- `getRawLockMap()` 是快照操作的"后门"，直接访问可变 map 用于 save/load

---

## 8. CP（Raft）一致性集成

### 8.1 CP Group 注册

`lock/constant/Constants.java:27`

```java
public static final String LOCK_ACQUIRE_SERVICE_GROUP_V2 = "lock_acquire_service_v2";
```

`LockOperationServiceImpl` 继承 `RequestProcessor4CP`，在 `@PostConstruct init()` 中注册自己：

```java
// LockOperationServiceImpl.java:106-115
@PostConstruct
public void init() {
    this.protocol = protocolManager.getCpProtocol();
    this.protocol.addRequestProcessors(Collections.singletonList(this));  // 注册 CP 处理器
    this.defaultExpireTime = EnvUtil.getProperty(..., 30_000L);
    this.maxExpireTime     = EnvUtil.getProperty(..., 1800_000L);
}

@Override
public String group() {
    return Constants.LOCK_ACQUIRE_SERVICE_GROUP_V2;  // CP group 名
}
```

### 8.2 Raft 请求载体：`MutexLockRequest`

`lock/raft/request/MutexLockRequest.java:29-62`

```java
public class MutexLockRequest implements Serializable {
    private LockInfo lockInfo;       // 锁信息
    private boolean forceRelease;    // 是否强制释放
    private String connectionId;     // 客户端连接 ID（断连清理时用）
}
```

所有 6 种操作（ACQUIRE/RELEASE/RENEW/EXPIRE/CANCEL_WAIT/CLEANUP_CONNECTION）都封装为 `MutexLockRequest`，通过 `WriteRequest` 的 `operation` 字段区分。

### 8.3 状态机入口：`onApply`

```java
// LockOperationServiceImpl.java:122-164
@Override
public Response onApply(WriteRequest request) {
    final Lock lock = readLock;     // 用读锁保护状态机执行（与快照写锁互斥）
    lock.lock();
    try {
        LockOperationEnum op = LockOperationEnum.valueOf(request.getOperation());
        MutexLockRequest req = serializer.deserialize(request.getData().toByteArray());
        Object data = switch (op) {
            case ACQUIRE             -> acquireLock(req);
            case RELEASE             -> releaseLock(req);
            case RENEW               -> renewLock(req);
            case EXPIRE              -> expireLock(req);
            case CANCEL_WAIT         -> cancelWaitInternal(req);
            case CLEANUP_CONNECTION  -> cleanupConnection(req);
        };
        ByteString bytes = ByteString.copyFrom(serializer.serialize(data));
        return Response.newBuilder().setSuccess(true).setData(bytes).build();
    } catch (...) {
        return Response.newBuilder().setSuccess(false).setErrMsg(...).build();
    } finally { lock.unlock(); }
}
```

### 8.4 写请求提交

以 `lock()` 为例：

```java
// LockOperationServiceImpl.java:413-449
public LockResult lock(LockInstance lockInstance, String connectionId) {
    MutexLockRequest req = new MutexLockRequest();
    LockInfo lockInfo = new LockInfo();
    lockInfo.setKey(new LockKey(lockInstance.getLockType(), lockInstance.getKey()));
    lockInfo.setOwner(lockInstance.getOwner());
    lockInfo.setConnectionId(connectionId);
    lockInfo.setWaitTime(lockInstance.getWaitTime());
    lockInfo.setWaiterRetry(lockInstance.isWaiterRetry());

    // 关键：服务端把 expiredTime 转为绝对时间戳
    long expiredTime = lockInstance.getExpiredTime();
    if (expiredTime < 0) {
        lockInfo.setEndTime(defaultExpireTime + getNowTimestamp());          // 默认 30s
    } else {
        lockInfo.setEndTime(Math.min(maxExpireTime, expiredTime) + getNowTimestamp()); // 上限 30min
    }
    req.setLockInfo(lockInfo);

    WriteRequest writeRequest = WriteRequest.newBuilder()
        .setGroup(group())                                                    // lock_acquire_service_v2
        .setData(ByteString.copyFrom(serializer.serialize(req)))
        .setOperation(LockOperationEnum.ACQUIRE.name())
        .build();

    Response response = protocol.write(writeRequest);                         // 提交 Raft
    if (response.getSuccess()) {
        return serializer.deserialize(response.getData().toByteArray());
    }
    throw new NacosLockException(response.getErrMsg());
}
```

### 8.5 读路径

```java
// LockOperationServiceImpl.java:595-598
@Override
public Response onRequest(ReadRequest request) {
    return null;   // 当前未实现 CP 线性一致读
}
```

> **重要**：lock 模块当前 **没有** 通过 Raft ReadIndex 提供线性一致读。所有读（如 `showLocks`、`LockExpireScanner` 扫描）都直接读本地内存，可能滞后于已提交但尚未 apply 的日志。这对 acquire/release 决策无影响（决策都在 `onApply` 内做），但运维查询可能看到旧值。

### 8.6 失败行为

- CP leader 不可达时 `protocol.write()` 抛异常 → 客户端收到 `NacosLockException`
- 集群半数以上节点宕机 → Raft 无法选主 → 所有写操作失败
- 锁**不会**因 CP 不可用而"丢失"或"分歧"，符合 CP 语义

---

## 9. 服务层 LockOperationServiceImpl 全流程

### 9.1 acquireLock（获取锁）

`LockOperationServiceImpl.java:212-247`

```java
private LockResult acquireLock(MutexLockRequest request) {
    LockInfo lockInfo = request.getLockInfo();
    AtomicLockService mutexLock = lockManager.getMutexLock(lockInfo.getKey());  // 获取或创建锁实例

    if (mutexLock instanceof AbstractAtomicLock atomicLock) {
        // 1) 等待队列重试：仅队首可获取（FIFO 强制）
        if (lockInfo.isWaiterRetry()) {
            return atomicLock.tryLockAsQueueHead(lockInfo);
        }
        // 2) 严格 FIFO：已有等待者时，新请求必须排在队尾
        if (!lockInfo.isWaiterRetry() && atomicLock.hasWaiters()) {
            if (lockInfo.getWaitTime() > 0) {
                int position = atomicLock.addWaiter(lockInfo);
                return LockResult.waiting(position);
            }
            return LockResult.fail("Lock is held by another owner");
        }
        // 3) 无等待者，直接尝试获取
        Boolean acquired = mutexLock.tryLock(lockInfo);
        if (acquired) {
            return LockResult.success(atomicLock.getReentrantCount());
        }
        // 4) 获取失败，若可等待则入队
        if (lockInfo.getWaitTime() > 0) {
            int position = atomicLock.addWaiter(lockInfo);
            return LockResult.waiting(position);
        }
        return LockResult.fail("Lock is held by another owner");
    }

    // 兜底：非 AbstractAtomicLock 实现的旧接口
    Boolean acquired = mutexLock.tryLock(lockInfo);
    return acquired ? LockResult.success(1) : LockResult.fail("...");
}
```

**关键设计**：
- **FIFO 强制**：一旦锁存在等待者，后续 acquire **必须** 排队，不能"插队"获取（即使锁恰好空闲）。这是通过 `hasWaiters()` 检查实现的。
- **队首重试**：等待者收到推送后重试时设置 `waiterRetry=true`，`tryLockAsQueueHead` 在同一临界区内原子完成"队首校验 + 获取 + 出队"。

### 9.2 releaseLock（释放锁）

`LockOperationServiceImpl.java:166-210`

```java
private LockResult releaseLock(MutexLockRequest request) {
    LockInfo lockInfo = request.getLockInfo();
    AtomicLockService mutexLock = lockManager.showLocks().get(lockInfo.getKey());
    if (mutexLock == null) {
        return LockResult.fail("Lock does not exist or already expired");
    }
    Boolean released;
    if (request.isForceRelease()) {
        released = mutexLock.forceRelease();    // 强制释放（断连清理用）
    } else {
        released = mutexLock.unLock(lockInfo);   // owner 校验释放
    }
    int remainingCount = 0;
    if (mutexLock instanceof AbstractAtomicLock) {
        remainingCount = ((AbstractAtomicLock) mutexLock).getReentrantCount();
    }
    // 锁已空闲后的处理
    if (mutexLock.isClear()) {
        boolean hasWaiters = mutexLock instanceof AbstractAtomicLock
            && ((AbstractAtomicLock) mutexLock).hasWaiters();
        if (hasWaiters) {
            // 通知队首等待者（gRPC 推送）
            WaitEntry entry = ((AbstractAtomicLock) mutexLock).peekFirstWaiter();
            if (entry != null) {
                LockNotificationRequest notification = LockNotificationRequest.available(
                    lockInfo.getKey().getKey(), lockInfo.getKey().getLockType(), entry.getOwner());
                String targetConn = entry.getConnectionId();
                notificationExecutor.submit(() ->
                    rpcPushService.pushWithoutAck(targetConn, notification));
            }
        } else {
            // 无等待者，从 map 移除空锁
            lockManager.removeMutexLock(lockInfo.getKey());
        }
    }
    return released ? LockResult.success(remainingCount)
                    : LockResult.fail("Unlock failed: not held by this owner");
}
```

**释放后流程**：
1. 锁空闲 + 有等待者 → 推送 `AVAILABLE` 通知给队首
2. 锁空闲 + 无等待者 → 从 `atomicLockMap` 移除（防止内存泄漏）

### 9.3 renewLock（续约）

`LockOperationServiceImpl.java:249-253`

```java
private Boolean renewLock(MutexLockRequest request) {
    LockInfo lockInfo = request.getLockInfo();
    AtomicLockService mutexLock = lockManager.getMutexLock(lockInfo.getKey());
    return mutexLock.renew(lockInfo);   // 委派给 AbstractAtomicLock.renew，做 owner 校验
}
```

### 9.4 expireLock（过期）

`LockOperationServiceImpl.java:306-341`

```java
private LockResult expireLock(MutexLockRequest request) {
    LockInfo lockInfo = request.getLockInfo();
    AtomicLockService mutexLock = lockManager.getMutexLock(lockInfo.getKey());
    if (mutexLock instanceof AbstractAtomicLock atomicLock) {
        Boolean expired = atomicLock.autoExpire();   // 二次检查是否真的过期
        if (expired) {
            if (atomicLock.isClear()) {
                if (atomicLock.hasWaiters()) {
                    // 通知队首等待者
                    ...
                } else {
                    lockManager.removeMutexLock(lockInfo.getKey());
                }
            }
            return LockResult.success(0);
        }
    }
    return LockResult.fail("Lock not expired or not found");
}
```

> **关键设计**：`LockExpireScanner` 本地检查过期后提交 Raft `EXPIRE`，但 `onApply` 内的 `autoExpire()` 是**权威检查**——若期间被 `RENEW` 续约，`autoExpire` 返回 false，过期操作被拒绝。这避免了"扫描时过期 → 提交 Raft 期间被续约 → 误释放"的竞态。

### 9.5 cancelWaitInternal（取消等待）

`LockOperationServiceImpl.java:255-278`

```java
private LockResult cancelWaitInternal(MutexLockRequest request) {
    LockInfo lockInfo = request.getLockInfo();
    AtomicLockService mutexLock = lockManager.showLocks().get(lockInfo.getKey());
    if (!(mutexLock instanceof AbstractAtomicLock atomicLock)) {
        return LockResult.success(0);
    }
    boolean removed = atomicLock.removeWaiter(lockInfo.getOwner(), lockInfo.getConnectionId());
    if (removed && atomicLock.isClear()) {
        // 锁空闲 + 移除的是队首 → 通知新队首
        if (atomicLock.hasWaiters()) notifyFirstWaiter(lockInfo.getKey(), atomicLock);
        else lockManager.removeMutexLock(lockInfo.getKey());
    }
    if (removed && !atomicLock.isClear()) {
        // 移除的是队首但锁仍被持有 → 通知新队首
        if (atomicLock.hasWaiters()) notifyFirstWaiter(lockInfo.getKey(), atomicLock);
    }
    return LockResult.success(0);
}
```

### 9.6 cleanupConnection（断连清理）

`LockOperationServiceImpl.java:359-410`

```java
private LockResult cleanupConnection(MutexLockRequest request) {
    String connectionId = request.getConnectionId();
    LockInfo lockInfo = request.getLockInfo();

    if (lockInfo != null && lockInfo.getKey() != null) {
        cleanupSingleLock(lockInfo.getKey(), connectionId);    // 单锁清理
    } else {
        // 批量遍历所有锁
        for (Map.Entry<LockKey, AtomicLockService> entry : lockManager.showLocks().entrySet()) {
            if (entry.getValue() instanceof AbstractAtomicLock) {
                cleanupSingleLock(entry.getKey(), connectionId);
            }
        }
    }
    return LockResult.success(0);
}

private void cleanupSingleLock(LockKey lockKey, String connectionId) {
    AtomicLockService lockService = lockManager.showLocks().get(lockKey);
    if (!(lockService instanceof AbstractAtomicLock atomicLock)) return;

    // Step 1: 强制释放（若持有者就是断连的 connection）
    boolean wasHeld = false;
    if (connectionId.equals(atomicLock.getConnectionId())) {
        if (atomicLock.getOwner() != null) {
            wasHeld = atomicLock.forceRelease();
        }
    }
    // Step 2: 移除该连接的所有等待条目
    atomicLock.removeWaiterByConnection(connectionId);
    // Step 3: 释放后通知新队首 / 移除空锁
    if (wasHeld && atomicLock.isClear()) {
        if (atomicLock.hasWaiters()) notifyFirstWaiter(lockKey, atomicLock);
        else lockManager.removeMutexLock(lockKey);
    }
    // Step 4: 即使锁仍被他人持有，若移除的是队首也要通知新队首
    if (!wasHeld && !atomicLock.isClear()) {
        if (atomicLock.hasWaiters()) notifyFirstWaiter(lockKey, atomicLock);
    }
}
```

> **注意**：批量清理通过**单个** `CLEANUP_CONNECTION` Raft 请求完成，遍历在 `onApply` 内进行，保证集群一致。`releaseLocksByConnection()` 是公开入口，由 `LockConnectionEventListener` 在客户端断连时调用。

---

## 10. gRPC 入口：LockRequestHandler

`lock/remote/rpc/handler/LockRequestHandler.java:42-113`

```java
@Since("3.0.0")
@Component
public class LockRequestHandler extends RequestHandler<LockOperationRequest, LockOperationResponse> {

    @Override
    public LockOperationResponse handle(LockOperationRequest request, RequestMeta meta) {
        LockInstance lockInstance = request.getLockInstance();

        // 1) 入参校验
        if (lockInstance == null) return fail("LockInstance cannot be null");
        if (lockInstance.getKey() == null || lockInstance.getKey().isEmpty()) return fail("...");
        String connectionId = meta.getConnectionId();   // 从 gRPC 元数据取连接 ID

        // 2) owner 默认填 connectionId（关键：客户端不传 owner 时用连接 ID 兜底）
        if (lockInstance.getOwner() == null || lockInstance.getOwner().isEmpty()) {
            lockInstance.setOwner(connectionId);
        }

        // 3) lockType 白名单校验
        String lockType = lockInstance.getLockType();
        if (!REENTRANT.equals(lockType) && !NON_REENTRANT.equals(lockType) && !NACOS_LOCK.equals(lockType)) {
            return fail("Invalid lock type: " + lockType);
        }

        // 4) 路由到对应 service 方法
        switch (request.getLockOperationEnum()) {
            case ACQUIRE:
                if (lockInstance.getExpiredTime() == 0) return fail("...");
                return success(lockOperationService.lock(lockInstance, connectionId));
            case RELEASE:
                return success(lockOperationService.unLock(lockInstance));
            case RENEW:
                if (lockInstance.getExpiredTime() == 0) return fail("...");
                return success(lockOperationService.renew(lockInstance));
            case CANCEL_WAIT:
                return success(lockOperationService.cancelWait(lockInstance, connectionId));
            default:
                return fail("There is no Handler of such operations!");
        }
    }
}
```

**注意**：`EXPIRE` 和 `CLEANUP_CONNECTION` 是**内部操作**，不通过 gRPC 暴露给客户端，只由服务端 `LockExpireScanner` 和 `LockConnectionEventListener` 内部调用。

---

## 11. 等待队列与推送通知机制

### 11.1 等待队列数据结构

`AbstractAtomicLock.java:56` —— 每个 `AbstractAtomicLock` 自带一个 `LinkedList<WaitEntry> waitQueue`。

### 11.2 入队：`addWaiter`

`AbstractAtomicLock.java:157-177`

```java
public int addWaiter(LockInfo lockInfo) {
    lock.lock();
    try {
        long now = System.currentTimeMillis();
        long deadline = lockInfo.getWaitTime() > 0 ? now + lockInfo.getWaitTime() : 0;  // 0=无限等
        // 幂等：同 owner+connection 已在队列则只更新 deadline
        for (int i = 0; i < waitQueue.size(); i++) {
            WaitEntry existing = waitQueue.get(i);
            if (existing.getOwner().equals(lockInfo.getOwner())
                && existing.getConnectionId().equals(lockInfo.getConnectionId())) {
                existing.setWaitDeadline(deadline);
                return i;
            }
        }
        WaitEntry entry = new WaitEntry(lockInfo.getOwner(), lockInfo.getConnectionId(), now, deadline);
        waitQueue.add(entry);
        return waitQueue.size() - 1;
    } finally { lock.unlock(); }
}
```

### 11.3 队首原子获取：`tryLockAsQueueHead`

`AbstractAtomicLock.java:216-241`

```java
public LockResult tryLockAsQueueHead(LockInfo lockInfo) {
    lock.lock();
    try {
        if (lockInfo == null) return LockResult.fail("...");
        WaitEntry head = peekFirstWaiterUnderLock();   // 跳过已过期
        if (!isSameWaiter(head, lockInfo)) {
            return LockResult.fail("Only queue head waiter can retry");
        }
        autoExpire();                                  // 二次过期检查
        Boolean acquired = doTryLock(lockInfo);        // 子类获取逻辑
        if (acquired) {
            waitQueue.poll();                          // 出队
            return LockResult.success(reentrantCount);
        }
        if (lockInfo.getWaitTime() > 0) return LockResult.waiting(0);
        return LockResult.fail("Lock is held by another owner");
    } finally { lock.unlock(); }
}
```

**关键**：把"队首校验 + 获取 + 出队"放在**同一临界区**，避免并发 cancel/cleanup 在校验与出队之间篡改队列。

### 11.4 推送通知

锁释放或过期后，若队首存在等待者，通过 `RpcPushService.pushWithoutAck` 异步推送 `LockNotificationRequest`：

```java
// LockOperationServiceImpl.java:537-554
private void notifyFirstWaiter(LockKey lockKey, AbstractAtomicLock atomicLock) {
    WaitEntry entry = atomicLock.peekFirstWaiter();
    if (entry == null) return;
    String targetConnectionId = entry.getConnectionId();
    LockNotificationRequest notification = LockNotificationRequest.available(
        lockKey.getKey(), lockKey.getLockType(), entry.getOwner());
    notificationExecutor.submit(() -> {
        try {
            rpcPushService.pushWithoutAck(targetConnectionId, notification);
        } catch (Exception e) {
            LOGGER.warn("Lock: failed to notify waiter, key={}, connectionId={}", ...);
        }
    });
}
```

- 推送类型 `LockNotificationType.AVAILABLE`（`api/lock/common/LockNotificationType.java`）
- `pushWithoutAck`：服务端推送后不等客户端 ACK，若客户端错过推送会通过轮询重试（见 §15.3）

### 11.5 过期等待条目清理

`AbstractAtomicLock.java:291-300`

```java
private WaitEntry peekFirstWaiterUnderLock() {
    while (!waitQueue.isEmpty()) {
        WaitEntry entry = waitQueue.peek();
        if (!entry.isExpired()) return entry;   // 返回首个未过期
        waitQueue.poll();                       // 顺手丢弃过期
    }
    return null;
}
```

每次 peek 队首时**副作用清理**过期条目，避免等待队列无限增长。

---

## 12. 连接断开清理

### 12.1 监听器

`lock/remote/LockConnectionEventListener.java:36-62`

```java
@Component
public class LockConnectionEventListener extends ClientConnectionEventListener {

    @Override
    public void clientConnected(Connection connect) { }   // 无操作

    @Override
    public void clientDisConnected(Connection connect) {
        String connectionId = connect.getMetaInfo().getConnectionId();
        LOGGER.info("Lock: client disconnected, connectionId={}, cleaning up locks", connectionId);
        try {
            lockOperationService.releaseLocksByConnection(connectionId);
        } catch (Exception e) {
            LOGGER.error("Lock: failed to clean up locks for disconnected connectionId={}", ...);
        }
    }
}
```

### 12.2 清理流程

`LockOperationServiceImpl.releaseLocksByConnection()` → `cleanupConnectionViaRaft()` → 提交 `CLEANUP_CONNECTION` Raft 请求 → `onApply` 内 `cleanupConnection()` → 遍历所有锁，对每把锁：
1. 若 `connectionId == atomicLock.getConnectionId()` → `forceRelease()`
2. `removeWaiterByConnection(connectionId)` 移除该连接的所有等待条目
3. 释放后若空闲 → 通知新队首或移除空锁

**为何用 Raft**：清理是状态变更，必须集群一致。否则不同节点对"哪些锁被释放"会有分歧。

---

## 13. 过期扫描：LockExpireScanner

`lock/schedule/LockExpireScanner.java:43-110`

```java
@Component
public class LockExpireScanner {

    @Scheduled(fixedDelayString = "${nacos.lock.expire.scan.interval:1000}")
    public void scanExpiredLocks() {
        try { doScan(); } catch (Exception e) { LOGGER.error("Lock: error during expire scan", e); }
    }

    private void doScan() {
        for (Map.Entry<LockKey, AtomicLockService> entry : lockManager.showLocks().entrySet()) {
            LockKey lockKey = entry.getKey();
            AtomicLockService lockService = entry.getValue();
            if (lockService instanceof AbstractAtomicLock atomicLock) {
                // 1) 空壳锁（无 owner 无等待者）直接移除
                if (atomicLock.isClear() && !atomicLock.hasWaiters()) {
                    lockManager.removeMutexLock(lockKey);
                    continue;
                }
                // 2) 本地过期检查（优化，避免无谓 Raft 提交）
                if (atomicLock.getOwner() != null
                    && atomicLock.getExpiredTimestamp() > 0
                    && System.currentTimeMillis() > atomicLock.getExpiredTimestamp()) {
                    expireLockViaRaft(lockKey, atomicLock);
                }
            }
        }
    }

    private void expireLockViaRaft(LockKey lockKey, AbstractAtomicLock atomicLock) {
        String owner = atomicLock.getOwner();
        if (owner == null) return;
        LockInstance instance = new LockInstance();
        instance.setKey(lockKey.getKey());
        instance.setLockType(lockKey.getLockType());
        instance.setOwner(owner);
        try { lockOperationService.expire(instance); }
        catch (Exception e) { LOGGER.warn("Lock: failed to expire lock via Raft, key={}", ...); }
    }
}
```

**核心思想**：
- 本地检查只是**优化**，避免每次扫描都提交 Raft。
- **权威检查在 `onApply` 内的 `autoExpire()`**：若提交 Raft 期间被 `RENEW` 续约，`autoExpire` 返回 false，过期被拒绝。
- 默认扫描间隔 1 秒（可配 `nacos.lock.expire.scan.interval`）。

---

## 14. 快照持久化：NacosLockSnapshotOperation

`lock/persistence/NacosLockSnapshotOperation.java:58-211`

### 14.1 快照保存

```java
// onSnapshotSave -> writeSnapshot -> dumpSnapshot
InputStream dumpSnapshot() {
    Map<LockKey, AtomicLockService> lockMap = new HashMap<>(lockManager.showLocks());
    return new ByteArrayInputStream(serializer.serialize(lockMap));
}

boolean writeSnapshot(Writer writer) throws IOException {
    String outputFile = Paths.get(writer.getPath(), "nacos_lock.zip").toString();
    Checksum checksum = new CRC64();
    try (InputStream is = dumpSnapshot()) {
        DiskUtils.compressIntoZipFile("lock", is, outputFile, checksum);
    }
    LocalFileMeta meta = new LocalFileMeta();
    meta.append("checksum", Long.toHexString(checksum.getValue()));
    return writer.addFile("nacos_lock.zip", meta);
}
```

- 序列化整个 `atomicLockMap` → Hessian 序列化 → ZIP 压缩（带 CRC64 校验）
- 快照文件名固定为 `nacos_lock.zip`
- 保存时持有 `ReentrantReadWriteLock.writeLock`，与 `onApply` 的 `readLock` 互斥，保证快照一致

### 14.2 快照加载

```java
// onSnapshotLoad -> readSnapshot -> loadSnapshot
void loadSnapshot(byte[] snapshotBytes) {
    Map<LockKey, AtomicLockService> snapshotData = serializer.deserialize(snapshotBytes);
    ConcurrentHashMap<LockKey, AtomicLockService> newData = new ConcurrentHashMap<>(snapshotData);

    // 重建 transient 字段（Hessian 不调用 readObject）
    for (AtomicLockService lockService : newData.values()) {
        if (lockService instanceof AbstractAtomicLock) {
            ((AbstractAtomicLock) lockService).initTransientFields();   // 重建 ReentrantLock
        }
    }

    ConcurrentHashMap<LockKey, AtomicLockService> lockMap = getRawLockMap();
    lockMap.putAll(newData);                  // 合并到当前 map
    migrateMutexAtomicLocks(lockMap);         // 旧版 MutexAtomicLock 迁移
}
```

### 14.3 反序列化陷阱：transient `ReentrantLock`

`AbstractAtomicLock.java:65-85`

```java
protected transient ReentrantLock lock = new ReentrantLock();

public void initTransientFields() {
    if (this.lock == null) {
        this.lock = new ReentrantLock();
    }
}
```

- `ReentrantLock` 不可序列化，标记 `transient`
- Hessian 反序列化**不调用** `readObject()`，因此 `lock` 为 null
- 加载后必须显式调用 `initTransientFields()`，否则任何锁操作 NPE

### 14.4 旧版迁移

`MutexAtomicLock.migrateFromLegacy()`（`lock/core/reentrant/mutex/MutexAtomicLock.java:70-93`）：
- 旧版用 `AtomicInteger state` (EMPTY=0 / FULL=1)
- Hessian 把 `AtomicInteger` 序列化为 int 值，反序列化到 `Integer state` 字段
- 迁移时按 state 值转换为新的 `owner/reentrantCount` 模型（state=1 时 owner 设为 `"legacy-migrated"`）

### 14.5 校验

加载时检查 CRC64：

```java
if (fileMeta.getFileMeta().containsKey("checksum")
    && !Objects.equals(Long.toHexString(checksum.getValue()), fileMeta.get("checksum"))) {
    throw new IllegalArgumentException("Snapshot checksum failed");
}
```

---

## 15. 客户端 SDK 实现

### 15.1 入口：`NacosLockFactory`

`api/lock/NacosLockFactory.java:39-48`

```java
public static LockService createLockService(Properties properties) throws NacosException {
    try {
        Class<?> cls = Class.forName("com.alibaba.nacos.client.lock.NacosLockService");
        Constructor c = cls.getConstructor(Properties.class);
        return (LockService) c.newInstance(properties);
    } catch (Throwable e) {
        throw new NacosException(...);
    }
}
```

通过反射实例化客户端实现 `NacosLockService`，保持 `api` 模块对 `client` 模块的解耦（与 Naming/Config 的工厂模式一致）。

### 15.2 `NacosLockService`（`LockService` 默认实现）

`client/lock/NacosLockService.java:45-168`

```java
public NacosLockService(Properties properties) throws NacosException {
    NacosClientProperties nacosClientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
    this.serverListManager = new NamingServerListManager(properties);    // 复用 Naming 的 ServerListManager
    serverListManager.start();
    this.securityProxy = new SecurityProxy(serverListManager, ...);
    initSecurityProxy(nacosClientProperties);                            // 鉴权 token 定时刷新
    this.lockGrpcClient = new LockGrpcClient(nacosClientProperties, serverListManager, securityProxy);
    this.watchdog = new NacosLockWatchdog();
    this.clientId = UUID.randomUUID().toString();
}
```

提供两种 API：
1. **直接 LockService API**：`lock(instance)` / `unLock(instance)` —— 返回 Boolean，简单互斥
2. **JUC Lock API**：`getReentrantLock(key)` / `getNonReentrantLock(key)` —— 返回实现 `java.util.concurrent.locks.Lock` 的 `NacosLock`

### 15.3 `LockGrpcClient`（gRPC 客户端）

`client/lock/remote/grpc/LockGrpcClient.java:71-475`

核心能力：

#### a) 能力协商

```java
// LockGrpcClient.java:471-474
private boolean isAbilitySupportedByServer() {
    return rpcClient.getConnectionAbility(AbilityKey.SERVER_DISTRIBUTED_LOCK) == AbilityStatus.SUPPORTED;
}
```

每个操作前检查 `SERVER_DISTRIBUTED_LOCK` 能力，不支持则抛 `SERVER_NOT_IMPLEMENTED`。

#### b) `lock()` —— 带等待队列的获取

`LockGrpcClient.java:137-221`，关键流程：

```
1. registerForNotification(key, owner)       // 先注册 future，避免 TOCTOU 竞态
2. loop:
   a. 构造 LockOperationRequest(ACQUIRE)
      - 首次失败后 copy.setWaiterRetry(true)（FIFO 标记）
   b. response = requestToServer(request)
   c. 若 lockResult.isSuccess() -> cancelWait + return true
   d. 否则 waitForNotification(key, owner, pollTimeout)
      - 收到 AVAILABLE -> 继续循环重试
      - 收到 TIMEOUT -> return false
      - 超时 (null) -> 继续循环（直到 deadline）
3. finally:
   - 获取成功 -> cancelWait(key, owner)（仅本地）
   - 失败       -> cancelWait(key, lockType, owner)（本地 + 服务端）
```

**关键设计**：
- **推送 + 轮询混合**：主要靠服务端 gRPC 推送通知，但客户端也会在 `pollTimeout`（默认 2s + 随机扰动）后主动重试，避免漏收推送导致永久阻塞。
- **future 复用**（`waitForNotification` 用 `compute` 而非 `put`）：避免连续两次 `waitForNotification` 之间推送丢失。
- **waiterRetry**：第一次失败后标记，告知服务端这是队列重试，可走 `tryLockAsQueueHead`。

#### c) 等待通知注册

```java
// LockGrpcClient.java:363-373
public void registerForNotification(String lockKey, String owner) {
    String waitKey = buildWaitKey(lockKey, owner);
    CompletableFuture<LockNotificationType> oldFuture =
        notificationFutures.put(waitKey, new CompletableFuture<>());
    if (oldFuture != null && !oldFuture.isDone()) {
        oldFuture.complete(LockNotificationType.AVAILABLE);  // 唤醒旧 future
    }
}
```

#### d) 服务端推送处理

```java
// LockGrpcClient.java:110-128
rpcClient.registerServerRequestHandler(new ServerRequestHandler() {
    @Override
    public Response requestReply(Request request, Connection connection) {
        if (request instanceof LockNotificationRequest) {
            LockNotificationRequest notification = (LockNotificationRequest) request;
            String waitKey = buildWaitKey(notification.getLockKey(), notification.getOwner());
            CompletableFuture<LockNotificationType> future = notificationFutures.get(waitKey);
            if (future != null) {
                future.complete(notification.getNotificationType());   // 唤醒等待线程
            }
            return new LockNotificationResponse();
        }
        return null;
    }
});
```

#### e) `unLockWithResult` / `renewWithResult` / `lockWithResult` / `cancelWaitWithResult`

封装 gRPC 请求，处理 `LockResult` 与旧版 `Boolean` 兼容（`response.getLockResult()` 优先，回退到 `response.getResult()`）。

### 15.4 `NacosLock`（JUC `Lock` 实现）

`client/lock/NacosLock.java:49-349`

这是 3.3.0 新增的、符合 `java.util.concurrent.locks.Lock` 接口的分布式锁包装。

#### a) owner 标识

```java
// NacosLock.java:108-110
private String currentOwner() {
    return clientId + ":" + Thread.currentThread().getId();   // 客户端ID + 线程ID
}
```

owner = `clientId:threadId`，同一进程内不同线程视为不同 owner。

#### b) 本地重入计数

```java
private final ThreadLocal<Integer> localReentrantCount = ThreadLocal.withInitial(() -> 0);
```

- 客户端用 `ThreadLocal` 跟踪当前线程的重入数，与服务端 `reentrantCount` 对应。
- `lock()` 成功后 `localReentrantCount.set(count + 1)`，`unlock()` 成功后 `set(count - 1)`。
- 仅当前线程可释放（`count <= 0` 时 `unlock()` 抛 `IllegalMonitorStateException`）。

#### c) `lock()`（阻塞获取）

```java
// NacosLock.java:138-175
public void lock() {
    checkReentrantGuard();            // 不可重入锁的本地防御
    boolean firstAttempt = true;
    while (true) {
        LockInstance instance = buildInstance(-1);   // expiredTime=-1 -> 服务端用默认值
        instance.setWaitTime(DEFAULT_SERVER_WAIT_TIME_MS);  // 5min
        if (!firstAttempt) instance.setWaiterRetry(true);
        grpcClient.registerForNotification(key, currentOwner());
        LockResult result = grpcClient.lockWithResult(instance);
        if (result.isSuccess()) {
            grpcClient.cancelWait(key, currentOwner());
            localReentrantCount.set(localReentrantCount.get() + 1);
            if (result.getReentrantCount() == 1) {    // 首次获取
                watchdog.register(key, grpcClient, instance);   // 注册续约
            }
            return;
        }
        firstAttempt = false;
        grpcClient.waitForNotification(key, currentOwner(), NOTIFICATION_POLL_TIMEOUT_MS);
    }
}
```

#### d) `tryLock()` / `tryLock(time, unit)`

- `tryLock()`：不等待，单次尝试
- `tryLock(time, unit)`：限时等待，逻辑类似 `lock()` 但带 deadline

#### e) `unlock()`

```java
// NacosLock.java:287-334
public void unlock() {
    if (inUnlock.get()) throw new IllegalMonitorStateException("Recursive unlock() detected");
    inUnlock.set(true);
    try {
        int count = localReentrantCount.get();
        if (count <= 0) throw new IllegalMonitorStateException("Current thread does not hold the lock");
        LockInstance instance = buildInstance(0);
        LockResult result = grpcClient.unLockWithResult(instance);
        if (result.isSuccess()) {
            localReentrantCount.set(count - 1);
            if (result.getReentrantCount() == 0) {    // 完全释放
                watchdog.unregister(key);
                localReentrantCount.remove();
            }
        } else {
            // 服务端拒绝（owner 不匹配等）-> 清理本地状态，避免锁永久不可用
            localReentrantCount.set(0);
            watchdog.unregister(key);
            localReentrantCount.remove();
            throw new IllegalMonitorStateException("Unlock rejected by server: " + result.getErrorMessage());
        }
    } catch (NacosException e) {
        // 服务端可能已释放（如过期）-> 清理本地状态
        localReentrantCount.set(0);
        watchdog.unregister(key);
        localReentrantCount.remove();
        throw new IllegalStateException("Failed to unlock: " + key, e);
    } finally {
        inUnlock.remove();
    }
}
```

**容错策略**：解锁失败时**强制清理本地状态**，防止锁对象因服务端状态丢失而永久不可用（重要设计）。

#### f) `checkReentrantGuard()`

```java
// NacosLock.java:130-136
private void checkReentrantGuard() {
    if (LockConstants.NON_REENTRANT_LOCK_TYPE.equals(lockType) && localReentrantCount.get() > 0) {
        throw new IllegalMonitorStateException("Non-reentrant lock does not allow reentry");
    }
}
```

防止 `NON_REENTRANT` 锁同线程二次获取导致**自死锁**（自己排队等自己释放）。

#### g) 不支持 `Condition`

```java
public Condition newCondition() {
    throw new UnsupportedOperationException("Condition not supported in Nacos distributed lock");
}
```

---

## 16. Watchdog 续约机制

`client/lock/NacosLockWatchdog.java:39-144`

### 16.1 设计

- 每个持有锁的客户端后台启动定时续约任务
- 默认续约间隔 10 秒（`DEFAULT_RENEW_INTERVAL_MS = 10000L`）
- 续约失败（owner 不匹配 / 服务端错误）→ 自动注销，锁最终会因过期被服务端清理

### 16.2 注册

```java
// NacosLockWatchdog.java:75-106
public void register(String key, LockGrpcClient client, LockInstance instance) {
    if (shutdown.get() || renewIntervalMs <= 0) return;
    lockInstances.put(key, instance);
    long ttl = instance.getExpiredTime() > 0 ? instance.getExpiredTime() : DEFAULT_RENEW_INTERVAL_MS * 3;
    long interval = Math.max(1000L, Math.min(ttl / 3, renewIntervalMs));   // 续约间隔 ≤ TTL/3
    ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
        try {
            LockInstance lockInstance = lockInstances.get(key);
            if (lockInstance == null) return;
            Boolean renewed = client.renew(lockInstance);
            if (renewed == null || !renewed) {
                unregister(key);    // 续约失败 -> 注销
            }
        } catch (Exception e) {
            unregister(key);
        }
    }, interval, interval, TimeUnit.MILLISECONDS);
    renewTasks.put(key, future);
}
```

### 16.3 续约间隔策略

- `interval = max(1000ms, min(TTL/3, renewIntervalMs))`
- 即至少 1 秒，至多 `TTL/3`（保证在 TTL 到期前至少续约 2 次）

### 16.4 注册时机

`NacosLock.java:154-156`：
```java
if (result.getReentrantCount() == 1) {   // 仅首次获取时注册
    watchdog.register(key, grpcClient, instance);
}
```

重入时不重复注册（避免重复续约任务）。

### 16.5 注销时机

- `unlock()` 完全释放时（`reentrantCount == 0`）→ `watchdog.unregister(key)`
- 续约失败时 → 自动 unregister

### 16.6 服务端续约逻辑

```java
// AbstractAtomicLock.java:346-361
public Boolean renew(LockInfo lockInfo) {
    lock.lock();
    try {
        if (lockInfo == null || lockInfo.getOwner() == null) return false;
        if (!lockInfo.getOwner().equals(owner)) return false;   // 仅 owner 可续
        expiredTimestamp = lockInfo.getEndTime();              // 刷新到期时间
        return true;
    } finally { lock.unlock(); }
}
```

---

## 17. 可观测性：Metrics 与切面

### 17.1 `RequestLockAspect`

`lock/aspect/RequestLockAspect.java:36-62`

```java
@Aspect
@Component
public class RequestLockAspect {
    @Around("execution(* com.alibaba.nacos.core.remote.RequestHandler.handleRequest(..)) "
          + "&& target(...) && args(request, meta)")
    public Object lockMeterPoint(ProceedingJoinPoint pjp, LockOperationRequest request, RequestMeta meta) {
        long st = System.currentTimeMillis();
        try {
            LockMetricsMonitor.getTotalMeter(request.getLockOperationEnum()).incrementAndGet();
            LockOperationResponse result = (LockOperationResponse) pjp.proceed();
            if (result.isSuccess()) {
                LockMetricsMonitor.getSuccessMeter(request.getLockOperationEnum()).incrementAndGet();
            }
            return result;
        } finally {
            LockMetricsMonitor.getLockHandlerTimer().record(System.currentTimeMillis() - st, TimeUnit.MILLISECONDS);
        }
    }
}
```

### 17.2 `LockMetricsMonitor`

`lock/monitor/LockMetricsMonitor.java:33-131`

注册的指标（注册中心 `NacosMeterRegistryCenter.LOCK_STABLE_REGISTRY`）：

| 指标名 | 含义 |
|--------|------|
| `grpcLockTotal` | ACQUIRE 总数 |
| `grpcLockSuccess` | ACQUIRE 成功数 |
| `grpcUnLockTotal` | RELEASE 总数 |
| `grpcUnLockSuccess` | RELEASE 成功数 |
| `grpcRenewTotal` | RENEW 总数 |
| `grpcRenewSuccess` | RENEW 成功数 |
| `aliveLockCount` | 存活锁数量 |
| `lockHandlerRt` | handler 延迟（Timer） |

> spec 要求：metrics 不得包含 raw lock key、params、credentials 等敏感标签，当前实现遵守。

---

## 18. 关键时序图

### 18.1 加锁成功时序（无等待者）

```
Client                LockGrpcClient         LockRequestHandler      LockOperationServiceImpl        Raft/JRaft            NacosLockManager
  │                        │                       │                          │                          │                       │
  │ lock(key)              │                       │                          │                          │                       │
  ├───────────────────────>│                       │                          │                          │                       │
  │                        │ LockOperationRequest(ACQUIRE)                   │                          │                       │
  │                        ├──────────────────────>│                          │                          │                       │
  │                        │                       │ lock(instance, connId)  │                          │                       │
  │                        │                       ├─────────────────────────>│                          │                       │
  │                        │                       │                          │ WriteRequest(ACQUIRE)   │                       │
  │                        │                       │                          ├─────────────────────────>│                       │
  │                        │                       │                          │                          │ Raft 复制+提交        │
  │                        │                       │                          │ onApply(ACQUIRE)         │                       │
  │                        │                       │                          │   acquireLock(req)       │                       │
  │                        │                       │                          │   ├──────────────────────────────────────────────>│ getMutexLock(key)
  │                        │                       │                          │   │  tryLock(lockInfo)   │   <- AtomicLock       │
  │                        │                       │                          │   │  doTryLock          │                       │
  │                        │                       │                          │   │  -> owner set       │                       │
  │                        │                       │                          │   │                     │                       │
  │                        │                       │                          │   LockResult.success(1)│                       │
  │                        │                       │                          │<─────────────────────────┤                       │
  │                        │                       │   Response(success)     │                          │                       │
  │                        │                       │<─────────────────────────┤                          │                       │
  │                        │ LockOperationResponse │                          │                          │                       │
  │                        │<──────────────────────┤                          │                          │                       │
  │ LockResult.success     │                       │                          │                          │                       │
  │<───────────────────────┤                       │                          │                          │                       │
```

### 18.2 加锁排队 + 推送唤醒时序

```
ClientA (持锁)   ClientB (排队)       Server (LockOperationServiceImpl + Raft)
   │                 │                          │
   │ lock(B-key)     │                          │
   │                 │ ACQUIRE(req, waitTime>0) │
   │                 ├─────────────────────────>│
   │                 │                          │ acquireLock: hasWaiters=false, tryLock 失败
   │                 │                          │ addWaiter(B) -> 队列=[B]
   │                 │                          │ LockResult.waiting(pos=0)
   │                 │<─────────────────────────┤
   │                 │ waitForNotification      │
   │                 │ (注册 future)             │
   │                 │                          │
   │ unlock(B-key)   │                          │
   ├──────────────────────────────────────────>│ RELEASE
   │                 │                          │ releaseLock: doUnLock -> owner=null
   │                 │                          │ isClear + hasWaiters(B)
   │                 │                          │ peekFirstWaiter -> B
   │                 │                          │ pushWithoutAck(B, AVAILABLE)  ◄── gRPC 推送
   │                 │<─────────────────────────┤
   │                 │ future.complete(AVAILABLE)│
   │                 │ 重试 ACQUIRE (waiterRetry=true) │
   │                 ├─────────────────────────>│
   │                 │                          │ tryLockAsQueueHead -> 校验队首=B
   │                 │                          │ doTryLock -> owner=B
   │                 │                          │ waitQueue.poll() (B 出队)
   │                 │                          │ LockResult.success(1)
   │                 │<─────────────────────────┤
   │                 │ cancelWait (本地)         │
```

### 18.3 客户端断连清理时序

```
Client (crash)              Server
   │                          │
   │ (gRPC 连接断开)          │
   │                       │  ConnectionEvent(DISCONNECTED)
   │                       │  LockConnectionEventListener.clientDisConnected
   │                       │      │
   │                       │      ▼
   │                       │  releaseLocksByConnection(connId)
   │                       │      │ CLEANUP_CONNECTION Raft 请求
   │                       │      ▼
   │                       │  onApply(CLEANUP_CONNECTION)
   │                       │  遍历所有锁:
   │                       │    - 若 atomicLock.connectionId == connId -> forceRelease
   │                       │    - removeWaiterByConnection(connId)
   │                       │    - 释放后通知新队首 / 移除空锁
```

### 18.4 过期扫描时序

```
LockExpireScanner (1s 周期)        LockOperationServiceImpl        Raft
   │                                    │                            │
   │ doScan()                           │                            │
   │ for each lock:                     │                            │
   │   if now > expiredTimestamp:       │                            │
   │     expireLockViaRaft()            │                            │
   │       lockOperationService.expire  │                            │
   │       ├───────────────────────────>│ EXPIRE WriteRequest       │
   │       │                            ├───────────────────────────>│
   │       │                            │                            │ Raft 复制+提交
   │       │                            │ onApply(EXPIRE)            │
   │       │                            │   expireLock(req)          │
   │       │                            │     autoExpire()  ◄── 权威检查
   │       │                            │       (若已被 RENEW 则返回 false，过期被拒)
   │       │                            │     若成功 -> 通知队首 / 移除空锁
   │       │                            │<───────────────────────────┤
   │       │<───────────────────────────┤                            │
```

---

## 19. 线程安全模型

### 19.1 三层锁

| 层级 | 锁 | 作用范围 | 用途 |
|------|-----|---------|------|
| L1 | `AbstractAtomicLock.lock`（实例级 `ReentrantLock`） | 单个 key | 保护单个锁的 owner/count/queue |
| L2 | `ConcurrentHashMap`（CAS） | 全局 lock map | 保护 `atomicLockMap` 的 put/get/remove |
| L3 | `ReentrantReadWriteLock`（service 级） | 整个 lock 模块 | `onApply` 用 readLock，快照 save/load 用 writeLock |

### 19.2 读写锁的取舍

```java
// LockOperationServiceImpl.java:75-77
private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

// onApply 用 readLock
@Override
public Response onApply(WriteRequest request) {
    final Lock lock = readLock;
    lock.lock();
    try { ... } finally { lock.unlock(); }
}

// 快照 save/load 用 writeLock（通过 loadSnapshotOperate 传入）
public List<SnapshotOperation> loadSnapshotOperate() {
    return Collections.singletonList(new NacosLockSnapshotOperation(lockManager, lock.writeLock()));
}
```

- 多个 Raft apply 可并发（虽然 JRaft 默认单线程 apply，这里仍用 readLock 是防御性设计）
- 快照保存/加载时阻塞所有 apply，保证快照一致性

### 19.3 实例级 `ReentrantLock` 的 transient 陷阱

`AbstractAtomicLock.lock` 是 `transient`，Hessian 反序列化后为 null。**必须** 调用 `initTransientFields()` 重建，否则 NPE。`NacosLockSnapshotOperation.loadSnapshot` 已处理。

### 19.4 客户端 `ThreadLocal` + 服务端计数

- 客户端 `NacosLock.localReentrantCount`（ThreadLocal）与服务端 `AbstractAtomicLock.reentrantCount` **必须保持同步**
- 客户端解锁失败时**强制清零**本地计数（`NacosLock.java:310-313`），避免永久不可用
- 跨线程锁传递不支持（`NacosLock` 类注释明确说明）

---

## 20. 配置参数

### 20.1 服务端

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.lock.default_expire_time` | `30000` (30s) | 默认租约时长（毫秒） |
| `nacos.lock.max_expire_time` | `1800000` (30min) | 最大租约时长上限（毫秒） |
| `nacos.lock.expire.scan.interval` | `1000` (1s) | 过期扫描间隔（毫秒） |

定义于 `lock/constant/PropertiesConstant.java:25-34`、`lock/schedule/LockExpireScanner.java:57`。

### 20.2 客户端

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `PropertyConstants.LOCK_REQUEST_TIMEOUT` | `-1`（=用 15000ms） | gRPC 单次请求超时 |

定义于 `api/lock/constant/PropertyConstants.java`、`client/lock/remote/grpc/LockGrpcClient.java:75,98-99`。

### 20.3 租约时长规则

`LockOperationServiceImpl.lock()`（`lock/service/impl/LockOperationServiceImpl.java:423-428`）：

```java
long expiredTime = lockInstance.getExpiredTime();
if (expiredTime < 0) {
    lockInfo.setEndTime(defaultExpireTime + getNowTimestamp());     // 默认 30s
} else {
    lockInfo.setEndTime(Math.min(maxExpireTime, expiredTime) + getNowTimestamp());  // 不超过 30min
}
```

- `expiredTime < 0` → 用默认值（30s）
- `expiredTime == 0` → `LockRequestHandler` 直接拒绝（ACQUIRE 必须 >0）
- `expiredTime > 0` → 用 `min(maxExpireTime, expiredTime)` 作为租约时长
- **服务端时间**计算，客户端时钟不影响

### 20.4 客户端常量

`client/lock/NacosLock.java`：
- `NOTIFICATION_POLL_TIMEOUT_MS = 60000L`（1 分钟轮询通知超时）
- `DEFAULT_SERVER_WAIT_TIME_MS = 300000L`（5 分钟服务端等待队列超时，`lock()` 用）

`client/lock/NacosLockWatchdog.java`：
- `DEFAULT_RENEW_INTERVAL_MS = 10000L`（10 秒续约间隔）

---

## 21. 实验性限制与待解决问题

引自 `specs/en/lock/lock-spec.md` §11 与代码 TODO：

### 21.1 安全限制

- **无 owner token / fencing token**：释放仅靠 owner 字符串匹配，无法防御伪造
- **`MutexAtomicLock` 不校验 owner**：旧版 `NACOS_LOCK` 任何客户端可释放任意锁
- **`LockRequestHandler` 含 `TODO Support auth`**：gRPC 鉴权尚未完整接入
- 当前应视为**可信客户端**场景使用

### 21.2 资源模型限制

- **无 namespace / group / tenant**：锁全局可见，无多租户隔离
- 资源标识仅 `lockType + key`，未来若加 namespace 是**不兼容变更**

### 21.3 一致性限制

- **无线性一致读**：`onRequest()` 返回 null，所有读走本地内存
- **无 owner token 校验**：release 不验证释放者真实持有锁

### 21.4 功能限制

- **无 Condition 支持**（`NacosLock.newCondition()` 抛异常）
- **不支持跨线程锁传递**（`NacosLock` 类注释）
- **无公平性保证**（虽然 FIFO 等待队列，但新请求若与"恰好释放"竞态可能插队——通过 `hasWaiters()` 检查缓解）
- **无 watch / query / renew 查询接口**

### 21.5 待决问题

- 是否保留为核心能力、迁移到扩展模块、或移除
- 稳定的 release ownership 语义（owner token / fencing / connection binding）
- 是否引入 `namespaceId` / tenant
- 完整 `SignType.LOCK` 鉴权
- 是否支持续约/查询/watch/公平/可重入语义
- 跨语言 SDK 契约
- 字段重命名（`expiredTime` 实为 lease duration，名称误导）

---

## 22. 核心结论速查

1. **一致性**：CP（JRaft），group = `lock_acquire_service_v2`，所有写操作走 Raft，宁失败不分歧。
2. **存储**：进程内 `ConcurrentHashMap` + Raft 日志 + `nacos_lock.zip` 快照，非数据库。
3. **锁类型**：`NACOS_LOCK`（旧）/`REENTRANT`/`NON_REENTRANT`，SPI 工厂可扩展。
4. **重入**：3.3.0 新增 `ReentrantAtomicLock`，重入计数在服务端 `reentrantCount` + 客户端 `ThreadLocal`。
5. **续约**：客户端 `NacosLockWatchdog` 默认 10s 一次，间隔 ≤ TTL/3。
6. **等待队列**：每锁一个 `LinkedList<WaitEntry>`，FIFO，服务端推送 `AVAILABLE` 唤醒队首。
7. **过期**：服务端时间计算 `endTime`，`LockExpireScanner` 1s 扫描，`autoExpire()` 在 `onApply` 内权威检查。
8. **断连清理**：`LockConnectionEventListener` 触发 → 单个 `CLEANUP_CONNECTION` Raft 请求 → 遍历所有锁 forceRelease + 清队列。
9. **线程安全**：实例级 `ReentrantLock`（transient，反序列化需重建）+ 全局 `ConcurrentHashMap` + service 级 `ReentrantReadWriteLock`。
10. **实验性**：无 owner token / fencing / 完整鉴权，spec 警告不应作为生产级强一致锁依赖。

---

## 附：核心源码索引（file:line）

| 关注点 | 位置 |
|--------|------|
| CP group 名 | `lock/constant/Constants.java:27` |
| CP 处理器注册 | `lock/service/impl/LockOperationServiceImpl.java:106-115` |
| Raft 写请求提交 | `lock/service/impl/LockOperationServiceImpl.java:413-449` |
| 状态机 onApply | `lock/service/impl/LockOperationServiceImpl.java:122-164` |
| acquire 逻辑 | `lock/service/impl/LockOperationServiceImpl.java:212-247` |
| release 逻辑 | `lock/service/impl/LockOperationServiceImpl.java:166-210` |
| expire 逻辑 | `lock/service/impl/LockOperationServiceImpl.java:306-341` |
| 断连清理 | `lock/service/impl/LockOperationServiceImpl.java:359-410` |
| 队首原子获取 | `lock/core/reentrant/AbstractAtomicLock.java:216-241` |
| 自动过期 | `lock/core/reentrant/AbstractAtomicLock.java:319-334` |
| 续约 | `lock/core/reentrant/AbstractAtomicLock.java:346-361` |
| 强制释放 | `lock/core/reentrant/AbstractAtomicLock.java:363-378` |
| 锁表 | `lock/NacosLockManager.java:43-44` |
| 快照保存 | `lock/persistence/NacosLockSnapshotOperation.java:104-119` |
| 快照加载 | `lock/persistence/NacosLockSnapshotOperation.java:139-173` |
| gRPC handler | `lock/remote/rpc/handler/LockRequestHandler.java:55-112` |
| 断连监听 | `lock/remote/LockConnectionEventListener.java:51-61` |
| 过期扫描 | `lock/schedule/LockExpireScanner.java:57-93` |
| 客户端 JUC Lock | `client/lock/NacosLock.java:138-334` |
| 客户端续约 | `client/lock/NacosLockWatchdog.java:75-106` |
| 客户端 gRPC | `client/lock/remote/grpc/LockGrpcClient.java:137-221` |
| 能力协商 | `client/lock/remote/grpc/LockGrpcClient.java:471-474` |
| SPI 注册 | `lock/src/main/resources/META-INF/services/com.alibaba.nacos.lock.factory.LockFactory` |
| Metrics 切面 | `lock/aspect/RequestLockAspect.java:42-61` |

---

*本文档基于 Nacos develop 分支（含 3.3.0 特性）源码梳理，仅用于学习分析，不代表官方承诺。锁能力为实验性，行为可能在未来版本不兼容变更。*