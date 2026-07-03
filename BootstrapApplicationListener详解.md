# BootstrapApplicationListener 详解

> 基于 spring-cloud-context 2.2.9.RELEASE + Spring Boot 2.2.x 源码。本文是 `Spring-Cloud-Bootstrap启动流程详解.md` 的深度下钻，专门剖析 `BootstrapApplicationListener.onApplicationEvent` 方法与 Bootstrap ApplicationContext 的创建启动全流程。
>
> 注：spring-cloud-context 源码不在本仓库（本仓库是 Nacos 服务端）。本文源码片段基于 2.2.9.RELEASE 还原，为突出主线有适度简化，关键逻辑保留。

---

## 一、`BootstrapApplicationListener` 的角色定位

```java
// org.springframework.cloud.bootstrap.BootstrapApplicationListener
public class BootstrapApplicationListener
        implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {

    public static final String BOOTSTRAP_PROPERTY_SOURCE_NAME = "bootstrap";
    public static final int DEFAULT_ORDER = Ordered.HIGHEST_PRECEDENCE + 5;
    private int order = DEFAULT_ORDER;
    ...
}
```

三个关键设计：
1. **监听 `ApplicationEnvironmentPreparedEvent`**：Spring Boot 在 Environment 准备好、context 还没创建时发布此事件——这是 bootstrap 的黄金时机。
2. **`Ordered.HIGHEST_PRECEDENCE + 5`**：优先级比 `ConfigFileApplicationListener`（`+10`）更高，**先于 `application.yml` 加载触发**。这保证了 bootstrap 拉的远程配置在 `application.yml` 加载前就已进入 Environment。
3. **它本身是 `ApplicationListener`，不是 `ApplicationContextInitializer`**：它负责"启动 bootstrap context"，不参与 context 内部初始化。

---

## 二、`onApplicationEvent` 方法职责

该方法是整个 bootstrap 机制的**总入口**，做四件事：
1. **三道防线**：判断是否要执行 bootstrap。
2. **解析配置**：读 `spring.cloud.bootstrap.name` / `location`。
3. **创建 bootstrap context**：调用 `bootstrapServiceContext`。
4. **应用 bootstrap**：调用 `apply`，建立父子关系 + 合并配置。

### 2.1 源码（简化还原）

```java
@Override
public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
    ConfigurableEnvironment environment = event.getEnvironment();

    // ★ 防线1：总开关（默认 true）
    if (!environment.getProperty("spring.cloud.bootstrap.enabled",
            Boolean.class, true)) {
        return;
    }

    // ★ 防线2：防重入——已经在 bootstrap context 内了，直接返回
    if (environment.getPropertySources()
            .contains(BOOTSTRAP_PROPERTY_SOURCE_NAME)) {
        return;
    }

    // 解析 bootstrap 配置名（默认 "bootstrap"）和位置
    String configName = environment.resolvePlaceholders(environment
            .getProperty("spring.cloud.bootstrap.name", "bootstrap"));
    String configLocation = environment.resolvePlaceholders(environment
            .getProperty("spring.cloud.bootstrap.location", null));

    // ★ 防线3：可选的自定义 context 类（spring.context.config.classes）
    ConfigurableApplicationContext context = null;
    for (String contextClass : environment.getProperty(
            "spring.context.config.classes", String[].class, new String[0])) {
        if (context == null) {
            try {
                context = createContext(contextClass, environment,
                        configName, configLocation);
            } catch (Exception e) {
                LOGGER.warn(...);
            }
        }
    }

    // ★ 核心路径：创建 bootstrap context
    if (context == null) {
        context = bootstrapServiceContext(environment,
                event.getSpringApplication(), configName, configLocation);
    }

    // ★ 应用：建立父子关系 + 合并配置到主 Environment
    apply(context, event.getSpringApplication(), environment);
}
```

### 2.2 三道防线决策流程图

```mermaid
flowchart TD
    Start([收到 ApplicationEnvironmentPreparedEvent]) --> G1{"spring.cloud.bootstrap.enabled<br/>== true ?<br/>(默认 true)"}
    G1 -->|false| Skip([直接返回<br/>走 configdata 机制])
    G1 -->|true| G2{"Environment 已含<br/>'bootstrap' PropertySource ?<br/>(防重入)"}
    G2 -->|是 已在 bootstrap 内| Skip2([直接返回<br/>避免无限递归])
    G2 -->|否 第一次进入| Resolve["解析 configName / configLocation<br/>默认 'bootstrap' / null"]
    Resolve --> G3{"spring.context.config.classes<br/>指定了自定义 context 类 ?"}
    G3 -->|是| CreateCustom["createContext 自定义类<br/>创建 context"]
    CreateCustom --> HasCtx{context != null?}
    G3 -->|否| Bootstrap["bootstrapServiceContext<br/>★ 核心路径"]
    Bootstrap --> HasCtx
    HasCtx -->|是| Apply["apply context application environment<br/>建立父子关系 + 合并配置"]
    HasCtx -->|否 创建失败| Done([返回 不做 bootstrap])
    Apply --> End([完成 主 context 启动继续])

    style Skip fill:#ffcdd2
    style Skip2 fill:#ffcdd2
    style Bootstrap fill:#fff3cd
    style Apply fill:#c8e6c9
```

### 2.3 三道防线详解

| 防线 | 检查条件 | 命中后的行为 | 意义 |
|------|---------|-------------|------|
| ① 总开关 | `spring.cloud.bootstrap.enabled != true` | 直接返回 | 允许禁用 bootstrap（Spring Boot 2.4+ 或自定义场景） |
| ② 防重入 | Environment 已含 `bootstrap` PropertySource | 直接返回 | 内层 run 也会发此事件，避免无限递归创建第三层 context |
| ③ 自定义 | `spring.context.config.classes` 指定类 | 用该类创建 context | 扩展点：允许自定义 bootstrap context 实现 |

防线② 是**防止无限递归**的关键：内层 run（bootstrap context 启动）也会发布 `ApplicationEnvironmentPreparedEvent`，`BootstrapApplicationListener` 会再次被触发。此时 `bootstrapServiceContext` 已经把一个名为 `bootstrap` 的 `MapPropertySource` 加进了 bootstrapEnvironment，所以防线②命中、直接返回。详见第十节。

---

## 三、`bootstrapServiceContext`：创建 bootstrap context

这是 `onApplicationEvent` 的核心调用，负责"造一个迷你 Spring 应用并启动"。

### 3.1 源码（简化还原）

```java
private ConfigurableApplicationContext bootstrapServiceContext(
        ConfigurableEnvironment environment, final SpringApplication application,
        String configName, String configLocation) {

    // ① 创建一个干净的 bootstrap Environment
    StandardEnvironment bootstrapEnvironment = new StandardEnvironment();
    MutablePropertySources bootstrapProperties = bootstrapEnvironment.getPropertySources();

    // ② 把主 Environment 里的非标准 PropertySource 搬过来
    //    (排除 systemProperties / systemEnvironment / commandArgs 等标准源)
    for (PropertySource<?> source : environment.getPropertySources()) {
        if (source instanceof EnumerablePropertySource
                && !STANDARD_SOURCES.contains(source.getName())) {
            bootstrapProperties.addLast(source);
        }
    }

    // ③ ★ 名字魔法：把 spring.config.name 设为 "bootstrap"
    //    这会让 ConfigFileApplicationListener 去找 bootstrap.yml 而非 application.yml
    Map<String, Object> bootstrapMap = new HashMap<>();
    bootstrapMap.put("spring.config.name", configName);          // ★★★
    if (StringUtils.hasText(configLocation)) {
        bootstrapMap.put("spring.config.location", configLocation);
    }
    bootstrapProperties.addFirst(
            new MapPropertySource(BOOTSTRAP_PROPERTY_SOURCE_NAME, bootstrapMap));

    // ④ 用 SpringApplicationBuilder 构建 bootstrap 应用
    SpringApplicationBuilder builder = new SpringApplicationBuilder();
    builder.profiles(environment.getProperty("spring.profiles.active")); // 继承 profile
    builder.environment(bootstrapEnvironment);
    builder.application().setApplicationName(configName);  // app name = "bootstrap"
    builder.main(application.getMainApplicationClass());
    builder.sources(BootstrapImportSelectorConfiguration.class);  // ★ 装配入口
    builder.configuration().setAllowCircularReferences(true);      // 允许循环引用

    // ⑤ ★★★ 启动！触发完整的 SpringApplication.run 流程（内层 run）
    final ConfigurableApplicationContext context = builder.run();
    context.setId("bootstrap");

    // ⑥ 注册关闭钩子
    if (context instanceof ConfigurableApplicationContext) {
        context.registerShutdownHook();
    }

    return context;
}
```

### 3.2 创建流程图

```mermaid
flowchart TD
    Entry([bootstrapServiceContext 入参<br/>environment / application / configName / configLocation]) --> S1["① new StandardEnvironment<br/>干净的 bootstrap Environment"]
    S1 --> S2["② 遍历主 Environment 的 PropertySource"]
    S2 --> S2a{"source 是 EnumerablePropertySource<br/>且不是标准源 ?"}
    S2a -->|是| S2b["bootstrapProperties.addLast(source)<br/>搬到 bootstrap Environment"]
    S2a -->|否| S2c[跳过]
    S2b --> S3
    S2c --> S3
    S3["③ ★ 名字魔法<br/>bootstrapMap.put spring.config.name = 'bootstrap'<br/>包装成 MapPropertySource 名为 'bootstrap'<br/>addFirst 到 bootstrapProperties"]
    S3 --> S4["④ 构造 SpringApplicationBuilder"]
    S4 --> S4a["profiles 继承主 Environment 的 active profile"]
    S4a --> S4b["environment bootstrapEnvironment"]
    S4b --> S4c["applicationName = configName 'bootstrap'"]
    S4c --> S4d["sources BootstrapImportSelectorConfiguration<br/>装配入口"]
    S4d --> S4e["setAllowCircularReferences true"]
    S4e --> S5["⑤ ★★★ builder.run()<br/>触发完整 SpringApplication.run 内层启动"]
    S5 --> S6["context.setId 'bootstrap'"]
    S6 --> S7["registerShutdownHook"]
    S7 --> Done([返回 bootstrap context])

    style S3 fill:#fff3cd
    style S5 fill:#ffe0b2
```

### 3.3 关键设计点

1. **干净的 bootstrapEnvironment**：不复用主 Environment，避免主 Environment 里的业务配置干扰 bootstrap 的配置加载。
2. **搬非标准源**：把主 Environment 里用户自定义的 PropertySource 搬过来，让 bootstrap 能读到用户配置（如 `spring.cloud.nacos.config.*`）。
3. **`spring.config.name = "bootstrap"`（名字魔法）**：这是核心——让 `ConfigFileApplicationListener` 去加载 `bootstrap.yml` 而不是 `application.yml`。复用 Spring Boot 现成机制，不自己写 yml 加载。
4. **继承 profile**：`builder.profiles(...)` 让 `bootstrap-{profile}.yml` 也能被加载（前提是 profile 已在主 Environment 里）。
5. **`BootstrapImportSelectorConfiguration` 作为 source**：这是 bootstrap 装配的入口，会触发 `BootstrapImportSelector` 扫描 `spring.factories`。

---

## 四、`builder.run()`：bootstrap context 的完整启动

`SpringApplicationBuilder.run()` 内部就是一次完整的 `SpringApplication.run()`。这是**内层 run**（见 `Spring-Cloud-Bootstrap启动流程详解.md` 第九节）。下面拆解这次 run 的每一步。

### 4.1 启动时序图

```mermaid
sequenceDiagram
    autonumber
    participant BAL as BootstrapApplication<br/>Listener
    participant SAB as SpringApplicationBuilder
    participant SA as SpringApplication (内层)
    participant BE as bootstrap Environment
    participant CFG as ConfigFileApplication<br/>Listener
    participant AC as ApplicationContext
    participant BISC as BootstrapImportSelector<br/>Configuration
    participant BIS as BootstrapImportSelector
    participant BCfg as PropertySourceBootstrap<br/>Configuration
    participant NPSL as NacosPropertySourceLocator

    BAL->>SAB: new SpringApplicationBuilder
    BAL->>SAB: 配置 profiles / environment / sources
    BAL->>SAB: run()
    SAB->>SA: new SpringApplication(BootstrapImportSelectorConfiguration.class)
    SA->>SA: SpringApplication.run() 内层启动

    Note over SA: ===== prepareEnvironment =====
    SA->>BE: new StandardEnvironment (已含 bootstrapMap)
    SA->>SA: publish ApplicationEnvironmentPreparedEvent

    SA->>BAL: onApplicationEvent (再次触发!)
    BAL->>BAL: 防线② Environment 已含 'bootstrap' 源<br/>★ 直接返回 防重入

    SA->>CFG: onApplicationEvent
    Note over CFG: spring.config.name = 'bootstrap'<br/>搜索 classpath
    CFG->>CFG: 找到 bootstrap.yml / bootstrap-{profile}.yml
    CFG->>BE: addFirst bootstrapProperties<br/>(bootstrap.yml 内容)
    Note over BE: bootstrap.yml 已加载<br/>含 spring.cloud.nacos.config.*

    Note over SA: ===== createApplicationContext =====
    SA->>AC: createApplicationContext<br/>(默认 AnnotationConfigApplicationContext)
    AC->>AC: 注册 BeanDefinitionReader

    Note over SA: ===== prepareContext =====
    SA->>AC: 注册 sources = BootstrapImportSelectorConfiguration
    SA->>AC: invoke ApplicationContextInitializers
    AC->>BISC: @Import 触发 BootstrapImportSelector
    BISC->>BIS: selectImports
    BIS->>BIS: SpringFactoriesLoader.loadFactoryNames<br/>(BootstrapConfiguration.class)
    Note over BIS: 找到 NacosConfigBootstrapConfiguration 等
    BIS->>AC: 注册这些 BootstrapConfiguration 为 @Configuration

    AC->>BCfg: PropertySourceBootstrapConfiguration.initialize(context)
    BCfg->>BCfg: autowireBean(this) 触发 locator Bean 创建<br/>NacosConfigProperties / NacosConfigManager / NacosPropertySourceLocator
    loop 遍历每个 PropertySourceLocator
        BCfg->>NPSL: locate(environment)
        NPSL->>NPSL: 用 bootstrap.yml 里的 nacos 配置连 Nacos
        NPSL-->>BCfg: CompositePropertySource[nacos-*]
        BCfg->>BCfg: composite.addPropertySource
    end
    BCfg->>BE: environment.getPropertySources().addFirst(composite)
    Note over BE: ★ 远程 Nacos 配置已进入 bootstrap Environment 最前面

    Note over SA: ===== refreshContext =====
    SA->>AC: refresh()
    AC->>AC: 实例化所有剩余 bootstrap Bean
    Note over AC: bootstrap context 就绪<br/>含 NacosConfigProperties / NacosPropertySourceLocator / ConfigService 等

    SA-->>SAB: 返回 context
    SAB-->>BAL: 返回 bootstrap context
    BAL->>BAL: context.setId("bootstrap")
    BAL->>BAL: registerShutdownHook
```

### 4.2 五个阶段拆解

#### 阶段一：`prepareEnvironment`（准备 bootstrap Environment）
- `bootstrapEnvironment` 已被 `bootstrapServiceContext` 第③步注入了 `spring.config.name = "bootstrap"`。
- 发布 `ApplicationEnvironmentPreparedEvent` → `BootstrapApplicationListener` 因防线②返回（防重入）→ `ConfigFileApplicationListener` 加载 `bootstrap.yml`。

#### 阶段二：`createApplicationContext`（创建空 context）
- 创建 `AnnotationConfigApplicationContext`（默认类型）。
- 此时 context 是空的，没有任何业务 Bean，只有 BeanDefinitionReader。

#### 阶段三：`prepareContext`（装配 + 跑 initializer）
- 注册 `BootstrapImportSelectorConfiguration` 作为 source。
- `@Import(BootstrapImportSelector.class)` 触发 `BootstrapImportSelector.selectImports()` → 扫描 `spring.factories` 加载所有 `BootstrapConfiguration`（含 Nacos 的）。
- 跑 `ApplicationContextInitializer`：**`PropertySourceBootstrapConfiguration.initialize(context)`** 在此刻执行——这是 locator 调用的时机（详见第八节）。

#### 阶段四：`refreshContext`（实例化 Bean）
- 实例化所有 `BootstrapConfiguration` 注册的 Bean：`NacosConfigProperties`、`NacosConfigManager`、`NacosPropertySourceLocator`、`ConfigService` 等。
- 注意：locator Bean 在阶段三的 `initialize` 里就已被 `autowireBean` 提前创建，这里只是补齐其他 Bean。

#### 阶段五：返回 context
- `builder.run()` 返回 bootstrap context，`setId("bootstrap")`，注册关闭钩子。

---

## 五、`BootstrapImportSelector` 与 `BootstrapConfiguration` 装配

### 5.1 装配入口链

```mermaid
flowchart TD
    A["BootstrapImportSelectorConfiguration<br/>(@Configuration)"] -->|"@Import 注解"| B["BootstrapImportSelector<br/>(DeferredImportSelector)"]
    B --> C["selectImports 执行"]
    C --> D["SpringFactoriesLoader.loadFactoryNames<br/>(BootstrapConfiguration.class, classLoader)"]
    D --> E["扫描所有 jar 的<br/>META-INF/spring.factories"]
    E --> F1["spring-cloud-context.jar<br/>→ 基础配置类"]
    E --> F2["spring-cloud-alibaba-nacos-config.jar<br/>→ NacosConfigBootstrapConfiguration"]
    E --> F3["其它配置中心 jar ..."]
    F1 --> G["合并去重"]
    F2 --> G
    F3 --> G
    G --> H["作为 @Configuration 注册进 bootstrap context"]
    H --> I["refresh 时实例化:<br/>NacosConfigProperties<br/>NacosConfigManager<br/>NacosPropertySourceLocator"]

    style B fill:#fff3cd
    style D fill:#fff3cd
    style F2 fill:#c8e6c9
```

### 5.2 `BootstrapImportSelector` 源码（简化）

```java
public class BootstrapImportSelector
        implements DeferredImportSelector, EnvironmentAware, Ordered {

    private Environment environment;

    @Override
    public String[] selectImports(AnnotationMetadata annotationMetadata) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        // ★ 从 spring.factories 读 BootstrapConfiguration 配置项
        List<String> names = new ArrayList<>(SpringFactoriesLoader.loadFactoryNames(
                BootstrapConfiguration.class, classLoader));
        // 用户可通过 spring.cloud.bootstrap.sources 追加
        names.addAll(Arrays.asList(environment.getProperty(
                "spring.cloud.bootstrap.sources", String[].class, new String[0])));
        // ... 注解配置处理
        return names.toArray(new String[0]);
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
```

关键点：
- **`DeferredImportSelector`**：延迟到所有 `@Configuration` 处理完再执行，保证用户配置优先。
- **`SpringFactoriesLoader.loadFactoryNames`**：读所有 jar 的 `META-INF/spring.factories` 里 key = `BootstrapConfiguration` 的配置类。
- **用户扩展点**：`spring.cloud.bootstrap.sources` 可追加自定义配置类。

---

## 六、`PropertySourceBootstrapConfiguration.initialize` 的调用时机

这是一个**容易混淆的关键点**：locator 在哪个阶段执行？

### 6.1 时机澄清

`PropertySourceBootstrapConfiguration` 身兼两职：
1. **`ApplicationContextInitializer`**：在 `prepareContext` 阶段被调用（context 创建后、refresh 前）。
2. **`@Configuration`**：本身也是个配置类，但其 `initialize` 方法是入口。

`initialize` 在 `prepareContext` 阶段执行，但此时 locator Bean 还没经过完整 refresh。`initialize` 通过 `autowireBean(this)` **强制触发 locator Bean 的提前创建**，然后调用 `locate()`。

### 6.2 initialize 流程图

```mermaid
flowchart TD
    A(["PropertySourceBootstrapConfiguration<br/>.initialize applicationContext<br/>(prepareContext 阶段)"]) --> B["获取 context 的 Environment"]
    B --> C["new CompositePropertySource 'bootstrap'"]
    C --> D["autowireBean this<br/>★ 强制触发 PropertySourceLocator Bean 创建"]
    D --> D1["创建 NacosConfigProperties<br/>读 bootstrap.yml 的 nacos 配置"]
    D1 --> D2["创建 NacosConfigManager<br/>构造 ConfigService"]
    D2 --> D3["创建 NacosPropertySourceLocator"]
    D3 --> E{"遍历<br/>propertySourceLocators"}
    E -->|取出下一个| F["locator.locate environment"]
    F --> F1["NacosPropertySourceLocator.locate:<br/>调 configService.getConfig 拉 Nacos 配置"]
    F1 --> F2["解析 content 为 Map<br/>封装 NacosPropertySource"]
    F2 --> G["composite.addPropertySource 结果"]
    G --> E
    E -->|遍历完| H["environment.getPropertySources<br/>.addFirst composite"]
    H --> I["★ 远程配置进入 bootstrap Environment 最前面<br/>优先级最高"]
    I --> Done([initialize 完成])

    style D fill:#fff3cd
    style H fill:#c8e6c9
```

### 6.3 为什么 locator 在 refresh 前执行

因为远程配置要在 bootstrap context 的 refresh 阶段就可用——这样：
- bootstrap context 内的 Bean（如果用 `@Value` 引用远程配置）能在 refresh 时拿到远程值。
- 更重要的是，bootstrap 的 Environment 最终会被 `apply` 合并到主 Environment，让主 context 的 `@Value` 拿到远程值。

如果在 refresh 后才 `locate`，主 context 启动时就来不及看到远程配置了。所以 `initialize`（prepareContext 阶段）是**最晚的可用时机**。

---

## 七、`apply`：建立父子关系 + 合并配置

`bootstrapServiceContext` 返回后，`onApplicationEvent` 调用 `apply`，把 bootstrap context"嫁接"到主应用上。

### 7.1 apply 源码（简化）

```java
private void apply(ConfigurableApplicationContext context,
        SpringApplication application, ConfigurableEnvironment environment) {
    if (context == null) {
        return;
    }
    // ① ★ 注册 initializer：主 context 创建时把 bootstrap 设为父
    application.addInitializers(
            new ParentContextApplicationContextInitializer(context));

    // ② 添加监听器：处理 bootstrap 配置源的传播/过滤
    application.addListeners(new BootstrapSourceFilter(environment));

    // ③ ★ 把 bootstrap context 的配置合并进主 Environment
    environment.getPropertySources().remove(BOOTSTRAP_PROPERTY_SOURCE_NAME);
    if (context.getEnvironment() instanceof ConfigurableEnvironment) {
        ConfigurableEnvironment bootstrapEnv =
                (ConfigurableEnvironment) context.getEnvironment();
        // 把 bootstrap 里非标准源搬到主 Environment
        for (PropertySource<?> source : bootstrapEnv.getPropertySources()) {
            if (!STANDARD_SOURCES.contains(source.getName())
                    && !BOOTSTRAP_PROPERTY_SOURCE_NAME.equals(source.getName())) {
                // 按优先级插入主 Environment
                environment.getPropertySources().addAfter(..., source);
            }
        }
    }
}
```

### 7.2 apply 流程图

```mermaid
flowchart TD
    Start(["apply context application environment<br/>(bootstrapServiceContext 返回后)"]) --> A{context == null?}
    A -->|是| Skip([返回])
    A -->|否| B["① addInitializer<br/>ParentContextApplicationContextInitializer context"]
    B --> B1["主 context 创建时 child.setParent bootstrap<br/>建立父子关系"]
    B1 --> C["② addListener BootstrapSourceFilter<br/>处理 bootstrap 源的传播/过滤"]
    C --> D["③ 移除主 Environment 的 'bootstrap' 占位源"]
    D --> E["遍历 bootstrap context 的 PropertySource"]
    E --> F{"非标准源<br/>且不是 'bootstrap' ?"}
    F -->|是| G["addAfter 插入主 Environment<br/>★ 把远程配置合并进来"]
    G --> E
    F -->|否| E
    E -->|遍历完| Done([主 Environment 现含远程配置<br/>主 context 启动继续])

    style B fill:#fff3cd
    style G fill:#c8e6c9
```

### 7.3 `ParentContextApplicationContextInitializer`：建立父子关系

`apply` 第①步注册的 initializer，在主 context 的 `prepareContext` 阶段执行：

```java
public class ParentContextApplicationContextInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext>, Ordered {

    private final ApplicationContext parent;

    public ParentContextApplicationContextInitializer(ApplicationContext parent) {
        this.parent = parent;   // ★ bootstrap context
    }

    @Override
    public void initialize(ConfigurableApplicationContext child) {
        // ★ 主 context (child) 的 parent = bootstrap context
        if (child != this.parent && this.parent != null) {
            child.setParent(this.parent);
        }
    }
}
```

调用时机时序图：

```mermaid
sequenceDiagram
    autonumber
    participant BAL as BootstrapApplicationListener
    participant Main as 主 SpringApplication
    participant Init as ParentContextApplicationContext<br/>Initializer
    participant BC as bootstrap context
    participant MC as 主 context

    BAL->>Main: application.addInitializers ParentContextApplicationContextInitializer bootstrapCtx
    Note over Main: 外层 run 继续
    Main->>Main: prepareContext
    Main->>Init: invoke initializers
    Init->>MC: child.setParent(bootstrapCtx)
    Note over MC: 主 context 的 parent = bootstrap context<br/>主 context 能看到父 context 的 Bean
```

---

## 八、防重入机制详解

这是 bootstrap 机制最微妙的部分：内层 run 会再次触发 `BootstrapApplicationListener`，如何避免无限递归？

### 8.1 递归触发点

```mermaid
flowchart TD
    A[外层 run publish ApplicationEnvironmentPreparedEvent] --> B[BAL 第一次触发]
    B --> C[bootstrapServiceContext]
    C --> D["builder.run() 内层 run"]
    D --> E[内层 run publish ApplicationEnvironmentPreparedEvent]
    E --> F[BAL 第二次触发!]
    F --> G{防线②: 已含 bootstrap 源?}
    G -->|是| H[直接返回 不递归]
    G -->|否 无防线| I[又调 bootstrapServiceContext]
    I --> J[又 builder.run 第三层...]
    J --> K[无限递归 StackOverflow]

    style G fill:#fff3cd
    style H fill:#c8e6c9
    style K fill:#ffcdd2
```

### 8.2 防线②的判定依据

`bootstrapServiceContext` 第③步往 `bootstrapEnvironment` 里 `addFirst` 了一个名为 `bootstrap` 的 `MapPropertySource`（含 `spring.config.name`）。所以内层 run 的 `ApplicationEnvironmentPreparedEvent` 触发时，`environment.getPropertySources().contains("bootstrap")` 为 `true` → 防线②命中 → 直接返回。

> 这就是为什么 `bootstrapServiceContext` 必须把 `bootstrapMap` 包成 `MapPropertySource` 并命名为 `bootstrap`——既是为了传 `spring.config.name`，也是作为防重入标记。

### 8.3 防重入流程图

```mermaid
flowchart TD
    Start([BAL 被触发]) --> Check1{防线① enabled?}
    Check1 -->|否| R1[返回]
    Check1 -->|是| Check2{防线②<br/>Environment 已含 bootstrap 源?}
    Check2 -->|是 标记已存在| R2["★ 直接返回<br/>(这是内层 run 触发的)"]
    Check2 -->|否 第一次| Resolve[解析 configName/location]
    Resolve --> Create["bootstrapServiceContext<br/>往 bootstrapEnvironment 注入 bootstrap 标记源"]
    Create --> InnerRun["builder.run 内层 run"]
    InnerRun --> Event["内层 publish ApplicationEnvironmentPreparedEvent"]
    Event --> TriggerAgain[BAL 再次被触发]
    TriggerAgain --> Check2
    Check2 -.命中 直接返回.-> R2

    style Check2 fill:#fff3cd
    style R2 fill:#c8e6c9
```

---

## 九、完整端到端时序图

把所有阶段串起来，从 `main()` 到 bootstrap 完成再回到主 context 启动：

```mermaid
sequenceDiagram
    autonumber
    participant User as 用户 main
    participant Main as 主 SpringApplication
    participant MainEnv as 主 Environment
    participant BAL as BootstrapApplicationListener
    participant BSC as bootstrapServiceContext
    participant SAB as SpringApplicationBuilder
    participant Inner as 内层 SpringApplication
    participant BE as bootstrap Environment
    participant CFG as ConfigFileApplicationListener
    participant BC as bootstrap context
    participant BIS as BootstrapImportSelector
    participant BCfg as PropertySourceBootstrapConfiguration
    participant NPSL as NacosPropertySourceLocator
    participant PCII as ParentContextApplicationContext<br/>Initializer
    participant NS as Nacos Server

    User->>Main: run(MyApp.class, args)
    Main->>MainEnv: new Environment
    Main->>Main: prepareEnvironment
    Main->>Main: publish ApplicationEnvironmentPreparedEvent

    Note over BAL: ===== onApplicationEvent 触发 =====
    Main->>BAL: onApplicationEvent(event)
    BAL->>BAL: 防线① enabled=true 通过
    BAL->>BAL: 防线② 不含 bootstrap 源 通过
    BAL->>BAL: 解析 configName='bootstrap'
    BAL->>BSC: bootstrapServiceContext(env, app, 'bootstrap', null)

    Note over BSC: ===== 创建 bootstrap context =====
    BSC->>BE: new StandardEnvironment
    BSC->>BE: 搬运主 Environment 非标准源
    BSC->>BE: addFirst 'bootstrap' MapPropertySource<br/>(spring.config.name='bootstrap')
    BSC->>SAB: new SpringApplicationBuilder
    BSC->>SAB: profiles / environment / sources<br/>applicationName='bootstrap'
    BSC->>Inner: builder.run() ★ 内层 run

    Note over Inner: ===== 内层 SpringApplication.run =====
    Inner->>BE: prepareEnvironment
    Inner->>Inner: publish ApplicationEnvironmentPreparedEvent
    Inner->>BAL: onApplicationEvent 又触发
    BAL->>BAL: 防线② 已含 bootstrap 源 直接返回 ★防重入
    Inner->>CFG: onApplicationEvent
    CFG->>CFG: spring.config.name='bootstrap' 搜索
    CFG->>BE: addFirst bootstrap.yml 内容
    Inner->>BC: createApplicationContext
    Inner->>BC: prepareContext 注册 BootstrapImportSelectorConfiguration
    BC->>BIS: @Import 触发 selectImports
    BIS->>BIS: SpringFactoriesLoader 读 spring.factories
    BIS->>BC: 注册 NacosConfigBootstrapConfiguration 等
    Inner->>BCfg: ApplicationContextInitializer.initialize
    BCfg->>BCfg: autowireBean 触发 locator Bean 创建
    BCfg->>NPSL: locate(environment)
    NPSL->>NS: getConfig 拉 Nacos 配置
    NS-->>NPSL: 配置内容
    NPSL-->>BCfg: CompositePropertySource[nacos-*]
    BCfg->>BE: addFirst 'bootstrap' composite ★远程配置注入
    Inner->>BC: refresh 实例化剩余 Bean
    Inner-->>SAB: 返回 bootstrap context
    SAB-->>BSC: 返回 context
    BSC->>BC: setId('bootstrap')
    BSC->>BC: registerShutdownHook
    BSC-->>BAL: 返回 bootstrap context

    Note over BAL: ===== apply =====
    BAL->>Main: addInitializers ParentContextApplicationContextInitializer(bootstrapCtx)
    BAL->>Main: addListeners BootstrapSourceFilter
    BAL->>MainEnv: 合并 bootstrap 的 PropertySource 到主 Environment
    Note over MainEnv: ★ 主 Environment 现含远程 Nacos 配置
    BAL-->>Main: 返回 外层继续

    Note over Main: ===== 主 context 启动 =====
    Main->>CFG: ConfigFileApplicationListener 加载 application.yml
    Main->>BC: createApplicationContext 主 context
    Main->>PCII: prepareContext 跑 initializer
    PCII->>Main: 主 context.setParent(bootstrapCtx) ★父子关系建立
    Main->>Main: refresh 主 context Bean 创建
    Note over Main: @Value 解析主 Environment 拿到远程配置值
    Main-->>User: 应用启动完成
```

---

## 十、关键设计点总结

### 10.1 `onApplicationEvent` 的设计哲学

| 设计 | 价值 |
|------|------|
| 监听 `ApplicationEnvironmentPreparedEvent` | 主 context 还没创建，是注入远程配置的黄金时机 |
| 优先级 `HIGHEST_PRECEDENCE + 5` | 先于 `ConfigFileApplicationListener`，保证远程配置在 `application.yml` 加载前进入 Environment |
| 三道防线 | 开关 / 防重入 / 扩展点，覆盖禁用、递归、自定义场景 |
| 用 `SpringApplicationBuilder` 嵌套 run | 复用 Spring Boot 完整启动流程，bootstrap context 也是个正规 Spring 应用 |

### 10.2 "名字魔法"是核心创新

`bootstrapServiceContext` 没有自己实现 yml 加载，只往 bootstrapEnvironment 注入 `spring.config.name = "bootstrap"`，就让 Spring Boot 现成的 `ConfigFileApplicationListener` 去加载 `bootstrap.yml`。这是**组合优于重写**的典范——一行配置名切换了整个加载目标。

### 10.3 防重入的巧妙

用 `MapPropertySource` 的**名字** `bootstrap` 作为标记：既承载 `spring.config.name`，又作为防线②的判定依据。一物两用，没有额外引入标志位。

### 10.4 locator 在 `prepareContext` 阶段执行

`PropertySourceBootstrapConfiguration` 作为 `ApplicationContextInitializer`，在 refresh 前通过 `autowireBean` 提前创建 locator 并调用 `locate()`。这保证远程配置在 refresh 前就进入 Environment，让所有后续 Bean 创建都能用上。

### 10.5 `apply` 的两个动作

1. **`ParentContextApplicationContextInitializer`**：建立父子关系，主 context 能继承 bootstrap context 的 Bean。
2. **合并 PropertySource**：把 bootstrap 加载的远程配置塞进主 Environment，让主 context 的 `@Value` 拿到远程值。
3. 两者缺一不可：只有父子关系没合并配置，主 context 看不到远程值；只合并配置没父子关系，bootstrap context 的 Bean 无法被主 context 复用。

---

## 十一、源码位置索引

| 关注点 | 类 | 关键方法 |
|--------|----|---------|
| 入口监听器 | `BootstrapApplicationListener` | `onApplicationEvent` |
| 创建 bootstrap context | `BootstrapApplicationListener` | `bootstrapServiceContext` |
| 应用到主应用 | `BootstrapApplicationListener` | `apply` |
| 装配入口 | `BootstrapImportSelectorConfiguration` | `@Import(BootstrapImportSelector.class)` |
| 扫描 spring.factories | `BootstrapImportSelector` | `selectImports` |
| 配置加载聚合 | `PropertySourceBootstrapConfiguration` | `initialize` |
| 建立父子关系 | `ParentContextApplicationContextInitializer` | `initialize` |
| 配置源过滤 | `BootstrapSourceFilter` | `onApplicationEvent` |
| Nacos 装配 | `NacosConfigBootstrapConfiguration` | `@Configuration` 注册三件套 |
| Nacos 拉配置 | `NacosPropertySourceLocator` | `locate` |

---

## 十二、串联回前序文档

```mermaid
flowchart LR
    A["Spring-Cloud-Bootstrap启动流程详解<br/>第三节 触发入口<br/>第五节 BootstrapConfiguration 装配"] --> B["本文<br/>BootstrapApplicationListener 详解<br/>onApplicationEvent + 创建启动全流程"]
    B --> C["Spring-Cloud-Bootstrap启动流程详解<br/>第九节 两次 run 调用<br/>第十节 ContextRefresher 重跑"]
    C --> D["@RefreshScope注解实现原理<br/>第四节 刷新流程<br/>第 4.1 节 addConfigFilesToEnvironment"]

    style B fill:#fff3cd
```

本文是 `Spring-Cloud-Bootstrap启动流程详解.md` 的下钻：
- 该文档第三节讲了 `BootstrapApplicationListener` 的**职责概述**，本文第二节给出 `onApplicationEvent` 的**逐行源码解析**。
- 该文档第四节讲了 bootstrap context 创建的**概要**，本文第三、四节给出 `bootstrapServiceContext` 的**完整源码 + 五阶段启动拆解**。
- 该文档第六节讲了 `PropertySourceBootstrapConfiguration` 聚合，本文第六节补充了**locator 的调用时机**（prepareContext 阶段而非 refresh）。
- 该文档没有专门讲防重入，本文第八节详解了**防线②如何防止无限递归**。

> **一句话总结**：`onApplicationEvent` 通过三道防线判断是否执行、用 `bootstrapServiceContext` 造一个名为 `bootstrap` 的迷你 Spring 应用并完整 `run()` 起来（加载 `bootstrap.yml` → 扫 `spring.factories` 装配 → 跑 locator 拉远程配置），最后 `apply` 把这个 context 设为主 context 的父并把远程配置合并进主 Environment。防重入靠 `bootstrap` 标记源，名字魔法靠 `spring.config.name = "bootstrap"`，两者都是 `bootstrapServiceContext` 第③步那个 `MapPropertySource` 的功劳。