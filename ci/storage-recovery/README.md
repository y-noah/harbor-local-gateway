# 存储迁移与恢复演练

本工具仅用于本任务新建私有数据库与目录的演练，不读取线上源、不停止服务、不改线上角色/网络/归档配置、不触发部署。真实来源迁移、服务器 TLS 与最终切换另在发布窗口验收。它不替代受控部署入口对实际候选镜像的核验。

## SQLite → PostgreSQL

需要 Node.js 24、已准备的隔离 PostgreSQL 18 和一个独立 32 字节备份加密密钥文件（当前用户、0600）。该备份密钥与员工 Key 主密钥、正文主密钥分别管理，不覆盖原文件。目录为当前用户所有的 0700 私有新目录。

管理员连接只通过当前进程的 `HARBOR_REHEARSAL_DATABASE_URL` 传入，不放在参数、工程文件或终端输出中。新目标使用随机数据库名和专用非超级角色，撤销 PUBLIC CONNECT，拒绝角色继承与其他连接；目标连接材料只存加密的 `target.enc`。本工具不会撤销既有业务库权限。

以下参数均为占位示例，请替换为自己明确创建的演练目录：

```bash
node storage-maintenance.mjs create-target --directory /private/rehearsal --key-file /private/backup.key
node storage-maintenance.mjs capture --source-data /private/source/data --directory /private/rehearsal --key-file /private/backup.key --roots-file /private/roots.json
node storage-maintenance.mjs transfer --directory /private/rehearsal --key-file /private/backup.key
node storage-maintenance.mjs restore-files --directory /private/rehearsal --destination /private/candidate --key-file /private/backup.key
```

`roots.json` 必须为 0600，仅列授权备份的根目录；清单中的路径随后加密存储。各根不可重叠、不可包含归档输出目录，不跟随符号链接或特殊文件。例如：

```json
[
  {"id":"data","source":"/private/source/data","destinationPrefix":"/private/candidate/data"},
  {"id":"runtime","source":"/private/source/runtime","databasePrefix":"/app/runtime","destinationPrefix":"/app/runtime"}
]
```

`databasePrefix` 表示数据库 home 字段使用的路径前缀，默认等于 source；`destinationPrefix` 是新 home 前缀，默认不变。容器内路径和宿主机文件路径不相同时必须显式声明，不能按员工或账号名猜目录。所有账号 home 须命中已捕获的根目录；有原密文时，必须保留并验证原 `key-encryption.key`，同时保留 `settings.json` 的管理员密码哈希。TLS/CA、其他加密材料、账号认证目录及暂存若位于额外目录，同样显式列入根清单。

原生只读 SQLite 在实例锁及 BEGIN IMMEDIATE 下创建固定 backup，再对数据库和文件集加密。不会构造有启动副作用的旧 Store；固定数据库独立于 WAL/shm，原库不改写。`capture-db` 仅作数据库阶段诊断，不代表完整文件恢复。未知表/字段、坏关系、活动请求、登录/均衡未排空、缺材料或不明 home 会拒绝处理。

全部九张旧业务表按固定主键分批导入，原 Key 摘要和 AAD、账号密文、会话归属、原始计量及费率版本保留。缺失历史费率不套现价；人工金额按大整数原样保留。逐表、逐行、关系及精确参考金额复核后才完成数据库阶段。新 schema 默认参数与旧历史分开核验；旧 SQLite 没有正文、类型模板、设备、批次或后台任务来源时不伪造数据。

导入期间独立连接持有网关所有权锁，`data_contract` 标记迁移中，网关/Worker 不能正常启动。加密 checkpoint 绑定固定来源，批提交后未更新 checkpoint 的崩溃可重新执行 `transfer`，相同数据核验后跳过，冲突不覆盖。schema 版本推进则拒绝沿用旧目标，需重新准备演练；不会自动把不同版本混起来。不要删除 checkpoint 或绕过失败直接修改契约。

文件集逐文件认证加密，摘要计算与加密使用同一读取流，复查源变化则不发布完整清单。恢复到空私有候选目录，校验每个密文和明文摘要，权限收窄为当前用户权限。配置、密钥、spool 及账号目录必须一起恢复；`restore-files` 不会改线上配置或将候选目录接入流量。原 SQLite 文件不会出现在新 data 根，固定来源仍保留在加密归档中。

演练结束后可用 `destroy-target`（同样的 directory/key-file 与管理员环境变量）删除工具创建的私有数据库/角色。它复核目标归属和访问隔离，遇到其他连接不强制删除；文件清理由调用方在确认资源已退出后办理。

## PostgreSQL 全量恢复与 PITR

正式可运行恢复主路径为 PostgreSQL 18 的物理 basebackup 加连续 WAL。物理恢复保留集群 system_identifier、事务 ID 及快照语义。应用配置、员工/上游 Key 主密钥、正文密钥、设备 CA/TLS 材料、账号 home 和两个 spool 与数据库共同保全；客户端设备私钥不属于服务器备份。

`createPitrRehearsal()` 只在新的空私有目录创建独立 Unix socket 集群，不监听 TCP，不修改共享集群。它用 `pg_basebackup` 固定物理备份，将文件集和 WAL 加密归档。归档先完成并认证密文，再原子发布 metadata；密文发布后崩溃可幂等补齐 metadata，不覆盖不一致的 WAL。

`mark()` 创建命名恢复点，`flushWal()` 等待对应 WAL 实际归档。`recover()` 恢复到新目录，使用认证的 WAL，到达指定点并真正退出 recovery 后再核验 system_identifier 和 schema，不能把 hot standby 就绪当作恢复完成。启动失败会核对本任务 PGDATA/PID 并回收实例，正常收尾也会停止本任务的主/恢复集群。

真实本机验收入口：

```bash
HARBOR_TEST_PG_BIN_DIR=/private/tools/postgresql/18/bin node --test tests/postgres-recovery.test.mjs
```

若工具链需要额外动态库，由调用环境按该工具链设置 `LD_LIBRARY_PATH`。未提供 PG18 工具时，物理演练测试明确跳过，不计作通过。`tests/postgres-migration.test.mjs` 读取既有隔离测试 PG 环境变量，只在其内创建新的随机私有库和角色；其他集群不操作。

恢复点验收至少包括备份后新增 A、命名点、再写 B：恢复后 A 存在、B 排除，原 Key、加密正文、冻结请求分页、设备门禁和批次结果仍可读取。当前小样本实测时间只说明本机演练，不证明生产的 15 分钟 RPO、2 小时 RTO；正式容量、持续归档监控、备份生命周期与异机恢复须结合真实发布环境另行验收。

## 专用集群的受控备份入口

`node storage-backup.mjs` 提供 `check`、`prepare-archive`、`backup`、`verify`、`checkpoint`、`restore` 和固定 WAL 文件名的 `archive-wal` 子命令。正式入口只读固定 root 配置 `/etc/harbor-storage-backup.json`、`/etc/harbor-storage-archive.json` 与既有管理连接文件，不接受连接 URL、任意配置路径或 shell。模板见本目录 `backup.example.json`、`archive.example.json`、`archive-command.example.conf` 及 systemd service/timer；工具不会自动安装、加载或重启服务。

物理备份覆盖整个集群，必须明确批准专用集群的 system_identifier、全部数据库、角色属性及成员关系；任一不符即拒绝，不能靠只授权一个 Harbor 库备走共享集群。root 信任工具链、PG UID 的私有归档密钥与目录、PGDATA 实际身份及 TLS 管理连接分别核验。备份前后与恢复点前后核对 postmaster 代次和恢复参数，源重启后不能沿用旧备份绕过新参数要求。原 MD5／SCRAM 凭据保留，私有恢复端仅监听 Unix socket。

`backup` 只生成已认证的数据库加密副本；`checkpoint` 创建独立命名恢复点并等待真实连续 WAL 归档。WAL 核对 segment 名称、页头、集群与分支历史，合法祖先 timeline 前缀不误拒绝；大目录采用加密分片清单，失败只清本次拥有且未被 checkpoint 引用的代次。配置的总备份字节、WAL 段数、子进程期限和可选限速生效；未完成明文仍被活进程占用时拒绝重入，不删除它。

`restore` 到新的私有目录执行实际 PITR：达到认证恢复点后退出 recovery，核对读写状态并完成临时写入读回；恢复后的角色、数据库、schema 和业务行计数重新读取。恢复收据绑定本次 checkpoint 摘要，后续 checkpoint 更新会使旧收据失效。取消传入实际解密流和恢复进程，停止并回收本次资源后才清密钥。该链仍明确为数据库范围，不代表原应用文件／设备材料、异机恢复、正式规模 RPO／RTO 或部署激活通过；这些须继续完成独立验收，不能仅凭 `backup` 或本机 `restore` 放行。

`node storage-backup.mjs schedule` 是默认关闭的周期数据库备份协调器。它只读 root 拥有、0600 的固定 `/etc/harbor-storage-schedule.json`，模板见 `schedule.example.json`；不接受调用方配置路径，不修改原备份／归档 profile。默认全备周期24小时、恢复点周期5分钟、检查周期1分钟，均可在该独立配置内调整，重启协调器生效；关闭后不再新增备份／恢复点，已有密文和WAL保留。缺少配置或非法参数会失败，不自动安装或启用服务。`schedule.service.example` 提供常驻进程入口，安装前核对实际工具路径、独立资源预算及原 profile 的 `maxRateKb`，不要与每日全备 timer 重复启用。

协调状态加密认证并绑定集群、原备份配置摘要和密钥版本；物理操作前持久化本轮UUID，取消／进程退出后仍沿同一UUID核验续跑。创建目录后、准备日志前退出留下的私有空目录可续跑，缺日志且含未知文件则拒绝。首次成功全备和真实WAL恢复点都完成后才发布活动备份；后续同一进程保留已验证的私有proof，不每5分钟重读整份大备份。进程重新启动后须完整核验活动备份才能复用；集群代次／timeline变化则另建全备，不能继续给旧代次追加恢复点。按本机规范化artifact目录互斥，不支持多主机同时写同一共享备份目录。每周期输出不含凭据的结构化状态，故障退出，由服务管理器按受限策略重试；正常SIGTERM等待本次取消收尾后退出，清理失败不当作正常停止。

同一备份目录中，原格式的认证 `application/application.enc` 或 `operator-final.enc` 一旦绑定检查点摘要，手工 `checkpoint` 会拒绝覆盖，协调器自动新建替代活动备份；未知格式、未完成的外来引用、认证损坏或摘要不一致均拒绝操作，不能通过手工pin或到期轮转绕过。应用摘要绑定和检查点发布使用同一备份目录锁，保留原pending流程先捕获应用、再建点、最后绑定的顺序。单纯数据库 `restore.enc` 仍保持原语义：后续检查点更新使旧恢复收据失效。

外置应用材料、独立密钥加密包、接收包或恢复批准无法从当前备份目录完整发现，仍必须列入 `pinnedBackupIds`；工具不扫描任意路径或读取其他密钥。旧手工基线不会被协调器主动接管。该入口仅完成数据库周期，不采集新的应用材料、不重放模型、不删除备份或WAL，始终返回 `applicationMaterialsVerified/restoreVerified/offhostVerified/deploymentAllowed=false` 和 `retentionApplied=false`。应用材料一致快照、引用核验后的保留清理及实际RPO／RTO仍须单独闭环，不能用定时配置值当作恢复承诺。回滚时停止本协调服务并关闭配置，可继续使用原手工CLI；不要删除认证状态来强行重置失败。

## 认证包与无源接收端恢复

最终 pending 备份默认不制作传输副本。调用 `createStorageOperatorBackup()` 时显式开启 `prepareReceiverBundle: true`，才会在同一受控写锁回调内，于本次备份的 `handoff/` 子目录生成认证包。必须先完成真实本机 PITR；包只包含当前备份实际引用的数据库、应用文件、检查点清单、WAL 及所需 timeline history 密文，不复制整个持续增长的归档目录。分页清单逐文件绑定长度与摘要；`handoff.enc` 另绑定源机器身份摘要、system_identifier、密钥版本、原 pending 绑定、恢复凭据与最终材料摘要。恢复凭据只在该加密文件内，不含源连接地址；归档密钥须在接收端独立配置，不能随包明文传输。开启后会额外占用密文包空间，应为传输副本、WAL 与实际恢复预留空间。

接收端命令为 `node storage-receiver.mjs restore`，只读取固定 root 配置 `/etc/harbor-storage-receiver.json`，不接收 URL、任意配置路径或 shell。模板见 `receiver.example.json`；工具不安装配置、不配置远端 SSH、不启动监听、不部署或激活服务。管理员须核准包来源的机器身份摘要、集群标识、密钥版本及 `handoff.enc` 的准确摘要与长度，而非根据收到的包自行信任其内容。

配置文件须 root 属主、0600。`bundleDirectory` 和 `workDirectory` 为 root 属主0700且互不覆盖的目录；`recoveryDirectory` 为明确的非 root PostgreSQL UID/GID 所有的独立0700目录，三者不能互相覆盖。独立配置的32字节 `keyFile` 须该 PostgreSQL UID 所有、0600。工具链及其父路径须 root 属主、不可被组或其他人改写。恢复目录路径须足够短，工具在复制前核验 Unix socket 的108字节限制。WAL 使用有认证长度上限、摘要核验与 fsync 的复制方式，可跨工作盘和恢复盘的文件系统；导入的文件、分页元数据和密文总长度受配置的数量／字节预算约束，原 incoming 包始终保留。

接收能力只能由实际完成认证复制的 receiver 产生，不接受 JSON 冒充。其数据库连接只指向本次新建的本机 Unix socket，无需原数据库在线；恢复后核对原数据库／角色清单、目标恢复点、退出 recovery 和实际写读，再从恢复文件核对原 Key、精确金额、正文／派生／导出密文与既有设备材料。它不能创建新源备份、检查点，或充当新的 pending 源能力。取消须等本次恢复进程组实际退出后才清理；无法证明退出时保留本次工作目录及恢复所需文件，不递归删除仍可能使用的明文。

任务内测试入口只接受已品牌 laboratory 下的私有目录，始终是 `task-owned-receiver`。同机恢复不能证明异机：只有固定 root profile 且实际源与接收机器身份不同、相应数据库／文件与密文材料验证成功，才可能报告范围明确的 `offhostDatabaseAndFilesVerified`。完整候选运行配置、TLS／存储报告组合及激活消费链尚未完成时，`applicationMaterialsVerified`、`offhostVerified` 和 `deploymentAllowed` 均保持 false。机器身份摘要依赖管理员确认的实际主机身份，不能把不同容器的标识当作不同物理主机验收。

本机真实回归覆盖停掉源 PostgreSQL 后的完整接收恢复、恢复进程组被 SIGSTOP 后取消、原包保留、错误摘要／容量拒绝，以及最终父目录同步失败时拒绝发布、清理本次包并保留原备份（仅该同步失败边界注入，其余PG／复制链实际运行）；独立真实文件系统测试覆盖跨盘 WAL 复制与认证长度放大拒绝。它们不替代正式 root 跨 UID、真实不同主机、完整候选组合或生产规模 RPO／RTO 验收。

## 恢复副本的实际源码运行组合

本机隔离验证可在 `createStorageOperatorBackup()` 显式设置 `verifyRestoredRuntime: true` 并提供原证书对应的 `restoredRuntimeTlsHost`（默认关闭），目前仅允许 `task-owned` 来源。它在活的恢复 session 内、原材料与密文校验之后，使用同一预检的材料校验核心，启动本仓实际 PostgreSQL 网关和独立 Worker；不注入替代的存储 probe，不请求上游模型，也不新增常驻服务。源镜像身份仍由原 `postgresPreflight()` 强制校验，材料校验核心不能代替镜像证明。

验证连接、PGDATA、应用目录和原角色只从受品牌恢复 session／应用计划取得；JSON、外部连接 DTO 或“passed”字段不能开启该验证。仅在这份隔离恢复副本中启用原短期 runtime 租约，启动／关闭／租约期限复用受信 operator 的预检预算；原 admin 保留精确 MD5 认证规则，其余本地角色使用 SCRAM，默认关闭时沿用既有恢复认证行为，不修改源集群。报告 publisher 以实际恢复 PGDATA／`pg_control`／关系文件及同 UID 的 `/proc` 观测签出报告，使用恢复出的独立 `postgres-report.key`；网关和 Worker 以 report 模式消费它，同时核验本地恢复数据与 spool 文件系统。

TLS hostname 从受信调用参数进入认证应用 plan，再由活 session 取得；实际连接固定回环地址，同时保留原 hostname／SNI、Host 与完整证书身份校验，不要求原证书必须有回环 IP SAN，也不重新签发恢复材料。运行核验包括真实 TLS 服务器证书、旧员工 Key 的数据库认证，以及在原员工／Key策略允许无设备证明时，经该 TLS 入口读取 `/api/me`。已强制设备证明的原策略不被修改；缺少实际客户端证明时单列 HTTP 验证缺口，不能用新签的设备证书冒充旧设备续用。独立 Worker 必须实际完成本次启动后的后台任务，随后重新核验两者的报告绑定及采集可用状态。取消进入受 session 约束的统一清理阶段；恢复PG若暂停，只先恢复已证明归属的进程以完成清理，PG backend可另建session且改写进程标题／环境，因此沿已核验postmaster的内核亲缘及PID启动代次持续核验，不以主进程组为空代替全实例退出；跨UID进程只参与可见占用观察，不能加入信号集合；发现未知PGDATA使用者保留拒绝，不杀未知进程。管理查询同时受客户端期限约束，避免PG暂停时仅服务端超时不生效。随后先排空网关与 Worker、停止并等完 publisher、撤销恢复副本租约，核实恢复 PG 此时仍在线，并写入仅含范围／状态的加密 `runtime-cleanup.enc` 审计，最后才释放 session／停止恢复 PG／删除副本；关闭响应错误、超时、原关闭结果为强制／未清空或进程非正常退出时不发布成功凭据；无法证明应用进程退出时保留其恢复文件，不递归删除仍可能使用的材料。

返回范围为 `task-restored-source-runtime`，其中各项结果只表示实际完成的源码／材料组合验证。`candidateImageVerified`、`applicationMaterialsVerified`、`offhostVerified` 和 `deploymentAllowed` 仍为 false。真实 candidate/Docker、跨 UID、真实不同主机、原部署拓扑以及最终 activation 继续单独验收；本机 loopback TLS 和源码运行不能替代它们。原 Key 不可用、报告失效／错签名／错库或集群、原材料缺失、Worker 未进展或清理不明均不能默认为通过。

## 任务内真实 OCI 业务镜像恢复组合

在上述显式验证中，额外传入 `restoredRuntimeCandidate` 可选择真实 OCI 业务镜像。该值只能由 `createTaskRestoredCandidate()` 产生，并显式钉住已核 CI 的项目、流水线、构建／桥接／验证作业、revision 与镜像 digest；JSON 或复制对象不能替代。每次启动重新核对本地 CI 材料和实际 OCI 对象，namespace 内重新验证层内容，业务文件保持只读。新增固定 `restored-report` 入口只提供 IPC 启停控制，直接调用镜像中的原网关、Worker 与含镜像身份验证的 `postgresPreflight()`，不覆盖业务模块，也不注入 local probe。缺省 namespace 入口与正式运行证明的 argv 规则不变。

恢复出的 data、runtime、完整 secrets、PGDATA 和 Unix socket 组成原五个材料根；报告仍由实际恢复 PGDATA 与进程观测签出，镜像仅以 report 模式消费。原恢复角色的短期租约及 application identity、旧 Key、原 TLS hostname、Worker 进展和清理审计继续生效。候选上下文摘要进入认证应用计划，进程内品牌不随备份包传输；接收端未重新建立该上下文时明确拒绝，不回退源码模式。

结果范围仅为 `task-restored-oci-runtime`，`actualOciBusinessRuntimeVerified` 只证明本任务同 UID 的真实 OCI 业务组件组合。`candidateImageVerified`、`applicationMaterialsVerified`、`offhostVerified` 与 `deploymentAllowed` 均仍为 false；正式 root 跨 UID、Docker 与原部署拓扑、不同机器和最终激活消费仍须单独验证。

## 固定 root 接收端的候选消费

接收端可在固定 `/etc/harbor-storage-receiver.json` 中显式增加 `candidateRuntime`，内容参考 `receiver-candidate.example.json`；省略时保持原数据库／文件恢复行为。该段批准接收端已有的真实业务 OCI、成功 CI 生产／候选验证链、原 hostname、原生启动工具、两个固定 harness 文件及独立 publisher 代码与 Node 摘要，不能由收到的备份包决定工具或候选。实际引用的 OCI layer 在每次启动、完整收尾和签发前均流式复核长度及 SHA-256，累计读取受批准的 `tmpfsBytes` 字节预算及 `timeoutMs`／取消信号约束。文件及祖先须 root 所有且不可被组／其他人改写；OCI、工具与 harness 还须允许 `restoreUid` 只读／执行。示例不是可直接安装的授权，也不自动安装配置或授予权限。

开启候选消费时，建议另选独立的 `recoveryDirectory`，其须为 root 属主、`restoreGid` 属组、0750，且所有祖先均由 root 控制、不可被服务 UID 改写；本次 `recoveryWorking`、恢复容器目录及其上层同样保持 root:restoreGid 0750。PGDATA、socket 和认证后的 application 才交给 PG UID。省略候选段的旧 profile 仍沿用原 PG UID 私有恢复目录要求。关闭候选消费或回滚时一并恢复原 profile 的恢复目录，不能只删除候选段后继续使用新 root 恢复根；原目录与源数据不被本工具改写。移交沿固定目录 FD、禁止跟随链接的打开和 `fchown` 执行，拒绝硬链接；原 `postgresql.auto.conf` 的改写同样核对真实文件属主并禁止跟随链接，不对可被替换的路径执行 root `chown`。

publisher 使用独立 `harbor-receiver-publisher-source-v1` 清单，覆盖控制器代码、JSON 与完整实际依赖；可由 `receiverPublisherSourceManifest()` 在准备材料时生成，清单本身放在源目录外，之后由管理员核准其 SHA-256。不能直接使用未包含依赖的镜像源码清单代替。固定 source-loader 在 Node 入口前逐模块验证 root 权限及摘要，禁止从祖先 `node_modules`、遗漏文件或其他路径加载；实际加载清单也随 publisher 回执复核。没有 root 批准或缺任一依赖时拒绝启动，不通过放宽权限解决。

只有由固定 root profile 实际产生的 receiver 能建立独立 `root-profile-restored-oci-runtime` 品牌。已有 task 品牌不跨包、不升级为 formal；原迁移目标 `targetDigest`、checkpoint、数据库／角色及材料绑定与 OCI digest 分别核对。批准的候选信息只进入接收端本次活应用计划，不改写原认证备份清单。控制器保持 root，恢复 PG、报告 publisher 和候选网关／Worker明确以 `restoreUid/restoreGid` 启动；应用文件在认证恢复／核验后归该 UID，原材料权限不放宽。原生 namespace 入口、报告模式和五个材料根保持不变。publisher 在 PG UID 使用真实 PGDATA、关系文件和同 UID `/proc`，以原独立报告密钥签发；不向业务 namespace 暴露宿主完整 `/proc`。

专用子进程管理复用恢复进程的 nonce、实际 UID、内核亲缘与 PID 启动代次观察，通过 pidfd 绑定进程实例后重核 UID／启动代次，再执行恢复／关闭；不回退数字 PID 信号。固定 root wrapper 与共享 helper 纳入批准源码清单，普通 helper CLI 仍只允许原同 UID 的 STOP／CONT／KILL。未知或无法证明退出时拒绝成功并保留恢复材料。候选必须通过原镜像预检、原 TLS hostname／SNI、旧 Key、独立 Worker 进展与新鲜报告，且实际排空、publisher 停止及租约撤销通过后才返回组件结果。组件的 B digest／revision、publisher 源摘要和执行范围进入恢复 summary，由原 receiver 的 `resultDigest` 一并覆盖；不增加未签名的旁路成功标记。

本机若没有 root→不同操作系统 UID 的执行入口，必须明确保留该真实组合缺证，不能用 user namespace 的映射 root 或 task 正向结果代替。固定 root 的组件实现也不代表 Docker／原部署拓扑、真实不同主机或设备客户端续用验收完成。强制设备的旧私钥仍留在员工设备；服务端 CA／登记校验不能代替旧设备实际连接挑战，缺此回执继续列为缺口。`applicationMaterialsVerified`、`offhostVerified` 与 `deploymentAllowed` 仍保持 false，activation 与发布 gate 不在本段开放。

## 回退边界

优先使用能读取新 PostgreSQL、继续执行模型/限额/设备门禁的兼容版本。旧 `session-metadata-only-v1` SQLite 镜像不得直接启动或恢复旧备份丢弃新数据。切换后有新写入时，先保全最新数据库、WAL 和文件材料，不把“旧库能打开”当作回退成功。

`requests.inserted_xid` 和 PG_SNAPSHOT 依赖源集群语义；逻辑 dump 跨集群恢复不能仅靠行哈希宣称等价。当前不实现未经验证的逻辑重建/SQLite 反迁移激活。

`validateRollback()` 必须使用本进程实际 `recover()` 的结果，并再次连接恢复库核验真实 schema 和集群身份，只给存储兼容证明。它始终返回 `deploymentAllowed:false`，仍要求应用验收和受控部署入口检查实际候选镜像；外部传入的标签、checksum 或伪造恢复对象不能批准部署。`validateRollback()` 不执行候选镜像核验、合并或发布。

## 隔离 PostgreSQL／设备 TLS 基础设施准备

`python3 ci/storage-recovery/bootstrap.py check|prepare|start|bind-report` 只读固定 root 配置 `/etc/harbor-postgres-bootstrap.json`（0600），格式见 `bootstrap.example.json`。默认不存在任何隐式安装或切换；命令必须显式指定。`check`只核已安装镜像、原容器及入口；`prepare`在新的固定 `postgres-infra` 目录生成独立数据库 TLS、设备 TLS、数据库初始化密码、独立报告与归档密钥，并独占新建固定`/etc/harbor-storage-archive.json`无效占位；原目录或未知profile存在就拒绝覆盖，失败材料保留。生成的`endpoint.json`只含固定网络、DB hostname、verify-full与材料相对路径，不含管理员或runtime凭据；runtime账号／目标库／密码仍由activation facts填入。它不改原 `data`、`runtime`、员工 Key 或 HTTP 地址，不停止原容器，不改部署引擎、不执行迁移、不签发 activation。证书分别覆盖实际数据库名称／loopback和配置的设备入口 IP／域名；原 HTTP 员工入口保持，设备入口必须是独立 HTTPS。不得使用 localhost 测试证书替代真实入口证书。

基础设施镜像使用本目录 `postgres-runtime.Dockerfile`，从既有固定官方 PostgreSQL 18 镜像及已核准的业务 B digest 构建；B 通过 `HARBOR_BUSINESS_IMAGE` 提供，须包含正式 PG 业务能力且不是 CI-only P。镜像标签明确是 `postgres18-report-v1` 基础设施，不作为业务候选，不可替换 B 或跳过原镜像／迁移预检。所有镜像须预先通过现有 CI／受控镜像交付入口安装并按 digest 校验，bootstrap 不接任意下载地址或命令。

`start`只启动自己的独立 PG 容器／网络，数据库端口仅绑定127.0.0.1，初始4CPU／8GiB；不会绑定原HTTP或设备公开端口。新容器保留官方 PG 初始化入口，publisher与真实 PG 同容器、同 PostgreSQL UID及PID／文件系统namespace，通过实际Unix socket、`pg_backend_pid()`、`/proc`、`PGDATA/pg_control`和关系文件核验，不以另一sidecar所见目录冒充同机证明。publisher每5秒刷新有效期15秒的签名报告；业务镜像仍须按report模式验签和时效。配置的 `publisherDatabase` 初始为null，迁移目标形成前明确不发布报告；root核对真实目标后更新固定配置并显式调用 `bind-report`，不能以创建替代业务表或伪造成功填补预检。启动失败保留新PG数据／资源供明确恢复，不自动销毁数据库。

原来源隔离仍由 operator 办理。缺省保持systemd模式，`source.supervisorUnit`必填，须实测masked/inactive；仅当运维确认Docker为唯一启动管理方时，才能在固定root operator配置显式设置 `source.supervisorKind: "docker-restart"` 并省略`supervisorUnit`，不能自动降级。两模式均核对固定容器代次、镜像、用户、来源目录、`restart=no`、正常退出及同项目无其他活动源。回退配置时恢复原`supervisorUnit`并移除`supervisorKind`。bootstrap只报告Docker容器状态和该配置建议，始终不代替运维认定管理方唯一、来源已隔离或迁移获准。

该准备不包含正式归档／备份工具链、异机receiver baseline、迁移角色激活或业务发布；这些依赖继续由既有root受控流程验证。没有Docker daemon的本机只能运行规划／拒绝和真实私有PG报告测试，不能据此声称镜像组合通过；镜像entrypoint及同容器验证须在获准的隔离Docker或CI入口实际运行。阶段8小时稳定性也须明确实际持续时长与非formal范围，不能用短selfcheck或run-lifetime预算代替持续执行，更不能清除未完成的正式远端helper／browser缺项。

`sourceMaterialPreparations`另外只读检查原`data/recording-encryption.key`的存在、普通文件类型、UID1000／0600与32字节长度；元数据通过不代表内容已验。旧SQLite可能没有该材料，须在正式迁移前由明确授权的材料准备步骤为缺失项生成新的独立正文主密钥并安装到源data，不能复用`key-encryption.key`或数据库报告key；已存在的材料绝不覆盖或自动重生成，异常须核查。bootstrap只报告该待办，不生成或安装此正文密钥，不写原data。原Key主密钥及账号登录材料保持，最终恢复应用与存储预检仍须实际核验正文材料。

### 可选 CI 基础设施镜像验证

默认流水线不运行基础设施验证，也不把它加入原业务镜像或部署门禁。创建流水线时显式设置 `HARBOR_POSTGRES_INFRA_SMOKE=1`，待同流水线 B 构建完成后，手动运行 `image_postgres_infrastructure`；后续准备和child smoke自动消费这两份同project/pipeline/commit/producer的严格digest收据，不回退tag或旧流水线镜像。可选作业失败不自动部署，也不改变现有候选验证与发布依赖。

构建沿既有Kaniko与registry入口，无需job内Docker daemon。真实启动使用Docker executor的CI services：服务以infra digest运行仅CI显式启用的空数据fixture wrapper，准备临时DB TLS／报告密钥后exec普通infra入口及官方PG初始化；普通服务器入口不受影响。测试用共享CI项目卷在checkout完成后只交接公开CA，私钥不离开服务；报告key仅经已验证的TLS在本次隔离测试内读取，不写日志或artifact。业务B按digest作为job镜像，CI job显式root用户只为共享公开CA和临时文件操作；PG及publisher仍是实际postgres UID，同真实PG/proc命名空间。

smoke核对B源码revision、服务的本次job/pipeline/commit标记、PG18、完整TLS及错误hostname／明文拒绝、真实schema迁移、同数据库宿主签名报告与持续刷新、篡改签名拒绝。结果固定`productionMigrationVerified:false`、`deploymentAllowed:false`；CI服务退出由Runner管理，不冒充生产迁移、角色激活、设备客户端验收或服务停机演练。需要Runner支持Docker services、私有registry digest拉取、共享项目卷及`image.docker.user`（Runner16.8+）；现有Runner18.10已实际运行原candidate服务，但本新增组合必须在本次可选作业成功后才算通过。

实际PGDATA固定为`/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker`，宿主`ROOT/db`按相同绝对路径挂入容器，publisher和CI smoke使用同值；因此SQL返回的`data_directory`可被宿主正式backup入口直接lstat，不使用路径转换或放宽核验。镜像内只预建可遍历父路径，数据库子目录为postgres999私有；官方镜像未使用的默认volume由只读空目录覆盖，避免另建未管理匿名数据卷。WAL archive目录／key和无效占位profile由bootstrap按同路径预建挂载；观测cluster绑定、完整backup元数据和实际WAL验证仍由正式流程办理，bootstrap不据挂载存在宣称连续归档已可用。

专用infra宿主`ROOT`为root/0711，仅允许其他UID沿已知路径遍历，不能列目录或写入；宿主PITR的PG999进程因此可读取自身`archive/`(999/0700)与`archive.key`(999/0600)。`state.json`、`endpoint.json`、`pg-ca.key`仍root/0600，`app-secrets`仍root/0700，设备和其他私钥权限不变。`load_state`精确校验0711，`check`不会自动chmod。已由本工具准备为root/0700的专用目录，须管理员核固定路径、root属主、无符号链接、原owner/configuration及私有子材料后，仅将该根显式改为0711，再重入`bootstrap.py check`；不得递归放宽子树或以此处理未知目录。回退为0700会再次阻断宿主归档恢复，不能在该权限下宣称PITR可用。

应用材料仅装配到`ROOT/app-secrets`，根为root/0700，精确包含`postgres-ca.crt`(root/0644)、`postgres-report.key`(1000/0600)和`device/`(1000/0700，私钥0600、证书0644)。此根不含PGDATA、DB CA私钥或管理员密码；endpoint的`applicationSecretsSource`以及各材料相对路径均指向这里。operator的`candidate.secretsDirectory`及`fileRoots.secrets`应一致批准此根；实际deploy再由受控材料安装将精选内容放入`stack/secrets`，保留既有registry等独立目录，不覆盖整棵stack或将整个infra ROOT作secrets。CI smoke只证明PG/TLS/report组合，不代表该实际operator挂载及激活已验收。

PG首次初始化即显式`wal_level=replica`、`archive_mode=on`，避免以后为启用归档重启已承载业务的源。归档command、独立key和同路径挂载已预置，但profile仍是无效占位，endpoint明确`awaiting_observed_cluster_binding_metadata_and_wal`，bootstrap结果`walArchiveReady:false`；不宣称连续WAL已经可用，不得在此状态激活业务。Root须通过下述固定入口完成实际cluster绑定、既有完整backup元数据及真实WAL验证，不能用成功标记代替；未绑定期间原命令失败并保留WAL，应保持无业务并监控空间。

正文主密钥的明确准备入口为`python3 ci/storage-recovery/prepare-recording-key.py`（无参数、固定root operator配置）。它核实际Docker来源，仅在原data缺少`recording-encryption.key`且没有recording-spool条目时独立生成32字节、1000/0600，原合法材料保留，异常或复用旧主key／批准secrets根报告key拒绝；不读数据库、不停服务、不安装配置。该入口须先在受信root位置安装核验，再由发布窗口显式执行；组件测试及本说明均不代表已在线准备。

官方initdb的固定初始化SQL仅作用于本工具新建的专用空集群，撤销全部初始数据库（含postgres/template1）的PUBLIC CONNECT/TEMP及当前库public schema默认CREATE；不在已有PGDATA或共享集群执行。Unix peer只匹配postgres管理／报告角色，其余local和hostssl路径用SCRAM，明文拒绝；后续runtime仍由正式迁移创建并核验自身目标库权限、无其他库CONNECT／无成员关系，不能借管理员URL绕过issuer。CI smoke另核实际数据库ACL已撤销PUBLIC通路。物理复制不匹配HBA的数据库`all`，bootstrap默认不开放physical replication。首次physical backup前，管理员须从实际root管理TLS连接读取`inet_client_addr()`，与本项目owned Docker bridge的gateway交叉核实后，仅为该单一来源批准IPv4 `/32`或IPv6 `/128`的`hostssl replication postgres` SCRAM规则，并显式拒绝非TLS复制；不得自动开放`0.0.0.0/0`或`::/0`复制准入，其他角色没有复制HBA许可，runtime也不得取得REPLICATION属性。核对原HBA字节、专用集群与归属后，仅替换已批准的固定HBA并调用`pg_reload_conf()`，再核`pg_hba_file_rules`及实际备份结果；普通数据库规则、监听和宿主回环发布端口不变，不重启PG，bootstrap的`check`不会自动改已有HBA。

宿主Node+业务源码安装入口为`install-tools.py`，仅读固定`/etc/harbor-storage-tools.json`(root0600)，明确批准业务B digest、infra digest、revision及Node24.21.0；它只消费已有本地镜像，不启动临时容器，核真实Node／imageManifest后原子发布`/opt/harbor-storage-tools/{node/bin/node,app}`，已有非空目录拒绝覆盖。输出及installation.json保持`postgresHostToolsInstalled:false`，不能据Node+app安装成功认定宿主PG备份工具链或其他迁移前置已齐全；该工具组件验证不代表已在服务器安装。

新空集群的固定initdb参数为`--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8`，与现有恢复／容量私有PG入口一致；CI smoke实际核`server_encoding=UTF8`及`datcollate/datctype=C`。不依赖官方镜像LANG或宿主glibc语言排序版本，也不会给已有库修改collation。

### 首次连续归档的安全准备

bootstrap在新infra ROOT预建`archive/`(PG999/0700)、独立`archive.key`(PG999/0600)及固定`/etc/harbor-storage-archive.json`(root0644)占位文件，Root state记录其原始dev/inode。archive目录、key在宿主和PG容器内同绝对路径，目录RW、key只读；固定profile单文件同名只读挂载。普通PG入口从首次启动即配置既有B代码`storage-backup.mjs archive-wal %f`和archive_timeout=60。占位expectedCluster为空时原归档程序明确失败，PG保留WAL；不能把容器可启动或archive_mode=on当作归档成功。

`python3 ci/storage-recovery/prepare-archive.py`是固定root、无参数的准备／复核入口。它核已批准infra镜像、owner标签及实际挂载，通过容器PG999的固定回环TLS连接，以挂载的独立管理员密码及CA完成证书／主机名验证，并核pg_stat_ssl后取得真实system_identifier，与宿主PG_VERSION／pg_control核对；仅在一致时同inode写原占位profile并fsync。采用该inode的独占锁和root0600小journal；journal与备份key先完整写入同目录临时文件并fsync，再以renameat2 no-replace发布，读取恢复journal后先同步其目录，读回profile后无条件fsync再移除journal。控制器中断导致部分写入时，下一次仍须重新核实际PG身份，才能按同一已准备绑定完成；未知内容、错误inode／key、非root journal不覆盖。profile绝不原子换文件，避免已挂载的旧inode长期不更新。只独占生成／核验相同归档key的root0600`/etc/harbor-storage-backup.key`副本，不覆盖已有不同key，不复用report或源加密主密钥。

返回明确区分`profileBound`、`metadataReady`、`walArchiveVerified`。新helper不生成`cluster.enc`，完整backup/operator inventory/profile及宿主工具由Root准备后，显式运行原有`storage-backup.mjs prepare-archive`初始化同一config digest／keyVersion／UID的认证绑定。之后须实际产生WAL，原archiver真正成功，且现有`inspectArchivedWal`验证对应加密segment、认证摘要和cluster绑定后，才可能ready；始终`deploymentAllowed:false`，不代替部署或正式恢复验收。占位、metadata未齐和WAL未验均只报下一步缺口，归档失败时保留WAL，需监控空间。

本机UID0测试仅使用新/tmp私有目录核root/PG999权限、同inode、并发锁及中断恢复，不安装真实/etc或/opt；私有PG测试只验证真实WAL与文件完整性组件，不升级为已安装root profile或实际Docker/173组合通过。
