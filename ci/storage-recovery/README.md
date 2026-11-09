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

固定 root `/etc/harbor-storage-operator.json` 可选 `transferBatchRows`，仅接受整数 1～500；省略时继续沿用每批100行，且不改变旧配置摘要。每批仍受2MiB、单行1MiB上限约束，事务前后均执行原来源围栏与 SQLite lease 检查；增加行数只减少批次数，不减少每批校验。显式字段纳入可信配置摘要，已有 operator state 不允许中途增加或更改此值（包括显式补写100）；需沿原配置续行，或在原任务安全收尾后另行准备新任务。发布可经实际预算验证后显式配置500，不作为所有环境默认值。

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

`backup`、`schedule`、`maintain-base` 三个服务模板均使用 installer 的固定 `/opt/harbor-storage-tools/node/bin/node` 与 `app/storage-backup.mjs`。启动前核对已安装工具树及固定配置；模板不会安装或启用任务。停止时仅向主CLI发送SIGTERM，等待其取消操作、子进程退出和租约释放，不额外按固定秒数强杀；单次操作预算仍由备份配置约束。若停止挂起，应检查实际进程及材料，不能把停止命令返回视为清理完成。回滚模板不删除配置、备份或WAL，也不改变已运行的服务。

同一备份目录中，原格式的认证 `application/application.enc` 或 `operator-final.enc` 一旦绑定检查点摘要，手工 `checkpoint` 会拒绝覆盖，协调器自动新建替代活动备份；未知格式、未完成的外来引用、认证损坏或摘要不一致均拒绝操作，不能通过手工pin或到期轮转绕过。应用摘要绑定和检查点发布使用同一备份目录锁，保留原pending流程先捕获应用、再建点、最后绑定的顺序。单纯数据库 `restore.enc` 仍保持原语义：后续检查点更新使旧恢复收据失效。

外置应用材料、独立密钥加密包、接收包或恢复批准无法从当前备份目录完整发现，仍必须列入 `pinnedBackupIds`；工具不扫描任意路径或读取其他密钥。旧手工基线不会被协调器主动接管。该入口仅完成数据库周期，不采集新的应用材料、不重放模型、不删除备份或WAL，始终返回 `applicationMaterialsVerified/restoreVerified/offhostVerified/deploymentAllowed=false` 和 `retentionApplied=false`。应用材料一致快照、引用核验后的保留清理及实际RPO／RTO仍须单独闭环，不能用定时配置值当作恢复承诺。回滚时停止本协调服务并关闭配置，可继续使用原手工CLI；不要删除认证状态来强行重置失败。

### 应用周期点、全备维护与真实恢复分离

源端 SSH 策略模块 `postgres-periodic-ssh-policy.mjs` 只读取 root／0600 的固定 `/etc/harbor-storage-periodic-ssh.json`，缺失或 `enabled:false` 时关闭，模板见 `periodic-ssh.example.json`。它核对 root 私钥（0600）、不可被组／其他用户改写的主机公钥文件、root／0700 索引目录及受保护祖先；启用时这些材料须预先准备，模块不会创建密钥、安装授权或发起连接。`host/port/user`、`identityFile/knownHostsFile/indexDirectory` 和两项超时由管理员配置；身份与主机公钥文件使用无空白、无 SSH token／环境变量展开的绝对字面路径。参数固定 `/usr/bin/ssh`、非交互身份认证、严格主机公钥核对、禁用代理／转发及远端命令 `harbor-periodic-receive`，不使用默认身份或修改主机公钥文件。策略每次调用读取；关闭阻止后续任务，已开始任务沿原预算收尾。该模块提供配置和参数；实际发送由独立固定 CLI 办理，配置启用本身不证明传输或异机恢复成功。

固定 CLI 为 `node storage-periodic-transfer.mjs send|receive`，只读固定配置，不接收任意配置路径。`send` 认证已有 `latestBackup` 周期点并生成轻量传输索引，在 SSH 子进程真实关闭后才释放传输租约。索引续跑允许在 `handoff.enc` 尚未发布时，复用完整写入、AEAD 认证且内容与预期 catalog 匹配的页；未知文件、损坏认证或不匹配内容保全并拒绝，不承诺任意 SIGKILL 遗留的半写文件都能自动清理。`receive` 只接收并认证密文传输，不执行数据库／应用恢复，也不签发恢复或异机验证证明。

自动传输与手动 `periodic-handoff` 共用所选源点原 `handoff/handoff.enc` 和目录页字节。自动入口先在该固定位置发布canonical控制闭包，再将相同控制字节复制到传输索引目录；base、所选point、应用材料及WAL仍从原不可变密文引用读取，生成索引不预制完整本地包。手动导出沿同一目录页补齐独立完整拷贝，不重加密控制清单，只有完整拷贝验证通过才返回导出结果。完整旧手动包直接复用；源点尚无canonical时，旧索引只有与当前认证源闭包完全匹配才可按原字节采用，两处已有不同控制字节时保全拒绝，不放宽源端恢复证明的包摘要比较。

源点控制闭包与手动补齐共用互斥，生成、拷贝和传输仍在各自既有的archive操作租约内进行。取消保留已经认证的控制页和匹配拷贝，清理本次未完成文件后释放资源；未知或损坏材料不覆盖、不当作成功。保留器认证同一控制闭包及任何已存在的拷贝，对尚未拷贝的条目核对原base／point／应用文件及WAL引用；只有控制页不等同材料缺失，也不允许提前删除其恢复依赖。源端验签继续使用原point路径及严格expected，自动与手动恢复签名不会因重新生成控制密文而形成不同包摘要。

自动发送模板为 `periodic-ssh.service.example` 和 `periodic-ssh.timer.example`，分别对应管理员安装后的 `harbor-postgres-periodic-ssh.service` 和同名 `.timer`。它们默认不安装、不加载、不启用，也不修改 SSH policy；policy 缺失或 `enabled:false` 时，CLI 返回关闭状态。`install-tools.py` 的实际安装根为 `/opt/harbor-storage-tools`：Node 位于 `node/bin/node`，应用位于 `app/`，模板只调用 `/opt/harbor-storage-tools/node/bin/node /opt/harbor-storage-tools/app/storage-periodic-transfer.mjs send`。安装前须核对已批准的工具树实际包含此 CLI 及其依赖、固定配置与资源预算；仓库中存在源码不等于当前服务器已经安装。接收端须另行准备既有固定 `receive` CLI、policy 和 SSH 授权，本模板不安装接收入口或密钥。

producer `schedule`、maintainer `maintain-base` 与 sender `send` 使用独立进程：发送仅选择已有最新认证应用周期点，不生产恢复点、不制作全备，也不执行恢复。timer 设为开机1分钟后检查，并在前一次服务退出后通过 `OnUnitInactiveSec=1min` 再次检查，不并发启动同一服务；开机时刻已过时启用 timer，首次检查可立即触发。这只是发送检查频率，不修改原备份间隔或承诺1分钟 RPO。传输还受固定 SSH policy 的 `timeoutMs`／`connectTimeoutSeconds` 和 CLI 的取消、租约与子进程结算约束。

停用时先停止并禁用 `harbor-postgres-periodic-ssh.timer`，再将固定 SSH policy 的 `enabled` 设为 `false`，阻止后续发送；若需取消已经开始的任务，再停止对应 `.service` 并等待收尾。只修改 policy 不会撤销已开始任务，它仍沿启动时读取的预算完成或取消。模板与全备维护一样不另设启动／停止强杀期限：`KillMode=mixed` 将 SIGTERM 交给主 CLI，CLI 负责取消本次 SSH、等待真实进程关闭并释放租约；`SendSIGKILL=no` 阻止服务管理器另行强杀。异常挂起时保留状态和材料并排查，不能把发出停止命令或超时当作清理成功。停用不停止 producer／maintainer，不删除已有索引、密文、恢复收据或源备份。模板仅提供可审阅的调度接线，未在真实服务器启用；`transportComplete` 仅表示传输完成，不等于接收端实际恢复、异机身份验证、RPO／RTO 或部署放行证明。

受限 SSH 接收包装器为 `periodic-receive.py`，由管理员核准后安装到 root 所有、不可被其他用户写入的 `/usr/local/lib/harbor/periodic-receive.py`。专用 SSH 账号仅允许公钥认证；下例账号名 `harbor_backup` 须与源端 policy 的 `user` 一致，且具有可执行固定命令的 shell。授权公钥文件及祖先由 root 管理，不允许该账号自行追加其他公钥；不要启用密码、用户环境或用户 rc。公钥行采用以下约束，替换占位公钥后才可使用：

```text
restrict,command="/usr/bin/python3 -I -B /usr/local/lib/harbor/periodic-receive.py" ssh-ed25519 REPLACE_WITH_SOURCE_PUBLIC_KEY
```

包装器只接受没有附加参数的 `harbor-periodic-receive` 原始命令，拒绝交互终端。管理员用 `visudo -cf` 核验以下专用 sudoers 规则；权限仅限这一个参数完全匹配的接收入口，不授予任意 Python、Node、shell 或恢复命令：

```sudoers
Defaults:harbor_backup !use_pty, !requiretty
harbor_backup ALL=(root) NOPASSWD: /usr/bin/python3 -I -B /usr/local/lib/harbor/periodic-receive.py --root-receive
```

提权后再次限制为固定接收阶段，并清除调用方环境；检查固定 Node、接收 CLI、工具校验器及其祖先，再复用 installer 的整树属主／权限／链接检查。固定 `/run/harbor-periodic-receive.lock` 的内核互斥覆盖首帧等待到接收进程退出，第二个接收任务立即拒绝，避免并发 SSH 会话在读取控制帧前堆积接收进程；锁文件由 root 管理并保留，不在运行时删除。仅执行 `/opt/harbor-storage-tools/node/bin/node /opt/harbor-storage-tools/app/storage-periodic-transfer.mjs receive`，从既有固定 `/etc/harbor-storage-periodic-transfer.json` 取得接收配置；该配置缺失或关闭时不加载密钥、不接收内容。标准输入输出专用于有界密文协议，错误只输出固定代码。包装器不会创建账号、安装公钥、写 sudoers、修改 sshd 或启用接收配置；真实接收主机须单独核实安装及信任，源端必须严格校验该主机公钥。停用接收配置可阻止后续收包，撤销专用公钥可阻止新 SSH 登录；已有任务按原生命周期收尾。仅收包不执行数据库恢复、不签名，也不取得部署权限。

独立全备维护模板为 `maintain-base.service.example` 和 `maintain-base.timer.example`，管理员核实际工具路径、资源预算及固定配置后，分别安装为 `harbor-postgres-maintain-base.service` 和同名 `.timer`。计时器每分钟检查一次，实际是否全备仍由 `fullIntervalMs`、源数据库代次和现有认证状态判断；同一服务执行中不会被计时器并行启动。它与 `schedule` 独立，不能同时启用旧的每日 `backup.timer` 来代替应用维护。模板不自动安装或启用；关闭时先停止计时器，再停止服务并等待取消收尾，保留已有密文与状态。首次备份尚未成功前，轻点仍报告未就绪，不承诺恢复能力。维护的启动及停止不另设强杀期限，沿固定备份配置和子进程取消机制收尾；异常挂起需管理员排查，不能强杀后把未确认清理当作成功。

应用周期默认关闭，仅 root 固定的 `/etc/harbor-storage-application.json` 显式启用后接入原 `schedule`。配置版本1，完整字段为 `enabled`、数据库 `name/oid`、`data/runtime/secrets` 三个材料根（可另设库中路径前缀 `databasePrefix`）、原 HTTP/设备 TLS origin、材料字节及文件数预算；配置不由 HTTP 调用方提交。捕获包含原加密 Key、上游密文对应密钥、正文密钥、账号目录及设备 CA/TLS/report 材料，仍需按实际部署配置完整根目录；配置启用本身不证明备份或恢复完成。

可选 `runtimeRebuildableCaches` 按已核实的材料角色声明可重建缓存，默认缺省或空数组保持完整捕获及旧策略摘要。支持 `codex-log-sqlite`（`runtime/accounts/*/logs_*.sqlite` 及其 WAL/SHM）、`codex-model-catalog`（`runtime/accounts/*/models_cache.json`）、`codex-launcher-aliases`（`runtime/accounts/*/tmp/arg0`）。最后一项只排除真实目录及其子树：Codex 启动会创建临时执行别名链接、退出会删除，恢复后由 CLI 重建；目录根若是链接仍拒绝，同名普通文件仍保留。未知角色、重复角色或其他区域链接均拒绝；auth、state、正文、设备及密钥继续完整覆盖。目录根计遍历预算，排除子树不遍历。角色选择进入策略摘要与加密文件清单，捕获、源复核及历史认证须精确绑定。旧字段继续兼容，重叠文件选择去重；启用前须更新源端及恢复端工具。回退移除角色配置即可恢复全量捕获，保留已有新点与配套工具，不删除源缓存或中断请求。

可选 `runtimeSqliteCaches` 声明已经核实可以重建的运行时 SQLite 缓存，例如 `["**/logs_*.sqlite"]`，默认缺省或空列表时仍完整捕获。每项只接受 runtime 根内的相对文件 glob，必须以 `.sqlite` 结束；对应 `-wal`、`-shm` 一起排除，不改变 data／secrets 根，也不能匹配 JSON 登录文件或密钥。采用 Node glob 语义，隐藏目录须在模式中显式写出；不得把登录或业务状态数据库列为缓存，依赖本机 SQLite 会话状态的旧通道须保持完整覆盖或另用一致性快照。未知 SQLite 默认保留。选择规则进入加密文件清单并与应用策略摘要交叉核验；排除文件仍计遍历预算。仅过滤根的目录分配字节数不参与指纹，目录路径、权限、入选子项和文件内容仍严格核验，新增普通文件或凭据变化照常拒绝。启用前更新源端和恢复端工具并核对覆盖范围；旧策略及旧点保持原语义，旧工具拒绝含新策略字段的点。回退移除该配置恢复全量捕获，保留新点、工具及独立密钥用于恢复，不删除缓存或中断模型请求。

可选严格布尔 `excludeModelCatalogCache` 默认关闭；缺省或 `false` 保持原策略摘要与全量覆盖。只有 root 显式设为 `true` 才排除 runtime 根的 `accounts/*/models_cache.json`，用于持续刷新导致在线捕获失败的可重建模型目录缓存；不排除其他位置的同名文件、登录 JSON、SQLite 会话状态、正文或密钥。该选择进入原加密清单及策略摘要，沿现有捕获、源重验、恢复与接收校验执行，不删除源文件、不扩大遍历预算。恢复后 CLI 需重新获取模型目录；断网时不承诺缓存命中。启用前先更新源端与恢复端工具，旧工具拒绝新字段；关闭或移除选项恢复完整捕获，新点仍须保留可识别该字段的工具恢复。

应用材料与控制清单使用独立 root-only 32字节密钥，PG 持有的 WAL key 不能解密。固定 `/etc/harbor-storage-backup.json` 可选增加以下 `applicationEncryption` 段；不配置时 DB-only 备份与保留照常可用，应用入口拒绝。`keyDigest` 为 `SHA256("harbor-periodic-application-key-v1\0" 的UTF-8字节 + 原32字节密钥)`；key文件及祖先须root控制，文件0600、单链接，不能位于任何被捕获的材料根内，也不能与WAL key内容相同。只将该可选段从原数据库配置摘要中分离，原DB字段仍完整校验；每个应用能力、清单、报告与交付独立绑定应用密钥版本及摘要，不为配置应用key强制重做大base。历史verify读取该固定备份配置，应用生产开关关闭或策略文件缺失不影响其密钥入口。

```json
"applicationEncryption": {
  "version": 1,
  "keyFile": "/var/lib/harbor-backup/application.key",
  "keyVersion": "REPLACE_WITH_APPROVED_APPLICATION_KEY_VERSION",
  "keyDigest": "REPLACE_WITH_DOMAIN_SEPARATED_KEY_DIGEST"
}
```

应用 capture/files、cycle/state、backup-complete、sealed restore/final 及 typed handoff/catalog 均为独立应用密钥保护的 v3 控制闭包；DB base、独立 point/checkpoint、普通DB恢复报告与WAL继续原归档协议。controller在关闭、取消与失败清理后清零所持两把key，应用备份key不交给PG或降权后的验证进程，不收入交付包。

启用应用时三个入口分开运行：`schedule` 按原恢复点间隔（默认5分钟）捕获材料、等待实际后续 WAL 闭包并复核源文件，发布独立不可变的 `backup-complete.enc`；它复用已认证的共享 base，不执行每点整库恢复。`maintain-base` 按原全备间隔（默认24小时）独立制作、完整认证新 base，长步骤不持活动状态控制锁；旧 active 可由另一个 context 持续产点，新 base 完整后才短锁切换。首次没有可用 active 时 schedule 明确输出 `bootstrap_not_ready`，须先成功运行维护入口。两者应使用独立进程和受控资源，不能把维护命令串入5分钟循环；模板不自动安装或启用这些任务。

`schedule` 配置可显式设置 `reuseAuthenticatedBase:true`，默认 `false`，且仅应用周期已启用、独立应用密钥已配置的源端可用。启用后维护只准备 pending 候选；完整 SHA/AEAD 认证结束、明文副本清理和检查点完成后，以 root-only 应用密钥发布源端收据。收据绑定机器、Node/执行代码、数据库及密钥配置、真实原始 verifiedAt，以及完整 immutable 文件集合的 dev/ino/uid/mode/nlink/size/mtime/ctime 和小控制摘要。producer 以小控制及全文件身份核验后短锁采用候选，不在日切或冷启动的产点主链路重读大 base。收据缺失或变化时另用独立 context 后台完整认证，旧可信 active 继续产点；active 的真实文件身份变化后立即停止使用旧 proof，未有可信替代时明确 not-ready，不伪报备份完成。关闭开关保留收据与旧备份并恢复原完整验证行为；普通 verify/restore/receiver 始终完整 SHA 与 AEAD，不使用该源端收据，收据也不进入交付包。源 postmaster 代次变化仍强制新 base，该重建空窗仍必须纳入实际 RPO，不能以收据复用绕过。

`node storage-backup.mjs verify-point <base UUID> <point UUID>` 独立选择已完成点，实际执行 PITR、原员工 Key/上游密文、四类正文密文、设备材料以及独立 Gateway/Worker/publisher 验证。它从已认证的归档策略和材料读取，不依赖当前源材料路径或应用生产开关；固定备份集群配置及工具信任仍需有效。只在真实执行与完整清理成功后封存 `application-restore.enc` 和 v3 `application-final.enc`。点的 `backupComplete:true/restoreVerified:false` 与实际验证状态严格分开；新点不能冒充已经恢复，也不授予 OCI、异机、正式容量或部署激活资格。

应用 v3 顶层状态分别记录 `latestBackup`、`lastVerified`、`pending`、`pendingVerification`；producer 与 verifier 只在短锁内推进各自字段，保留并发产生的新点及最后真实验证点。取消保留已认证的恢复材料与待续状态；未知半成品不猜删。旧的未发布共享密钥应用周期 v1/v2 保全并明确拒绝，不自动迁移、重加密或回退尝试 WAL key。回退先关闭新调度，保留 v3 密文、状态及独立应用密钥，旧清理器对未知格式须拒绝。原数据库-only及 operator pending-final 的格式和严格行为不改变。

周期 checkpoint 对同一 context 已完整验证的 WAL 保存有限内存缓存，命中仍复核密文及 metadata 的实际文件身份与认证小清单，并保留原 `verifiedAt`，不伪刷新完整认证时间。新段、身份变化、源 postmaster 代次变化或显式 scrub 重新完整验证；context 重启后缓存为空，必须重新认证。周期入口与手工 checkpoint 均要求实际 postmaster 代次与 base 一致；源重启后，即使同 timeline、当前五项恢复参数与 base 相同，也必须先维护新 base，不能用当前值推断跨重启 WAL 的历史参数高水位。同代仍核对五项恢复参数不超过 base 封存值，点内代次变化同样拒绝。已有认证旧点及材料保留，仍可按其原目标恢复，不因停止给旧 base 产新点而删除。正式规模吞吐与冷启动耗时仍需真实容量测量，不能用小库运行时间替代。

`node storage-backup.mjs periodic-handoff <base UUID> <point UUID>` 手工生成明确 `periodic-application` 类型的离源包，保存在选定点的 `handoff` 私有目录。包只含共享 base、所选点自己的 checkpoint/WAL 闭包及其应用捕获，不带其他点、operator pending-final、原 `application-final` 或激活证明；归档密钥与独立应用密钥仍须在接收端分别配置。完整包可认证续用，未知或不完整的既有目录保全并拒绝，不借用当前 base 的最新 checkpoint 覆盖所选历史点。

接收端须在原固定 root receiver profile 中显式填写 `handoffKind:"periodic-application"` 并核实包摘要、真实源机器身份、集群、密钥版本、独立工具与恢复预算，缺省仍为原 pending-final；新类型还须在固定receiver profile配置独立 `applicationEncryption`，版本／摘要与源一致，本机keyFile可不同，不能先用未认证包决定密钥或授权。新类型可通过 `storage-receiver.mjs restore` 从包和独立配置的两把密钥实际恢复所选点及原应用材料，无需原数据库、WAL 或源目录。该路径使用接收端 root 控制的已安装源码和 Node、降权后的 Gateway/Worker/publisher；不接受 candidateRuntime，也不能借用旧 pending-final／OCI／激活权限。`restore-and-attest-periodic` 使用接收端既有固定root签名key产生独立目的域的周期恢复签名，绑定选定point、包／checkpoint／backup-complete摘要、应用策略及应用key版本／摘要、真实runtime与恢复结果；签前重核已完成实际恢复的私有receipt与当前闭包。原 attest、pending-final／OCI／activation消费者对新目的明确拒绝，普通签名验证不授予部署权限；严格offhost验证还要求真实不同机器上的root接收与完整恢复；同机任务或 mount namespace 不算异机证明。保留器对完整 typed 包认证并纳入所选 base 整组闭包，控制清单变化令旧清理计划失效。

### 源端周期恢复证明复核

`node storage-backup.mjs verify-periodic-receiver <base UUID> <point UUID>` 只复核周期异机应用恢复，不启停数据库或服务。固定 root/0600 的 `/etc/harbor-storage-periodic-receiver.json` 缺失或 `enabled:false` 时不连接数据库、不读应用密钥、不新增记录；默认模板为 `periodic-receiver.example.json`。启用配置须明确批准真实接收机及独立核实的已安装源码／Node摘要和恢复UID，不能从收到的未验签payload选信任值：

```json
{
  "version": 1,
  "enabled": true,
  "receiverMachine": "REPLACE_WITH_APPROVED_RECEIVER_MACHINE_SHA256",
  "receiverRuntime": {
    "sourceDigest": "REPLACE_WITH_APPROVED_RECEIVER_SOURCE_SHA256",
    "nodeDigest": "REPLACE_WITH_APPROVED_RECEIVER_NODE_SHA256",
    "restoreUid": 65534
  }
}
```

公钥只读既有固定 `/etc/harbor-storage-receiver.pub`（root/0600、单链接），输入只读所选点目录的 `receiver-attestation.json`（root/0600、最多64KiB），不接受调用方路径、Key或机器声明。入口从真实私有backup-complete/point能力和认证handoff重建expected，绑定原数据库与应用密钥版本、策略、所选point及包／checkpoint／complete摘要，再验证独立周期签名、批准runtime和真正不同机器上的root恢复。验前、写前、返回前重核信任、输入和控制闭包；DTO复制、跨context、取消、关闭、配置变化或内容损坏均不发活资格。

成功只在所选点发布独立应用key保护的 `periodic-receiver.enc`，作用域为周期异机应用恢复；`checkpointAt` 固定指向原点，`acceptedAt` 只是源端接收时间，重入不刷新，也不改写现有四个周期状态指针。记录绑定已签恢复结果和接收机／runtime，不授予OCI候选或部署激活资格。task单独品牌入口仅产生 `periodic-receiver-diagnostic.enc`，即使签名格式有效，材料、offhost和部署资格仍为false；同机或namespace不得冒充异机。关闭配置会停止新增并撤销旧活资格，历史密文保留。保留器认证这些记录与输入摘要；尚未消费的输入、未知或损坏控制先保全拒清理。

此入口不自动跨机传输包或签名，不做增量运输，也不直接恢复激活8092。真正异机正向、自动交付／增量复用和完整恢复发布仍需分别完成；旧pending-final／OCI／activation消费者继续拒绝新目的，不能以周期签名替代原目标库角色、候选和设备证明。

### 已接收周期点的固定恢复入口

`node storage-periodic-restore.mjs verify <base UUID> <point UUID>` 在接收机以真实root运行，只选择固定 `/etc/harbor-storage-periodic-transfer.json` 对象缓存中已经认证、传输完成的周期点。它先物化独立完整包，再复用既有周期receiver实际恢复数据库、原应用材料及降权后的Gateway／Worker／publisher，成功后才用既有固定root签名key输出独立周期签名JSON。完整包占用第二份物理空间，仍受对象缓存配额约束。该命令不接收配置路径、密钥、机器声明或任意恢复命令；手动迁移入口及其 `/etc/harbor-storage-receiver.json` 不变。

独立root／0600、单链接的 `/etc/harbor-storage-periodic-restore.json` 缺失或仅配置 `{"version":1,"enabled":false}` 时返回关闭状态，在加载对象缓存和密钥之前结束。启用时沿用周期receiver的 `handoffKind:"periodic-application"`、`workDirectory`、`recoveryDirectory`、两把密钥的配置、批准source／cluster／版本、恢复UID／GID、PG工具路径及文件／容量／进程预算，增加 `enabled:true`；动态模式须省略 `bundleDirectory` 和 `handoffDigest`，这两项只能从本次认证 `received.enc` 对应的真实物化能力取得。固定配置的source、cluster、归档key版本和应用key版本／摘要必须与缓存批准值一致；不能根据收到的未认证内容选择信任或密钥，也不能配置candidateRuntime。已有带固定包路径／摘要的周期profile仍可由原周期receiver入口使用，但固定自动恢复CLI拒绝混用这种静态绑定。

一次任务的互斥覆盖物化、实际恢复、签名及清理；总超时来自周期profile的 `processTimeoutMs`。SIGINT／SIGTERM和超时沿既有取消链等待所启动恢复进程退出并清理本次私有恢复目录，再释放资源；不得把发出取消信号当作已完成清理。恢复与签名前重核固定profile、认证接收收据、包摘要和实际恢复receipt，复制DTO、关闭能力、配置变化或损坏材料都不能产生签名。失败不升级传输收据、不清理缓存或覆盖未知材料；排查后可对同一点重试。

本入口不安装或启用timer，不由收包SSH包装器启动，不扩大专用收包账号权限，也不自动把签名取回源端。签名安全交付到源端所选点的 `receiver-attestation.json` 后，仍须运行上文 `verify-periodic-receiver` 独立验签消费；同机root恢复只能证明恢复链路，不能冒充真实异机或授予部署激活资格。

### 独立的备份保留入口

保留不随 `schedule` 自动启用。`storage-backup.mjs` 的 `retention-prepare`、`retention-plan` 和 `retention-apply <计划UUID>` 只读取固定 root/0600 `/etc/harbor-storage-retention.json`；缺少配置或 `enabled:false` 时不连接数据库、不新增计划、不清理文件。模板为 `retention.example.json`：启用前必须自行填写正整数 `keepDatabaseCount`、`keepApplicationCount` 和 `minAgeMs`，没有默认保留天数。`pinnedBackupIds` 与 `operatorStateFiles` 也须显式给出；后者只列使用同一备份密钥、仍有外部引用的既有 `operator.enc`，不接收 HTTP 配置或任意命令行路径。

数据库、WAL 与原 operator 协议继续使用原归档密钥；新周期应用 v3 的控制清单和材料只用固定备份 profile 的独立 `applicationEncryption` 密钥及其版本／摘要认证。该密钥须由 root 独占持有，与 PG 归档密钥不同；存在应用材料却未配置对应应用密钥时拒绝清理，不回退尝试归档密钥。没有应用材料的 DB-only 保留不要求新增应用密钥。

先将 host 备份工具及 PG archiver 使用的工具安装为同一受控版本，并排空旧备份／恢复命令，再运行 `retention-prepare`。 新版常驻数据库 `schedule` 使用 artifact 下固定 `.schedule-coordination` 私有目录及独立生命周期锁登记；加密记录及其文件名绑定宿主 `boot_id`、PID／进程启动 tick，记录另绑定 UID、命令行摘要、代码版本、系统与配置及归档／材料作用域。仅当前进程和登记全部匹配时可通过旧进程排空检查，空闲轮询不持归档 SH，活动操作继续由实际 SH 与周期引用保护。旧未登记入口不豁免；崩溃的历史登记保全，不能认领为新进程，正常停止只移除自身未变化的记录。它只准备加密的协调意图，不改 `archive_command`、重启数据库或删除任何备份。 只有成功归档实际 24 位 WAL 段才更新协调回执；history 与 `.backup` 正常归档，保留原有效段回执。归档目录中的 `.backup-coordination.lock` 为 PG UID/0600，root 与 PG 使用同一文件 inode；锁持有期间复核目录和文件的 dev/ino、属主、模式及单链接。固定 `/usr/bin/flock` 对继承 FD 加锁，父 FD 保持锁，进程退出自动释放，不依赖容器不可见的 `/run` 或仅同 netns 有效的抽象 socket。

未准备协调时，新版 `archive-wal` 保留原归档行为，不因缺锁文件而突然停归档；原 host 备份／恢复入口则可持共享锁。准备后，必须等待新版 archiver 在同一真实锁下完成一段实际 WAL 归档并发布认证回执，才能预览或执行清理。回执绑定协调 nonce、工具闭包摘要、锁身份、实际命令摘要及 postmaster 代次；旧版 archiver 忽略新文件时不会获得该回执。源重启、命令或工具变化后必须重新准备并取得新回执。尚有旧受管工具进程或未清理的恢复目录时拒绝清理，不假设旧进程已经配合。发布回执不等于允许停止或改写服务。

数据库全备、checkpoint、应用捕获／绑定、PITR、handoff 和周期状态写入共享归档文件锁；清理提交使用排他锁，并保留每个备份已有的 checkpoint／绑定互斥。共享锁允许 checkpoint 等待 WAL 时 archiver 继续工作。冲突直接返回 busy，不延长原操作期限。大文件认证使用共享访问；排他区间只重新核对小引用和已见证身份、将确定候选移交到 quarantine，不在该区间解密整个备份。

`retention-plan` 对当前 artifact 根及原有 `pending-<runUUID>` 子组逐组认证数据库文件、WAL 清单和配套应用材料，生成加密分页计划，不修改原备份。分别保留最新数据库组、最新应用备份完成组及最小期限内的组；应用组按组内最新认证点的 checkpoint 时间排序，最小期限从 base 创建与最新 `backup-complete.completedAt` 的较新时间计算。周期 v3 的 `backup-complete.enc` 只表示应用材料与后续 checkpoint 已完整绑定，不表示实际恢复通过；最后真实 `restore-verified` 点所在组另行强制保护，新产生但尚未演练的点不能挤掉它。删除单位始终是整个 base 组，组内所有 point、capture 与 WAL 闭包一并保护，不单独剪除仍在保留组中的点。

数据库周期 active/pending、应用周期 v3 的 latestBackup/lastVerified/pending/pendingVerification 四引用、pins 和已登记的外部 operator 引用均受保护。已认证但未闭合的应用 reservation 保留整组；只有明确 failed 且其残留字节全部可认证的闭合失败才不永久 pin。完整点与顶层 latestBackup/lastVerified 的 point ID、checkpoint 和完成／验证时间逐项匹配，不接受指向缺失或未验证点的状态。包内原有 `application.enc`、`operator-final.enc` 和 handoff 摘要仍是组内引用，符合保留条件的旧应用组可整组清理；仅数据库 `restore.enc` 不被误认为永久外部 pin。外置独立密钥包仍由操作者显式 pin 对应数据库备份，工具不导出或尝试其他密钥。未知格式、损坏认证、未知子文件或无法界定的引用继续拒绝清理；旧应用周期 v1/v2 的顶层状态或材料不当作 v3 协议读取，保全并拒绝清理，不自动迁移。精确认证的空 staging/points 目录也必须有明确允许项。

共享锁下完整认证应用点、捕获文件集、独立恢复证据和每个点自己的 WAL catalog；排他移交前只重读 cycles、capture/files、point/checkpoint、backup-complete、application-final、application-restore 与普通 restore 的小控制引用及其名称集合，不在排他区间重读 GiB 材料。控制引用变化令计划过时；checkpoint/files manifest 绑定 catalog 及材料摘要，实际字节仍需先通过共享锁下的全认证。journal 重入使用同一 v3 认证与双闭包，仍只处理原计划删除集合。

WAL 依据所有保留恢复点的连续区间、保留 base 之后的已归档连续尾部及认证 timeline/history 保护，不使用 mtime 或字符串排序判断过期。缺段、未知 timeline、替换或错配的密文／metadata 对均拒绝清理；history 和备份历史文件保守保留。计划中的配置、引用、源代次和候选身份在执行前重新验证，计划过时不会自动扩大删除集合。

规划先同步 `.retention` 与计划目录的父链，并发布加密 `plan-preparation.enc`，之后才分配归档 quarantine、同步父目录、记录 inode 和发布最终计划。计划发布前中断时，下一次 `retention-plan` 在每计划独占锁下续发布原准备记录；只有认证 reservation 指定的私有空分配目录可在尚无 inode 见证时完成绑定，非空、异主或已见证身份变化均拒绝，不删除未知内容。最终计划仍须在 apply 时重新认证，不因恢复发布而获得执行资格。协调回执引用的 WAL 也属于保留闭包；共享锁下完整认证回执及其 WAL/history，并见证认证前后一致的文件身份，共享锁内再核目录配对及见证前后的目录时戳，EX 提交时只复核小回执与原 FD/路径身份、目录时戳，回执或目录变化则拒绝并重新认证；孤立密文或 metadata 均按不完整归档对拒绝。

`retention-apply` 先持久化认证 prepared journal，先同步数据库 quarantine 的父目录，再按计划将完整备份组及无需 WAL 对原子移入其所在文件系统的私有 quarantine，按顺序同步父目录；全部移交后才重新认证并逐文件删除。每次 unlink 前落盘意图，进度使用有界游标。unlink 已不存在时也先同步其父目录再推进；深到浅删除目录时，以计划内已见证且仍存在的最近祖先补同步，最终归档 quarantine 已移除的重入仍同步归档父目录后才完成。每次未完成 journal 重入先在 SH 下重新认证当前保留组和必需 WAL/history 闭包；只按本 journal 的精确移交位置、已提交删除游标及 unlink 意图容许原候选的缺失，不扩大清理集合。中断重入只处理原计划已见证的源／quarantine 身份，不接管同名新文件。计划执行未结束时拒绝创建另一个清理计划，避免遗漏已移交组的 WAL 引用。恢复进行中、源代次或引用变化、身份不明时保留现状并报错，不自动删除未知目录。完成回执仍为 `applicationMaterialsVerified/restoreVerified/offhostVerified/deploymentAllowed=false`；真实恢复和发布资格沿原入口单独核验。

配置关闭只禁止后续清理，不删除协调意图、锁、计划或 journal，也不撤销已完成的文件删除。需要回退工具版本时先关闭清理并核清所有在途操作；未结束的 journal 应由原受控版本认证续行，不能删状态或直接 `rm -rf` 绕过失败。所有计划／执行均有实际磁盘读取成本，启用前单独核存储余量、I/O 影响和原服务占用，不把保留数量当作容量或 RPO/RTO 承诺。

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

### 保留冷集群的显式版本换绑

换绑的写挂载检查复用下述 `rotate-tools.py` 的完整本机库存及持资源状态判定，覆盖暂停、重启、移除中和 dead 状态；只读挂载及已确认无资源的停止实例保留原行为，任何未知库存或状态均拒绝，不依赖仅 running 的列表查询。

已存在的 PGDATA、归档密钥和旧迁移 pending 不使用 `prepare` 重建。独立入口 `python3 ci/storage-recovery/rebind-bootstrap.py check|apply|rollback` 仅消费固定 root/0600、单链接的 `/etc/harbor-postgres-rebind.json`。批准版本1包含随机 UUID `id`、`expected`（`bootstrapSha256/stateSha256/publisherSha256/owner/cluster`）、`source`（`containerId/startedAt/dataSource/runtimeSource`）和完整 `nextBootstrap`。SHA 均取原文件字节；下一配置只能改变业务 B、与其绑定的基础设施 I、真实来源镜像 ID，并将 `publisherDatabase` 清为 null，端口与两个 origin 保持。旧 operator 已绑定的 publisher 允许与 bootstrap 的空值不同，但其完整旧字节也必须明确批准。

仅在原 infra 正向确认缺失或已停止、owner/旧指纹/精确来源代次与目录均匹配、无运行容器可写挂载受保护材料、冷 PG18 的真实 system_identifier 与原归档配置一致时允许修改。入口持原三个控制文件 inode 锁，在 `postgres-infra/rebind/<UUID>/journal.json` 保存原字节和新字节，先持久化写意图，再原 inode 写入并 fsync；重复 `apply` 续跑，`rollback` 同样支持中断续跑并恢复原字节。原 key/TLS/PG 控制材料身份与摘要发生变化、未知 inode、已完成文件被外部修改或批准文件变化时均保全拒绝。日志与快照只留 root 私有目录，不输出密钥；回滚后同一批准不能再次 apply，应另签新批准。操作期间不可并发启动 infra、重启来源或修改控制文件。

该入口不启动/删除容器，不改 PGDATA、TLS、归档、旧 pending，也不迁移或签发激活。先准备真实 CI 的 B/I 与受信 host helper；Node/app 安装不依赖 bootstrap，可先完成，原 `install-tools.py` 对非空工具树仍拒绝覆盖，升级前应排空引用原路径的任务并原样保留旧工具树。宿主 PG 工具仍需单独安装。完成换绑后按原入口启动同一集群，实际重做本代 baseline、异机恢复和新的独立迁移 run；旧 baseline/target 不能代替最新业务捕获，原 SQLite 锁及 activation 门禁保持。

`ci/verify-source-recovery.mjs` 是独立 host 源码入口，安装到 `/usr/local/lib/harbor/verify-source-recovery.mjs` 后只由受控恢复器在持部署和 controller 锁时调用。它从固定 root operator 配置和已安装 B 模块读取实际状态，不接收“可恢复”参数；复用正式 resume inventory、签名 registry、精确角色 OID、SQLite lease 与源文件验证。未分配/未创建 runtime、部分导入及 promotion 已提交但尚未 consume 均须逐项正向证明；目标文件缺失本身不构成许可。完整旧捕获存在时匹配原数据/材料，否则在新私有目录调用原 capture 校验当前被隔离的权威源，输出明确的验证模式，不伪称历史快照匹配。仅撤销本次已认证角色/私有 lease，外来会话保留并拒绝恢复；任何实际 activation/deployment 消费拒绝。返回前释放源和 DB lease，恢复器继续持互斥锁；结果不是持久 capability，也不恢复网络/启动源或删除 PG。task-owned 真 PG 组件测试只输出 diagnostic 合同，不能作为 formal 恢复凭据。host 脚本须按独立批准 commit/SHA 交付，不为此重建固定业务镜像。

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

`rotate-tools.py prepare|accept|rollback` 提供既有非空工具树的独立可逆轮换，不改原安装算法。只消费固定 root/0600 `/etc/harbor-storage-tools-rotation.json`：版本1、UUID `id`、`expected.profileSha256/receiptSha256/treeIdentity`（原目录 dev/ino）和原工具配置格式的 `nextTools`。先停止定时调用并核实无旧 host 任务；工具再核真实进程 exe/cwd/maps/fd、Node/Python 的独立脚本入口，以及运行容器的工具树挂载，不对 `python -c` 源码文本或搜索词做子串判定。Docker 库存固定连接本机 root 控制的 `/run/docker.sock`，由真实 `docker info` 取得可信 data-root；磁盘仅提供 root 所有的 hex ID 目录集合，再逐 ID 调用真实 inspect 取状态及挂载，目录集合/inode、父目录及 socket 前后须一致，不读取磁盘 State 代替 API，也不以失败/超时当不存在。paused/restarting/removing/dead 保守检查工具挂载，完整停止且无资源者不扩大阻断。共享原 Node 安装锁，并持旧/新树原 PG 安装锁；观察不完整或存在使用者均拒绝，不停止进程。

`prepare` 将原树原子 no-replace 移至 `/opt/harbor-storage-tools-history/<UUID>/previous`，在私有 journal 保存原 profile 字节并同 inode 写新 profile；原 installer 随后仍只向缺失 canonical 安装，PG 工具继续单独安装。`accept` 实际核新 receipt、源码 manifest 和 Node，再登记新树身份。`rollback` 保留新版整树为同目录 `replaced`，恢复原 profile 字节及原树 inode；每次移名和配置写入先存意图、同步目录/文件，重复原命令可续跑。未知目录、外来 canonical、原 profile 被替换或修改、批准变化均保全拒绝；不删除任何版本，不要求重新下载新镜像才能回退，也不改原服务和 activation。轮换 helper 必须从工具树之外的受信 host artifact 执行，不能以本机测试冒充服务器已完成轮换。

新空集群的固定initdb参数为`--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8`，与现有恢复／容量私有PG入口一致；CI smoke实际核`server_encoding=UTF8`及`datcollate/datctype=C`。不依赖官方镜像LANG或宿主glibc语言排序版本，也不会给已有库修改collation。

### 首次连续归档的安全准备

bootstrap在新infra ROOT预建`archive/`(PG999/0700)、独立`archive.key`(PG999/0600)及固定`/etc/harbor-storage-archive.json`(root0644)占位文件，Root state记录其原始dev/inode。archive目录、key在宿主和PG容器内同绝对路径，目录RW、key只读；固定profile单文件同名只读挂载。普通PG入口从首次启动即配置既有B代码`storage-backup.mjs archive-wal %f`和archive_timeout=60。占位expectedCluster为空时原归档程序明确失败，PG保留WAL；不能把容器可启动或archive_mode=on当作归档成功。

`python3 ci/storage-recovery/prepare-archive.py`是固定root、无参数的准备／复核入口。它核已批准infra镜像、owner标签及实际挂载，通过容器PG999的固定回环TLS连接，以挂载的独立管理员密码及CA完成证书／主机名验证，并核pg_stat_ssl后取得真实system_identifier，与宿主PG_VERSION／pg_control核对；仅在一致时同inode写原占位profile并fsync。采用该inode的独占锁和root0600小journal；journal与备份key先完整写入同目录临时文件并fsync，再以renameat2 no-replace发布，读取恢复journal后先同步其目录，读回profile后无条件fsync再移除journal。控制器中断导致部分写入时，下一次仍须重新核实际PG身份，才能按同一已准备绑定完成；未知内容、错误inode／key、非root journal不覆盖。profile绝不原子换文件，避免已挂载的旧inode长期不更新。只独占生成／核验相同归档key的root0600`/etc/harbor-storage-backup.key`副本，不覆盖已有不同key，不复用report或源加密主密钥。

返回明确区分`profileBound`、`metadataReady`、`walArchiveVerified`。新helper不生成`cluster.enc`，完整backup/operator inventory/profile及宿主工具由Root准备后，显式运行原有`storage-backup.mjs prepare-archive`初始化同一config digest／keyVersion／UID的认证绑定。之后须实际产生WAL，原archiver真正成功，且现有`inspectArchivedWal`验证对应加密segment、认证摘要和cluster绑定后，才可能ready；始终`deploymentAllowed:false`，不代替部署或正式恢复验收。占位、metadata未齐和WAL未验均只报下一步缺口，归档失败时保留WAL，需监控空间。

本机UID0测试仅使用新/tmp私有目录核root/PG999权限、同inode、并发锁及中断恢复，不安装真实/etc或/opt；私有PG测试只验证真实WAL与文件完整性组件，不升级为已安装root profile或实际Docker/173组合通过。


### 后台总览查询索引

`request-overview-index.sql` 是可选的在线性能索引，不改变 schema migration 版本或业务数据。由管理员针对已核对的目标数据库，以同一连接、自动提交模式依次执行三个 `-- statement-boundary` 分隔的语句；不能塞进业务 migration 事务。先核对同名索引的表、字段及有效性，再并发创建 `(usage_at) INCLUDE(status)`，最后重验定义。连接局部设置有界 `lock_timeout` 和 `statement_timeout`；失败保留现场，不自动删除未知或无效同名索引。该索引让近7天每日计数只读取窄索引，查询本身仍能在未安装索引时正确运行。回滚查询代码无需删索引；确需撤销时，在确认无其他维护占用后单独执行 `DROP INDEX CONCURRENTLY harbor.request_overview_cover`，不删除请求记录。新的安装或逻辑恢复后如需同等性能，应重新执行本脚本；物理备份保留该索引。实际收益须以目标环境的执行时间、Index Only Scan 和 Heap Fetches 核对，不以索引存在冒充性能通过。

`request-statistics-indexes.sql` 同样只通过管理员自动提交逐段执行，提供员工／会话聚合与默认类型聚合的两份窄覆盖索引。它们只覆盖各自需要的身份、时间、状态及价格版本，不包含正文、错误、客户端信息或上游返回的模型字符串，避免无界模型字段触发 B-tree 元组长度限制。类型查询按当前分组只投影必要身份，跨维度筛选仍按原字段执行；带额外筛选时允许优化器选择其他索引或堆扫描。两份索引前后都核对精确字段顺序与有效性，同名冲突拒绝，旧业务代码和数据库版本不变。保留以目标环境真实执行计划与耗时为依据；只在确认不再需要时分别并发删除该脚本声明的索引，不处理其他索引或请求记录。
