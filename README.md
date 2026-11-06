# Harbor · 本地 AI 网关

Harbor 提供员工 Key、订阅账号池、Responses 转发、用量查询与管理页面。文件、终端、工具权限和上下文策略由客户端控制；服务器凭据不交给员工客户端。

## 本地运行

需要 Node.js 24.21 或更新版本、npm 与适用的官方 CLI。先执行 `npm ci`，再执行 `npm start`；Windows 可使用仓库的启动／停止入口。默认只监听本机，首次运行生成管理密码找回文件。按 [客户端说明](clients/README.md)配置员工 Key；远程地址由部署者自行提供。

## 数据与密钥

`data/`、`runtime/`、管理密码、员工 Key 和上游授权材料不能提交或分享。Key 与采集正文分别使用独立主密钥；数据库、WAL、文件材料及对应密钥需要配套备份。恢复前核验身份与完整性，禁止用旧 SQLite 覆盖 PostgreSQL 切换后的新写入。

## 规模化使用与 PostgreSQL

新增 PostgreSQL 网关和独立正文 Worker、数据库分页及统计查询、员工使用类型与模型／限额模板、套餐容量管理、按北京时间五个时点平滑交接，以及设备登记和 mTLS 绑定。配置默认值、拒绝边界与具体能力以源码和测试为准；不能由组件测试推导约 60 人正式容量或生产发布资格。

`npm run start:postgres`、`npm run worker:postgres` 与 `npm run db:migrate` 使用显式 PostgreSQL 配置；参考 [存储恢复说明](ci/storage-recovery/README.md)与 [PG 配置示例](ci/postgres-env.example)。迁移／恢复工具只接受受控来源和固定批准材料，不会自动安装主机配置或发起服务切换。设备访问参考 [设备辅助程序](clients/harbor-device/README.md)。

CLI在数据库所有权丢失后停止接纳、完成必要持久化并退出，由部署者的进程监督器重建连接；不会自动重发结果不明的模型请求。嵌入式调用保留调用方控制关闭的行为。

正文采集记录客户端实际上传的本轮内容及工具交互，不把整段历史重复记为本轮，也不声称记录客户端未上传的操作。部署者应按实际需求配置采集、访问控制和保留周期。

## 管理登录配置

同一主机并行运行多个实例时，可在各自 `data/settings.json` 显式设置不同的 `adminCookieName`（例如 `harbor_session_preview`），避免浏览器在不同端口覆盖管理登录。名称须为1–64位ASCII字符、字母开头，仅含字母、数字、下划线或连字符；空值、非法类型及其他字符均拒绝启动，PG预检也校验。未设置时沿用 `harbor_session`；名称在启动时固定，不从请求Host或转发头派生。变更后重启并重新登录；只读取和注销当前名称，不回退读取或清除其他名称的Cookie。删除此字段或设回 `harbor_session` 即恢复默认，但同主机不同端口再次共用该名称。配置不修改管理密码、员工Key或Cookie原有属性，也不提供端口间安全隔离；各实例分别维护自己的配置。

## 容量实验资源

容量运行可显式提供 `--resource-limits-file <JSON文件>`，通过现有 `--owner-entry` 和 `--delegated-cgroup` 路径交给 native owner；声明资源但未提供委派时拒绝，不把声明当实测。省略配置仍为网关／Worker共享4 vCPU／8 GiB、数据库独立4 vCPU／8 GiB。配置只包含 `gatewayWorker`、`database` 两角色的 `cpuQuota` 与 `memoryLimitBytes`，可附完整 `processLimits.gateway/worker` 及可选 `processLimits.database`；未附时叶子继承所属角色上限，并继续受共享组约束。CPU须能按100000微秒周期精确表示且不低于内核1000微秒最小quota，内存为正安全整数字节。非法值、缺字段、未知字段及超过角色的叶子限额在创建资源前拒绝。

`run/seed/verify` 的 `--disk-budget-file <JSON文件>` 沿相同执行链传入显式磁盘计划，固定包含 `dataBytes`、`metadataBytes`、`indexBytes`、`walBytes`、`backupBytes`、`temporaryBytes`、`growthBytes` 七项正安全整数字节；未知字段、缺项、零值或非法类型在导入适配器／分配资源前拒绝。实际formal执行仍按 `--storage-root` 核选定目录的真实可用空间、完整正文源大小及各项合计，不把计划当作全量实测。WSL等稀疏虚拟磁盘还须核实际宿主卷空间，不能只根据虚拟文件系统的逻辑余量开始全量造数。省略该参数不生成虚构预算，formal仍因缺证拒绝；既有selfcheck默认不变。

formal `run` 通过CLI和native owner时自动接通原有物理恢复阶段，不能显式关闭；`seed/verify` 及selfcheck默认不启用恢复。它让同factory的原Key、TLS、正文与设备恢复评估可达，不会自动安装runtime trust、批准拓扑或把其他运行的恢复收据当成本轮资格。正式数据量和性能门槛保持不变，其他前置缺失仍在造数前拒绝。

例如，采用不同进程限额复现实验，可使用以下声明（文件由执行者明确选择，不来自客户端请求）：

```json
{
  "gatewayWorker": {"cpuQuota": 1.5, "memoryLimitBytes": 2684354560},
  "database": {"cpuQuota": 1, "memoryLimitBytes": 2147483648},
  "processLimits": {
    "gateway": {"cpuQuota": 1, "memoryLimitBytes": 2147483648},
    "worker": {"cpuQuota": 0.5, "memoryLimitBytes": 536870912},
    "database": {"cpuQuota": 1, "memoryLimitBytes": 2147483648}
  }
}
```

声明随报告保留；allocator只在自身委派后代写入角色、共享总额和叶子限额，启动、观测及恢复后的每个epoch均重核实际值、PID归属、祖先、CPU亲和性及组合可用配额。不改变宿主委派或既有服务。配置变化必须新建一次实验，不能中途放宽报告口径。`preflight --manifest`可在原清单中提供同名 `resourceLimits`，但普通JSON或配置本身不能替代真实cgroup证据。五分钟CPU／内存80%与所有接口、并发、时长、计价、126万／540万元数据及正文覆盖标准保持原值；较低配额通过与否仍取决于实际结果。此配置不会把CLI连接到既有数据库，也不会补齐LAN、代理或不可变运行证明。

## 远程配置

远程监听须显式配置 `HARBOR_LISTEN_HOST`、`HARBOR_PUBLIC_ORIGINS` 与 `HARBOR_PORT`，并自行提供 HTTPS、持久化目录和备份。示例为占位地址，不含现成主机、账号或部署凭据。本公开投影不包含公司 GitLab 发布流水线；保留的受控部署 Python 模块是存储预检与源码清单的通用依赖，只有管理员自行安装和批准后才可能使用。

## 开发验证

`npm test` 运行本地单元与组件检查。真实 PostgreSQL、Windows、namespace、root 或外部材料测试须显式提供隔离条件；缺条件会清楚跳过，跳过不等于通过。公司 CI 编排专属测试未包含在公开投影，通用业务测试与其断言保留。使用合成数据与私有临时资源，不访问真实订阅或客户内容。

设备、迁移、备份恢复及容量工具的局部实现不代表完整异机恢复、正式容量或实际部署已经验收。使用前核对自身环境和受控配置，兼容回退须保留 Key、设备与数据安全门禁。
