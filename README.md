# Harbor · 本地 AI 网关

同库发布在暂停入口前预检旧网关与Worker；相同镜像ID、引用及关闭参数只检查一次。静态证明仅在本次受锁进程内使用，消费前重核源容器ID、镜像ID／引用、参数及受保护配置、证书和密钥文件身份；并在旧容器内重新执行完整预检，校验最新签名存储报告、空间、数据库、凭据和设备状态，不缓存动态就绪结论。容器内校验和宿主调用均有期限，材料以非阻塞方式打开并拒绝非普通文件；数据库连接不同则回退到原隔离预检，连接凭据不进入命令参数。停源前拒绝与停止结果未知分开处理，前者核验原组合后按原恢复路径释放入口；任一漂移不据旧证明停容器。普通首次部署仍执行原现场检查，回退保留既有关闭确认、持久采集许可及入口恢复规则。撤回本段对应代码可恢复原每成员现场预检，不改变数据库或配置格式。该优化只缩短暂停窗口，不能据此承诺发布零等待。

## 服务器部署（GitLab CI/CD）

当前服务入口为 `http://173.208.167.168:8092/`，所有分支共用项目变量 `GATEWAY_DEPLOY_HOST` 指定的部署主机，不区分主分支。流水线先运行测试，再构建镜像并执行候选验证；保留部署作业名供既有入口使用，部署作业手动触发。本机默认端口仍为 `43127`。CI 的 `SERVICE_ORIGIN` 只用于环境链接，不决定 SSH 目标；实际监听与允许的 Origin 由服务器 root 配置决定。`gateway-93` 环境名及 `harbor-gateway-93` 串行锁保留为既有标识，不代表部署目标仍是旧主机。

8092 的 `preserve-current` 策略禁止原先停再建入口。受控 PostgreSQL 切换使用 `deploy-entry-postgres`，必须通过固定 root controller 的入口排空、真实来源围栏、候选身份、材料及 activation 核验，候选健康后才切换；首次分发至 PostgreSQL 后不自动恢复 SQLite。固定 root/0600 的 `/etc/harbor-gateway-deploy.json` 可设严格布尔 `entry_installed_release:true`，仅让显式新 action 使用已安装的批准镜像，不接受附加镜像字节，仍执行全部发布门禁；缺省或关闭时沿原运输方式，客户端不能开启。此模式配合专用 CI 手动作业，避免切换窗口重复运输镜像，不等于允许使用任意本地镜像。关闭该值恢复原运输路径，既有数据及镜像不删除。

复用其他工程的 `aistrong` Runner，继承非秘密配置 `HARBOR_REGISTRY_HOST`。凭据使用本项目独立的 masked CI 变量 `GATEWAY_REGISTRY_USERNAME`、`GATEWAY_REGISTRY_PASSWORD`、`GATEWAY_DEPLOY_KEY_B64`；目标及可信主机公钥使用项目变量 `GATEWAY_DEPLOY_HOST`、`GATEWAY_DEPLOY_KNOWN_HOSTS`。缺失时作业失败，不回退到共享凭据。镜像放在独立的 Harbor 私有项目 `harbor-local-gateway`，构建机器人只有该项目的 pull/push 权限，机器人有效期为 90 天。作用域依据 [Harbor 项目机器人文档](https://goharbor.io/docs/2.13.0/working-with-projects/project-configuration/create-robot-accounts/)。

`test_postgres` 在安装依赖和运行大数据测试前，只读检查作业目录及临时目录的可用磁盘。默认下限为2GiB，可由 CI 变量 `HARBOR_TEST_PG_MIN_FREE_BYTES` 配置为正整数字节数；依据是独立十万请求诊断约1.06GiB的实际集群占用，并保留临时文件及WAL增长空间。低于预算或观察失败时停止本次测试，不清理或自动重试共享资源。该检查只覆盖测试作业所在文件系统，不证明独立 PostgreSQL 服务卷、并行作业或正式容量环境的空间充足；GitLab 服务容器可能已在脚本前启动。

Node、Python 校验镜像及服务的 Node 基础镜像使用 Docker 官方 ECR 公共同步仓库并锁定摘要，多架构索引已与 Docker Hub 官方源核对一致。Kaniko 使用本项目 `kaniko-executor` 依赖仓库中的官方 `v1.23.2-debug` 完整镜像，索引及各层摘要与 GCR 官方源一致，内容与既有成功构建核对一致；该依赖须在构建前完成缓存。构建作业的 `DOCKER_AUTH_CONFIG` 仅由现有项目仓库变量展开，供 Runner 拉取构建器，不新增凭据。测试命令、构建工具版本和部署入口不变，更新依赖镜像时须重新核验来源、缓存和摘要。

两个镜像构建由`ci/image-digest-build.sh`调用Kaniko，成功后记录真实digest，产物绑定当前项目、流水线、提交、角色、仓库及生产作业。PG验证服务镜像以本次业务镜像的`repo@sha256`为基座，并记录父digest。`prepare_postgres_candidate`仅消费当前依赖作业的唯一摘要文件，生成受控子流水线，将候选job及PG服务的镜像引用固定为digest字面量；缺失、错源或摘要非法立即失败，不回退标签。父`verify_postgres_candidate`以`strategy: depend`等待子流水线的原候选检查，部署继续依赖该门禁；部署作业、参数和归档校验保持原流程。

Kaniko进程局部向既有`GODEBUG`追加`http2client=0`，用于规避基座镜像层传输中已观察到的HTTP/2流错误；不改变构建源、摘要、原TLS参数或增加重试，也不影响模型服务。真实构建才能验证该传输调整的效果。回退需同时恢复CI配置与配套摘要脚本，无数据库迁移；候选digest链不等同于正式容量环境的只读运行、资源隔离或LAN证明。

构建缓存分别保存在本项目业务、验证、基础设施镜像的 `-build-cache` 仓库，沿用原项目机器人权限；不添加外部缓存源。Kaniko复用RUN／COPY层并移除层时间戳，提交revision只在稳定依赖之后进入镜像清单与label，避免仅源码变化也重装Codex／Qoder。CI参数校验不能代替真实缓存证明；须比较同依赖、不同源码的两次实际镜像层摘要，依赖版本或锁文件变化仍重新构建相应层。回退构建脚本及Dockerfile即可关闭缓存，已发布镜像与数据库不变。

### CI直接准备镜像

启用CI变量 `HARBOR_IMAGE_STAGE=1` 后，`prepare_gateway_image_testing` 在真实业务候选成功后手动执行：Runner从现有仓库按digest读取镜像，经原受限SSH直接发送到173，不经过开发机，也不向173发送仓库密码。启用时隐藏原普通部署作业；真正切换仍使用原 `deploy_gateway_entry_installed_testing`、原同库票据与入口控制器。需更新备份工具时，另启用原 `HARBOR_POSTGRES_INFRA_SMOKE=1`，通过实际smoke后执行 `prepare_postgres_tools_image_testing`；普通业务发布不默认更新工具profile或触发备份。

服务器先由管理员安装受信的 `ci/release-image.py` 至 `/usr/local/lib/harbor/release-image.py`，并更新原forced wrapper。原root配置 `/etc/harbor-gateway-deploy.json` 的严格布尔 `image_staging_enabled` 默认关闭，只有显式 `true` 才允许本项目SHA镜像准备；CI不能覆盖该开关。`prepare-image` 仅校验、导入，不运行镜像，不暂停入口、不改票据或业务容器；`image-stage-status` 在导入任务持原部署锁时仍可只读查询。`image-stage-plan` 只接受有界OCI元数据，从最多三份历史complete收据及其整包SHA／长度均匹配的归档计算缺层；Runner单流发送元数据与缺层，服务器组装完整原OCI并重核闭包。首次无可复用包时传全包，缓存命中后只传缺失层；不把镜像存在或未知目录当可复用来源。

准备包在 `/var/lib/harbor-image-staging/<身份摘要>/`，只由root写入。归档成员、压缩及展开字节、平台、源码revision、原manifest／config／各层摘要均校验；只支持当前gzip／普通tar层，不降级转换OCI身份。传输失败清理本次未完成目录；导入或结果未知保留现场。导入在独立systemd任务中执行，SSH观察断开后先查询同一身份，不重启load；只有实际load退出0且原RepoDigests／Descriptor／config／layers一致才记录complete。CI将准备、传输字节、导入状态和完成结果写入作业日志，完成前不会显示成功；收据始终 `ciVerified:false`、`deploymentAllowed:false`，自报流水线信息仅用于审计，不授发布权限。

原宿主四文件manifest和固定票据会绑定wrapper字节。更新前使用root-only `ci/install-image-stage.py install`，传入已核对的source、revision及旧manifest／票据、新wrapper／helper的SHA；它在原部署与controller锁内核当前已完成release及真实active启动授权，保全旧文件，安装后只同步原manifest与固定票据的hostToolsRevision，重新验证源码和原active授权。历史release记录与active approval不改写，待处理release或未知字节拒绝安装。失败恢复旧文件；`rollback --revision <已安装revision>` 仅在当前active身份未变且文件仍为已记录前／后件时恢复。该管理工具不属于CI forced动作，不修改开关或备份工具profile，也不重启服务。移除CI变量及关闭root开关停止后续准备；已有导入按原预算收尾，原installed-release消费方式保留。

遇到公共Node基座限流且内网未命中时，可显式设置CI变量`HARBOR_CACHE_NODE_BASE=1`，再手动执行独立`cache_node_base`；默认不创建该作业，不成为测试或部署依赖。`HARBOR_NODE_CACHE_SOURCE`默认`dockerhub`，仅允许显式改为`ecr`，无自动回退。它复用原缓存脚本的固定`node`枚举，复制与Dockerfile相同摘要的24.21.0基座到既有同项目registry-map路径，保留全部平台和原始摘要，核复制前后index字节一致后生成`.ci-node-base-cache/流水线-作业.json`；不改变Dockerfile、镜像版本或候选门禁。源为官方匿名TLS，目标沿用现有项目凭据；关闭变量只撤销后续手动入口，已上传缓存保留，不删除镜像或影响服务。实际缓存成功及后续构建仍须分别核验，不能把本地脚本测试当远端构建已修复。

`test_gateway`、`test_postgres` 通过 `ci/install-debian-test-dependencies.mjs` 安装测试依赖。固定 Node slim 镜像没有系统 CA，脚本先将 Node 内置的 Mozilla 根证书写入临时公开 PEM（目录0755、文件0644，供 `_apt` 读取），仅将 `debian.sources` 中官方 `deb.debian.org` 的活动 URI 升级为 HTTPS。首次 APT 更新和安装显式使用该 CA 文件，并安装 `ca-certificates`；发行版、组件和 `Signed-By` 保持原值，TLS 证书／主机名及仓库签名校验继续启用。成功、失败或受控中止后清理临时 CA，不自动重试或延长作业期限。该引导只作用于两个测试作业；回退对应 CI 改动即可撤销。

`test_gateway` 同时安装 GCC、libc 开发文件与 Python 3，以编译并校验受限 namespace 原生入口；Python 用于固定解包工具的身份绑定，即使只测试拒绝非法入口参数也必须存在。该依赖仅用于 CI 测试，不改变业务镜像依赖或部署方式。

`test_gateway` 在隔离的 Linux root 测试容器中显式开启 `HARBOR_RECEIVER_ROOT_TEST=1`，验证接收端的跨 UID 文件移交、链接拒绝、publisher 加载边界及 pidfd 信号保护；不开启该标记的普通本机回归跳过这些专项。测试只使用容器内私有合成文件与进程，不安装宿主配置；通过只证明相应容器 root 组件，不代表完整 root 接收端、实际 PG publisher、业务镜像、异机或部署资格。变量只作用于该测试作业，移除它即可关闭专项，不改变服务配置。

测试作业可显式设置 `HARBOR_TEST_DEBIAN_MIRROR_ORIGIN` 选择 Debian 镜像站。默认未设置或空值时继续使用官方源；非空值只接受无凭据、查询、片段和路径的完整 HTTPS origin，并保留 `/debian`、`/debian-security`、发行版、组件和原 `Signed-By`。重复使用同一镜像源保持幂等；未知原源或非法配置直接拒绝，不回退其他站点、不关闭 TLS 或 APT 签名校验。变量只影响测试容器安装依赖，不影响网关运行和模型请求；不设置该变量即可关闭，移除变量后须用新作业容器恢复官方源。镜像站的实际可用性、索引新鲜度和提速效果以该 Runner 的真实 APT 作业为准。

业务镜像构建复用同一 HTTPS 安装器安装原有的系统证书和 OpenSSL。Kaniko 仅对业务镜像将上述可选变量作为 `HARBOR_BUILD_DEBIAN_MIRROR_ORIGIN` 构建参数传入；未设置时仍使用官方源，非法值由安装器拒绝，PG验证镜像只继承本次业务镜像的固定digest，不重复传入镜像源。构建后移除临时安装器与APT索引，不新增运行时开关或改变默认服务入口；原FROM、CLI版本、依赖包、CI摘要链及部署步骤保持。回退须一起恢复Dockerfile与构建脚本；真实构建、最终候选镜像及模型协议组合仍以对应提交的CI结果为准。

PG18.6基础镜像可用默认关闭的 `HARBOR_CACHE_POSTGRES_BASE=1` 显式显示手动作业 `cache_postgres_base`。它仅将与官方Docker Hub／ECR逐字节核对的固定多架构索引复制至本项目依赖仓库，以完整摘要组成独立标签，不覆盖通用版本标签；Skopeo保留全部平台及摘要，复制后重新核对原始索引才发布作业收据。`HARBOR_POSTGRES_CACHE_SOURCE` 未设或为空时保持 `dockerhub`，也可显式选择 `ecr`；只映射两个固定官方源与同一摘要，不接收URL、不自动回退。source inspect失败只输出timeout／dns／tls／rate_limit／other固定类别与阶段码，不输出原stderr或匿名token。源匿名HTTPS读取，目标沿现有内网registry及项目robot权限，临时认证文件0700目录／0600文件并在退出时清理；不输出凭据、不自动重试。测试PG服务和验证镜像固定同一官方索引摘要，Kaniko沿既有registry map读取；该作业允许失败，不成为既有部署门禁依赖。关闭变量只移除后续手动入口，不删除已缓存材料；回退同时恢复CI、Dockerfile与脚本。实际Runner复制、权限及缓存消费仍须真实验收，脚本测试不代表预热完成。

容量只读运行原语位于 `tools/capacity/runtime-artifact.mjs` 与 `runtime-entry.mjs`。正式业务身份只接受root控制的固定 `/etc/harbor-capacity/runtime.json`，绑定实际OCI业务镜像、CI生产作业及成功父子候选链；工作区、验证镜像或自报JSON不能取得该身份。解包逐层核对压缩摘要、diff_id、whiteout及链接语义；原生启动器使用实际user/mount namespace、独立inode复制、只读根、降权与seccomp，通过挂载FD保留原目录身份，不回退字符串路径。普通JSON数据读取不等于代码来源许可。工具不自动安装root信任、不接入线上；正式资源隔离及其完整证明、跨机消费桥仍须另行验收。namespace正向测试须显式 `HARBOR_CAPACITY_NAMESPACE_TEST=1`，缺条件则明确跳过，不能当通过；回退移除未接入原语即可，不涉及业务库。

`runtime-source-loader.mjs`作为固定native入口的只读preload，约束本次支持的Node ESM／CJS及直接native addon来源；实际路径须位于候选只读文件系统，拒绝可写临时／材料代码、data URL和显式proc FD入口，普通JSON数据／配置读取保持原行为。私有分页观察只含代码路径／摘要，仍是`formalQualified:false`的辅助证据，须与实际B、Node argv、loader产物、内核挂载及factory私有cap相交，不作为自报资格；不宣称禁止eval或所有内存代码执行。默认legacy启动不加载该preload，私有IPC命令`runtime-sources`明确返回`not-installed`。同版本Node的真实namespace已验证ESM／CJS／N-API正常加载及拒绝边界；实际CI业务B的GW／Worker组件诊断已验证，正式资源与私有cap链仍须在同一组合另验。

`formal-runtime-proof.mjs`将正式runtime的`allocation → pre-seed → final/restore`连接到同一factory的私有nonce/epoch：分角色的一次性句柄先经真实allocator入组，再调用root信任配置固定的launcher；业务镜像、独立harness、只读根、实际Node argv/namespace/降权、模块来源与cgroup交叉检查后，资源观测才能使用不可变来源证明。临时材料仅映射到固定数据目录；旧selfcheck继续原启动路径。root信任的launcher还需固定其构建manifest及摘要，核实二进制内编译的Python/extractor路径与各工具摘要一致；不能只复制launcher而继续执行工作区helper。正文存储仅用私有owner的当前PG/epoch身份收据替代namespace目录别名核验，PG_VERSION/control/relations/WAL/statfs及余量策略仍在受限子进程执行，恢复或PG重启后旧收据拒绝。root信任配置额外要求显式`runtime`预算（`tmpfsBytes`、`maxBytes`、`maxFiles`、`timeoutSeconds`），工具不会自动安装信任或修改宿主。复制DTO、旧epoch、重启后的旧cap及信任变化均拒绝。新版实际CI业务B已取得，public namespace组件诊断覆盖GW／Worker、存储采样、TLS合成设备及正文索引；root信任／正式委派资源与私有cap的同组合仍待验收；其他正式producer未完成时仍在造数前拒绝。

`tests/capacity-runtime-business.test.mjs`是默认关闭的真实B组件诊断：需显式提供`HARBOR_CAPACITY_B_EVIDENCE`、`HARBOR_CAPACITY_B_HARNESS`、`HARBOR_CAPACITY_B_REVISION`及`HARBOR_CAPACITY_B_DIGEST`，并启用既有namespace与独立PG工具入口。材料必须绑定成功CI生产及父子候选检查；诊断只用私有合成库和受控本机上游，不消费真实账号，不安装root配置、不授正式cap，报告始终`formalQualified:false`。tar目录根标记可以使用空名，与既有`.`／`./`一样无写入且先计预算；普通文件、链接及外部harness路径不因此放宽。

容量验证材料可通过可选 `export_capacity_business_oci` 作业取得：仅显式设置 `HARBOR_CAPACITY_EXPORT=1` 后出现手动作业，默认不创建，且不成为部署依赖。它只消费本次业务构建的严格摘要产物，从既有项目registry按digest读取Linux业务镜像，逐对象核摘要与长度，并用当前作业Token上传至同GitLab项目的Generic Package `harbor-capacity-oci`，版本为“流水线ID-导出作业ID”；不接收任意镜像地址、Token目的地、部署命令或账号目录。registry沿现有CI的HTTP内网入口，Bearer认证仅允许同源固定token路径，不回退其他主机；GitLab上传与registry认证分开，禁止重定向转送凭据。认证目的地校验失败仅增加固定检查项代码，不输出原始挑战地址、参数、scope或Token；诊断信息不改变拒绝与凭据隔离规则。

基础设施镜像另由可选手动作业`export_postgres_infrastructure_oci`导出，仅在`HARBOR_POSTGRES_INFRA_SMOKE=1`时出现，作业内固定`HARBOR_CAPACITY_EXPORT_KIND=infrastructure`。它复用上述有界传输与鉴权，严格核对同流水线B和infra两份构建清单、父摘要及实际infra标签；只能读取本项目固定`-pg-infra`仓库，不能指定任意镜像。材料上传到独立Generic Package `harbor-postgres-infrastructure-oci`，完成收据位于`.ci-postgres-infrastructure-materials`，保留两份来源清单。该入口不自动部署、不作为部署依赖，也不替代实际基础设施启动及恢复验收；关闭上述开关可移除后续入口，已交付对象保留。原B导出默认行为保持。

导出认证模式 `HARBOR_CAPACITY_EXPORT_REGISTRY_AUTH_MODE` 未设置时为 `strict`，要求广告realm也同源；非法值和空值拒绝。registry通过内网别名接入但广告公网名称时，可显式选 `registry-origin`：仍校验realm的HTTP(S)、固定 `/service/token` 路径、无凭据／查询／片段及合法service和精确pull scope，再丢弃广告origin，只请求已批准registry自身的固定token路径；不会向广告其他主机发送请求或机器人凭据，不接受额外URL、不跟重定向、不自动回退或重试。该同源接口不存在或拒绝认证时直接失败。此变量只影响B及infra可选材料导出，不影响镜像构建、网关或部署；取消该变量恢复默认拒绝行为，真实接口和上传权限仍需CI验证。

导出总压缩字节预算 `HARBOR_CAPACITY_EXPORT_MAX_BYTES` 默认3GiB、允许1MiB–8GiB；总期限 `HARBOR_CAPACITY_EXPORT_TIMEOUT_MS` 默认15分钟、允许1秒–1小时。先核最大单层所需磁盘，再逐层流式校验／上传／清理，不解包运行镜像；索引在全部对象完成后才发布，失败、取消或到期不写完成清单。失败前已上传的对象可能保留在该次独立Package版本，不能当完整OCI或自动删除其他版本；重试使用新作业ID。小型CI artifact只记录来源与文件摘要，不含凭据。实际使用还须核生产／导出作业成功及父子候选成功链，重新校验下载材料；该材料本身不证明不可变运行、资源隔离或正式容量通过。关闭变量只关闭后续入口，既有交付材料保留；回退移除该可选作业及脚本，不改数据库或线上服务。真实Runner导出与Package权限仍须相应CI验收。

部署作业在 Runner 内用 Skopeo 读取镜像，通过校验主机公钥的 SSH 连接发送压缩 Docker archive；目标主机不必直连仓库内网，也不接收仓库密码。Skopeo 会生成旧格式兼容链接，发送前只复制单一现代 manifest 及其引用的常规文件，不解包或发送旧格式链接和元数据，并先执行与目标相同的校验。归档仅在作业和目标主机暂存，结束即清理，不成为 CI artifact。目标固定允许本项目完整提交 SHA 镜像，拒绝其他 RepoTags、仓库元数据、符号链接、目录穿越及额外文件；归档解压大小最多 2 GiB，并检查剩余空间。

专用 SSH 用户 `harbor_deploy` 使用本项目独立的 Key，强制调用 root 管理的 `/usr/local/bin/harbor-gateway-deploy`，禁用 PTY、端口转发和代理转发；用户不加入 Docker 或 sudo 组。该入口不能指定命令、挂载、端口或其他容器，也不能读取管理密码。部署参数由 root 管理的 `/etc/harbor-gateway-deploy.json` 决定，其中 `image_transport=ssh_archive` 仅由管理入口配置；缺省值 `registry` 保持既有内网部署兼容。CI 分支不能修改服务器权限边界，wrapper 更新须走管理入口。

迁移主机时须同时更新 `GATEWAY_DEPLOY_HOST`、经运维核实的新主机公钥 `GATEWAY_DEPLOY_KNOWN_HOSTS`、CI 的 `SERVICE_ORIGIN` 及服务器配置中的 `origin`。现有 SSH 脚本使用22端口。新主机须保留部署用户及受限入口，并挂载迁移后的 `data`、`runtime` 目录；仅修改地址或网页可访问不代表 CI 已获部署权限。部署私钥可在新主机授权其对应公钥后继续使用，个人登录公钥不能替代该部署公钥；未经核实不更换可信主机条目，不关闭严格主机校验。

默认仍只监听本机。服务器模式显式配置 `HARBOR_LISTEN_HOST=0.0.0.0`、`HARBOR_PUBLIC_ORIGINS`（逗号分隔、无路径的完整 Origin）和 `HARBOR_PORT`；未配置允许的 Origin 时禁止远程监听。Host 与 Origin 必须匹配允许列表，不信任转发头。关闭服务器配置即可恢复默认本机模式。

镜像包含 Node.js 24.21.0、Codex CLI 0.160.1 和系统 CA 证书；官方 CLI 使用系统 TLS 证书验证，缺失时设备码请求无法完成。运行时没有 npm 依赖。容器名为 `harbor-local-gateway`，由 CI resource group 串行部署，数据与独立账号目录分别持久化到 `/opt/product-development/harbor-local-gateway/data` 和 `runtime`。首次生成的管理密码仅保存在 `data/管理密码.txt`，不写入 CI 日志。通过受控服务器管理入口领取，不复制本机的密码、数据库或订阅凭据。

此前从1.12迁往93时保留了管理密码、数据库和账号目录；8个仅引用已安装官方CLI的生成链接由CLI重新创建。1.12旧容器停止并移除，原持久化目录和镜像保留供恢复。当前服务已迁往上述新地址，后续部署须以新的项目目标变量及主机配置为准。

部署先校验并导入提交 SHA 对应镜像，再更新本项目容器；健康、镜像与鉴权检查失败时自动恢复旧容器，首次部署失败时移除候选容器，持久化数据保留。镜像不包含账号凭据，部署成功不代表订阅调用已验收。Codex 使用官方设备码授权，可在访问页面的电脑上完成登录；不能直接使用本机已有账号。服务器暂不支持 Claude 的浏览器登录，Claude 适配尚待真实订阅验收。

会话隐私版本发布前，管理入口须更新 root 所有的部署 wrapper，并在 `/etc/harbor-gateway-deploy.json` 设置 `required_data_contract` 为 `session-metadata-only-v1`。候选镜像必须带匹配的数据兼容标签；检查失败只恢复具有相同标签的旧容器，不兼容旧容器保持停止，避免重新采集标题。未配置该字段时沿用历史回退行为，因此本实例必须启用；客户端及 CI 请求不能覆盖此配置。首次升级若失败且没有兼容旧版本，需要修复候选后重试，不能用旧标题版本恢复服务。

回滚时通过同一部署作业设置 `DEPLOY_IMAGE` 为已验证的旧镜像，仅更新本项目容器。数据库沿用前次兼容的逻辑移除字段，本次迁移不增加数据库结构；后续涉及迁移时须补充数据库备份与兼容性验证。

本轮 SQLite → PostgreSQL 首次切换另提供显式的本机保障发布路径。管理员须在 `/etc/harbor-gateway-deploy.json` 设置布尔值 `local_assurance_release: true`，并启用 root 所有、0600 的 `/etc/harbor-local-assurance-release.json`；缺省不启用，CI 请求不能覆盖。该路径通过 GitLab CI 执行，保留原 SQLite、配置和旧镜像，完成本机一致备份、历史数据导入及核对、候选启动和健康检查；不要求先将备份传回开发机，也不签发异机恢复通过的证明。正式异机恢复路径及其默认要求不变，本机发布成功不代表异机恢复或完整容量验收完成。

共享宿主机上的本机发布可显式启用 root profile 的 `allowDynamicBridgePeers`，仅容忍旧源桥上无关容器的接口新增或消失；源容器、网络命名空间、关键接口及入口规则仍须匹配。候选网关和 Worker 必须通过 `postgres_network` 接入可信迁移配置指定的独立网络，该网络继续严格核验，不回退到默认 Docker 桥。源停止、SQLite 写租约和数据核对保持不变；缺省的严格发布路径不启用此选项。

候选尚未对外开放时，独立发布进程会尝试恢复原服务；操作结果未知或恢复所需的网络身份变化时保留现场，需人工恢复，不保证所有故障均自动回退。新 PostgreSQL 已对外接受写入后，不自动回到旧 SQLite；重新部署主分支程序不能撤销数据迁移，须保留新增数据，按实际存储和兼容情况恢复。关闭本机发布配置不撤销已完成的迁移，当前部署仍须保留运行所需的身份配置；不删除数据或自动回退当前服务。

本机发布已存在 PostgreSQL `ACTIVE` 时，后续代码升级使用独立的 `harbor-postgres-runtime-upgrade-v1` 合同，不重跑首次 SQLite 迁移。管理员提供 root 所有、0600 的 `/etc/harbor-postgres-runtime-upgrade.json` 一次性批准票据及 `/etc/harbor-postgres-runtime-tools.json` 工具清单，绑定当前容器、目标 CI 镜像、原数据库和材料、宿主工具版本及摘要；缺少或不匹配时拒绝升级，CI 参数不能替代批准。升级复用排空、同库预检与优雅停止，成功后记录新 PostgreSQL 容器身份；明确失败且旧 PostgreSQL 双容器重新核验健康时才恢复，停止或提交结果未知时保留现场。没有票据不触发升级，也不撤销当前运行；代码回退仍使用同一 PostgreSQL 数据库，禁止启动旧 SQLite 覆盖新增写入。

已经部署在本机：**http://127.0.0.1:43127/**。Node.js 24 + 内置 SQLite + 原生网页，零 npm 运行依赖。

## 从 GitHub 获取源码

仓库仅包含源码、测试和说明，不含订阅登录凭证、管理密码、数据库或 CLI 二进制。现有电脑上的运行数据不会随 Git 推送。

在新的 Windows 电脑上，先安装 Node.js 24 或更高版本及官方 Codex CLI，再运行「启动 Harbor.cmd」。首次启动会在本机生成管理密码和数据库；进入账号池后通过官方登录连接自己的订阅。Claude CLI 为可选外部依赖，当前适配尚待真实账号验收；可安装到 `runtime/claude-cli/`，或在本机 `data/settings.json` 中设置 `claudeBinary` 为其可执行文件路径。

## 打开与停止

- 双击 **启动 Harbor.cmd**：后台启动并打开密码登录页面，已有服务会复用，重复双击不会启动多个实例。
- 双击 **停止 Harbor.cmd**：停止服务并取消未完成请求，异常用量保留核对记录。
- 管理页面已打开在 Codex 旁边。输入「管理密码.txt」中的复杂随机密码即可登录，无需账号；表单支持浏览器密码管理器保存。
- 不会添加开机启动、修改防火墙或安装全局依赖。关闭电脑后服务停止，下次使用运行启动器即可。

## 密码登录

首次部署自动生成 36 位随机管理密码，找回副本保存在应用目录的 `管理密码.txt`；请勿分享此文件。服务配置保存加盐 scrypt 哈希。登录状态使用 HttpOnly、SameSite=Strict Cookie，有效期 12 小时；退出或服务重启后失效。连续 5 次错误密码会暂停登录 1 分钟。启动器不再通过 URL 自动登录，浏览器不保存管理令牌到网页存储。员工 API Key 与管理密码相互独立。

同一主机并行运行多个实例时，可在各自 `data/settings.json` 显式设置不同的 `adminCookieName`（例如 `harbor_session_preview`），避免浏览器在不同端口覆盖管理登录。名称须为1–64位ASCII字符、字母开头，仅含字母、数字、下划线或连字符；空值、非法类型及其他字符均拒绝启动，PG预检也校验。未设置时沿用 `harbor_session`；名称在启动时固定，不从请求Host或转发头派生。变更后重启并重新登录；只读取和注销当前名称，不回退读取或清除其他名称的Cookie。删除此字段或设回 `harbor_session` 即恢复默认，但同主机不同端口再次共用该名称。配置不修改管理密码、员工Key或Cookie原有属性，也不提供端口间安全隔离；preview仅修改自己的独立配置，原实例保持不变。

## 开始使用

管理侧栏按运行总览、员工与密钥、上游账号池、使用数据和系统管理归类。运行总览只显示请求汇总、请求趋势及运行状态，不显示参考消耗金额；按使用类型筛选时只显示该范围的汇总，不重复展示逐Key、逐类型明细及分页。这些明细统一在使用数据查看，系统操作历史不混入用量记录。

PostgreSQL 首页使用 `GET /api/admin/state?view=overview`，仅查询近7天请求数、每日趋势、员工人数和账号连接状态，不读取会话、Key、请求正文或计价投影；不返回未计算的参考消耗。未指定视图时保持原完整状态接口。离开首页后按需补全页面状态，切页时拒绝迟到响应，无数据库变更。

PostgreSQL账号页通过能力标识`accountStateView`使用`GET /api/admin/state?view=accounts`，在同一只读快照保留原完整账号字段、套餐／额度、权重依据和精确参考统计，只聚合账号维度，不顺带查询会话页、请求列表、每日趋势或操作历史。该视图不代表全量状态已加载；切换其他页面仍按原入口补齐，迟到请求不能覆盖当前页面。旧后端未声明能力时保留旧请求方式；失败保留既有账号信息并提供重试，不把未加载金额显示为零。


会话绑定页支持员工／会话标识搜索、员工当前类型、绑定账号及访问状态筛选。`/api/admin/state` 接受 `session_q`、`session_type`、`session_account`、`session_status=enabled|disabled`，在同一数据库快照内先筛选再计数、分页；未传参数沿用原范围。类型按员工当前配置查询，与请求发生时类型统计区分。筛选不改变全公司汇总或会话绑定，会话页独立保留最后成功的条件、分页与数据，失败时整组恢复并提示重试，旧行管理暂不可用；其他页面刷新不覆盖该范围，离开后返回重新核验当前绑定。服务端已确认近7天自然日窗口变化时不展示旧窗口缓存，保留筛选并从第一页重新读取。旧服务未声明 `sessionFilters` 时不显示筛选入口。

PG 开发入口新增“使用类型默认配置”：A—F 分别表示复杂研发、日常开发、按方案开发、开发辅助、方案与原型及文档处理，不表示职级或权限继承。六类初始均待配置；管理员明确保存允许模型和员工共享限额（参考 USD，滚动24／168小时），也可明确选择全部模型或不限额。复制其他类型仅作为编辑起点，保存后各自独立。默认保存只供后续新增或显式应用使用，不修改现有员工。

管理接口为 `GET /api/admin/member-types` 和 `PATCH /api/admin/member-types/:id`，修改携带 `expected_version`，默认规则同时提供 `allowed_models` 与 `usage_limits`。新建员工明确选择类型，提交 `type_id`、`expected_type_version`、`expected_template_version`；模型与限额分别选择 `models_mode`／`limits_mode` 的 `template` 或 `custom`，自定义才提交对应字段。版本冲突返回409，页面保留草稿并要求核对最新配置；待配置类型必须明确自定义两部分，不能隐式获得全部权限或无限额。

类型卡片提供“历史版本”。`GET /api/admin/member-types/:id/templates?after=0&limit=25` 按版本倒序分页，只读取近7天，默认25、最多100项，并受256KiB字节预算约束；更早版本仍保留数据库。选择历史版本后预览实际模型／每人限额，确认通过 `PATCH /api/admin/member-types/:id` 提交 `{expected_version,restore_version}`，不能同时混入其他修改。恢复沿当前模型目录和限额规则校验，事务内创建新的递增模板版本、切换当前默认及记审计，不改写原模板，不改变类型启停或名称，也不自动应用到现有员工。版本冲突保留恢复预览并可重新读取；如历史模型或费率已不再满足现行校验，则明确拒绝，不能借恢复放宽校验。无需新增数据库结构。

员工、已应用配置、首把 Key 和审计同事务保存。后续 Key 省略 `allowed_models` 时沿员工已生效默认，不直接读取类型最新规则；单把 Key 的显式覆盖不改员工默认。旧创建 API 未传类型仍保留原明确配置行为，原员工不自动分类。`PATCH /api/admin/members/:id/type` 当前只实现 `apply_defaults:false`，并携带 `expected_usage_type_version` 与目标 `expected_type_version`；只改类型不改权限、累计消耗、会话或在途请求。

请求受理时由服务端保存类型 ID、代码、名称及分类版本，完成结算不追随员工后续分类，客户端不能提交记账类型。`011-member-types.sql` 追加类型、不可变默认版本、员工／Key 配置来源及请求快照，001—010迁移保持不变。回退旧 PG 版本会失去类型管理和新请求快照，不能将列兼容视为行为兼容；当前默认 SQLite 与正式部署入口未切换。

PG 类型卡片的“预览并应用到此类员工”和员工详情的“预览并应用配置”共用固定目标批次，可分别应用默认模型／共享限额、可选同时修改分类。默认保留个人自定义、保留的旧配置和来源不明配置；明确覆盖后，预览列出个人及逐 Key 变化，未删除的停用 Key 仍保持停用。每名员工的多 Key 与个人限额同事务修改，既有消耗不清零、已派发请求继续完成；后续新 Key 沿员工已应用默认。

接口为 `POST /api/admin/member-config-batches/preview`，指定 `scope:{type_id}`（此类型全部未删除员工）或 `scope:{member_ids:[...]}`（固定选定员工），以及目标 `type_id`、`expected_type_version` 和明确布尔值 `models/limits/classify/overwrite_exceptions`。只保存预览，不立即修改员工。`GET /api/admin/member-config-batches/:id?after=0&limit=25` 返回有界逐人前后配置、例外和结果，`POST .../:id/execute` 只接受空对象并异步受理，由独立 `npm run worker:postgres` 执行。执行核对类型、模板、员工配置版本及原 Key 集合；变化项跳过，不重新扩大目标。保存成功指配置、审计和结果已真实提交，受理成功不等于完成。

`POST .../:id/retry` 只从失败项生成新预览，保留原目标与版本；`POST .../:id/undo` 只从已应用项生成撤销预览。确认执行撤销前核对应用后完整配置，后续改动的对象跳过；还原使用新的递增版本，不倒退版本或恢复历史用量。Worker 中断只恢复未完成项，不重复应用已提交项；重试耗尽在结果中明确显示，可重新预览未完成项。若全部员工已提交而作业终态丢失，重试提交原批次完成状态并返回 `no_retry_needed:true`，不创建空批次，也可直接生成撤销预览。`DELETE .../:id` 取消剩余项，已完成配置保留。员工页“配置记录”可重开近7天预览／结果，`GET /api/admin/member-config-batches?before=<上一页末ID>&limit=25` 分页读取；更早记录只保留数据库。只有当前打开且执行中的结果自动刷新，历史、预览和配置表单不轮询。

批次结束后同步员工缓存；新增 Key 前重新读取员工默认，并提交可选 `expected_defaults_version`，服务端在员工锁内核验，变化返回409，继承时省略 `allowed_models`。原 API 未提供此版本仍保持兼容，页面不会因旧员工缓存误提交“不限制”。员工选择保留旧默认时，其继承 Key 也作为保留例外；只有明确覆盖才共同改用新默认。

`012-member-config-batches.sql` 追加批次和逐人配置记录，001—011迁移不改；操作者沿现有单管理员登录记为 `admin`，不接受客户端指定。仅保存必要配置与内部 Key ID，不复制访问 Key、账号凭据或对话。服务端构造参数默认 `maxTargets=1000`、`maxKeysPerMember=1000`、`maxPreviewBytes=4MiB`、`pageBytes=256KiB`、`ttlMs=1小时`，分别支持1–10000、1–10000、1KiB–64MiB、1KiB–2MiB、1分钟–24小时；超限整份预览回滚，不默默省略对象。默认页面25项、最多100项，并受字节上限约束。配置由服务器构造，不由请求覆盖；旧版本无此 Worker 处理器，回退前应处理或取消未完成批次，已应用配置不会自动倒回。

PG 员工页使用 `GET /api/admin/members?page=1&page_size=25&q=&type_id=&status=`，名称按字面子串搜索，类型可选目录 ID／`unclassified`，状态可选 `enabled/disabled`；默认25项、最多100项，服务端排序分页。行内显示员工当前类型、Key数量、默认模型与配置来源、精确滚动限额及剩余、近7天／12小时参考USD和最近使用。列表不读取或解密 Key 凭据。筛选／搜索变化清空选择，页大小与分页保留在当前页面状态；搜索防抖且取消旧请求，加载期间禁用旧范围操作。

选择本页以稳定员工 ID 跨页累计；“选择全部筛选结果”通过 `GET /api/admin/members/selection` 固定当前条件下的ID与分类／默认版本，默认最多1000人，超限明确拒绝而非返回部分目标。批次 `scope.member_versions` 可同时携带这些版本；选择后分类／配置已变时，预览返回409要求重新选择，预览后发生变化仍按原逐人跳过规则。整类配置与已选员工配置入口分开，搜索条件不暗中缩小整类范围。

`GET /api/admin/members/:id` 只返回单员工概要；`GET .../:id/keys?page=1&page_size=25` 才加载该员工当前页Key配置与完整凭据，沿现有管理员鉴权及加密验证，支持翻页和复制。`GET /api/admin/keys?page=1&page_size=25&q=&member_id=` 为“使用数据 → Key 用量”独立分页，只返回前缀及用量，不返回凭据；运行总览不再读取该明细。API固定会话选择员工也改用已启用员工分页搜索，不依赖全量员工或Key缓存。迟到结果不能替换新弹窗或其他页面，无新增全局轮询。

页面读取 `/api/admin/state?people=none`，只保留公司总计、员工／Key数量和其他原状态，不读取员工或Key配置、也不解密凭据。未提供 `people` 的旧API保持完整状态格式，但服务端 `maxPeople` 默认1000（1–10000）限制员工与Key数量，超过返回 `state_people_capacity`，不能成为无上限旁路；此参数是构造期配置，客户端不能放宽。员工／Key页的单页响应默认最多512KiB（服务端构造参数 `pageBytes` 支持1KiB–2MiB），超限明确拒绝，不隐藏部分结果。013迁移只增加员工／Key排序与请求用量索引，001—012不改；分页关闭或回退仍保留数据，旧SQLite页面行为保持原样。

批量启停／续期、设备管理及类型统计筛选已接入PG页面，各自按需分页，不新增全局轮询。现有员工详情旧分类接口仍保留，选择同时应用新默认通过上述预览入口完成。类型与分页界面分别受 PG `memberTypes/pagedMembers` 能力控制，不为 SQLite 假装提供新管理接口。套餐统一调度及正式发布组合继续集成验证，完整容量尚未验收；当前默认 SQLite 和部署入口未切换，本批不部署。

1. 在“员工与密钥”创建员工并设置Key有效期。
2. 复制完整Key，按下方Windows/WSL说明配置Codex客户端。
3. 同一个Key可用于多个并行会话；上下文和工具由客户端管理。
4. 在“使用数据 → Key 用量”查看每个Key最近7天用量。续期、停用和新增Key不会重置窗口内统计。

## PostgreSQL设备接入开发入口

设备辅助程序见 [Windows／WSL接入说明](clients/harbor-device/README.md)。PG网关的同进程HTTPS入口可读取受控配置：`HARBOR_DEVICE_TLS_ENABLED=1`、`HARBOR_DEVICE_TLS_PORT`、`HARBOR_DEVICE_TLS_KEY_FILE`、`HARBOR_DEVICE_TLS_CERT_FILE`、`HARBOR_DEVICE_CA_CERT_FILE`；文件均须为绝对路径，TLS私钥须由运行用户持有且只允许本人访问，证书不得被其他用户写入。不自动生成或替换CA。需要新登记、签发时另配置同CA的 `HARBOR_DEVICE_CA_KEY_FILE`，未配置签发私钥时明确拒绝签发，已批准设备仍可按既有证书使用。生产必须使用客户端信任且域名匹配的服务器证书；普通HTTP与HTTPS共用Host／Origin检查、管理Cookie、资源预算和业务处理器，不通过HTTP转发头传递设备身份。

开关默认关闭，非法开关、端口或缺失证书在启动时拒绝。关闭TLS仅关闭接入传输，数据库内已有员工强制策略及Key设备范围继续拒绝无证明访问，不自动降级。设备认证沿真实TLS证书及同进程请求证明，客户端填设备ID、转发证书头或请求正文不能成为授权依据。读取请求前、受理事务内及真正派发前分别核验；Responses、图片、文本CLI、其他平台和员工自查询均覆盖。设备停用阻止新请求和未派发任务，已经派发的请求完成原计量，不重放模型调用。受理时保存设备名称、系统、证书代次与认证状态，后续改名、停用或换机不重写历史；旧记录保持设备未知，兼容入口明确标记未核验。

管理Cookie保护 `/api/admin/device-invitations`、`/api/admin/devices`、设备批准／启停／取消待续期、员工 `device-policy` 和Key `devices` 接口。邀请、挑战和登记凭据只在受限TLS登记接口使用；邀请与登记请求受64KiB限制，列表不返回原始硬件编号、私钥、邀请凭据或登记凭据。先批准设备并完成一次真实模型请求，管理员才能开启员工强制策略。CI的真实PG设备测试明确安装OpenSSL，运行镜像也显式包含该签发依赖；普通CI不会伪装Windows实机验收。

设备管理页面及真实Codex多轮工具链已通过私有PG、Chromium及实际Windows／WSL辅助程序验证；模型响应来自受控上游，不代表真实官方账号间的不透明上下文兼容。正式TLS部署组合及容量／回退仍在同一需求中继续验收，默认SQLite服务尚未切换到PG。

PG共享入口已接回 `GET /v1/models`、`GET /v1/codex/models` 和固定图片MCP脚本下载。目录复用上游模型元数据与精确Key白名单；仅不限制模型的Key显示本地codex／claude别名，不合成未确认模型。管理员Cookie不能代替员工Key。读取前后都核对Key／模型／设备策略及固定账号代次，迟到目录不得放宽权限；源超限整次拒绝，普通源失败标记 `catalog_status.complete=false` 与完成header，不能当作完整目录。上游目录IO与网络发送不持数据库事务，各自有界；慢读、断开及关闭等待真实收尾。脚本固定读取，不插入账号或Key。

图片MCP可读取 `HARBOR_DEVICE_LOCAL_TOKEN` 并仅向固定回环HTTP辅助入口发送，远程地址或畸形令牌在生成前拒绝，默认未配置时保持原协议与文件保护。模型provider和MCP需分别配置，详见 [客户端说明](clients/README.md)。本机实际官方CLI已验证目录下载、生图、参考图编辑与文本续用；候选镜像固定客户端版本的同组合仍须另行验证，不能复用本机不同版本证据。没有数据库迁移或新后台轮询。

## PostgreSQL开发验证入口

规模化存储正在开发，当前日常启动仍沿用SQLite。迁移命令只初始化新PostgreSQL schema，不搬迁旧库或切换流量；独立开发应用入口见下文。正式切换仍需历史迁移、功能对齐、备份恢复及容量验收。

隔离物理恢复演练在删除故障源目录前，核对本次启动的 PostgreSQL 及其后代的 UID、内核亲缘和启动时间，冻结后通过 pidfd 终止各个独立会话，并确认无存活后代；不额外排空或刷新 WAL。Python 3 的 pidfd 能力不可用、身份变化或目录有未知进程占用时拒绝，不回退裸 PID 信号或重复删除；无法确认退出时保留材料。该检查仅用于任务内恢复演练，不改变生产服务，也不代表正式容量或异机恢复通过。

新增独立开发应用入口`npm run start:postgres`，普通`npm start`和服务器默认命令仍是旧入口，设置数据库URL本身不会切换流量。新入口读取相同的网络配置和受控`data/settings.json`，首次密码只写权限0600的`data/管理密码.txt`，不输出密码。仅连接已运行`db:migrate`且结构校验匹配的数据库，不自动迁移、搬运或双写SQLite；数据目录仍有`gateway.sqlite`时拒绝启动，须等待正式迁移工具完成。已接入账号／会话管理、固定费率计量和现有推理通道；正式迁移、套餐统一调度、容量及发布组合仍须验收，不能把该开发入口直接替代现网。

应用按核心／管理查询／正文写入分离连接池，默认12／3／2条连接，构造参数`resources.corePoolSize/queryPoolSize/recordingPoolSize`控制（各1–64，重启生效）。管理列表、统计、总览、详情、正文分页、费率及限额状态共享有界查询预算，限额保存仍使用核心短事务；真正的权限和受理限额不能读取可延迟缓存。独立1条连接持有数据库会话锁，跨目录也只允许一个活动网关；本机目录锁仍防止同目录重复启动，所有权丢失停止接纳并中断在途。CLI通过只读`ownershipSignal`复用原关闭流程，必要收尾完成后退出1，由既有容器重启策略创建新进程、重新核验数据库和实例锁；不在失锁进程中盲目重新获取锁，也不自动重发模型。嵌入式factory保留调用方控制关闭的行为，数据库不可用期间继续拒绝请求。

启动先校验迁移及数据契约、恢复关键计量暂存，再将遗留未完成请求标为中断／未知，保留已保存正文前缀并标明不完整；关键暂存损坏则拒绝启动，正文暂存损坏只降级采集。密钥丢失时不生成替代材料。003迁移追加未完成请求和待采集记录的部分索引；启动按批处理这些状态，不同步扫描已完成历史。`GET /health`是存活，`GET /ready`检查数据库及实例所有权；管理员`GET /api/admin/status`提供连接池、查询／记录／计量积压及事件循环指标，不返回目录或凭据。

正常关闭先停止接纳并排空，默认等待5分钟（构造参数`drainTimeoutMs`，100ms–1小时，重启生效），期间已接纳调用可以完成；超时或中途失去实例锁时中断、结算并持久暂存，最后释放连接池和实例锁。排空期间仍监测失锁，只有在途任务和必要持久化收尾后才按主动释放处理；失锁关闭返回异常退出状态。新形成的空闲长连接会及时关闭，避免把已完成请求误判为超时。计量恢复必须先于遗留请求标记；关闭正文采集不关闭计量。该开发应用的启动、池隔离、异常恢复和排空已有隔离真实数据库／HTTP验证，正式全通道准入、容量、迁移及发布仍须继续验收。

PG命令入口及受控部署共享 `postgres-shutdown-v1` 关闭合同。root配置 `postgres_shutdown` 包含 `gateway_drain_ms`（默认300000，100–3600000）、`cleanup_grace_ms`（默认30000，1000–300000）、`worker_stop_ms`（默认30000，1000–3600000）；部署分别映射为 `HARBOR_GATEWAY_DRAIN_TIMEOUT_MS`、`HARBOR_SHUTDOWN_CLEANUP_GRACE_MS`、`HARBOR_WORKER_STOP_TIMEOUT_MS`，非法值在启动前拒绝，客户端不能覆盖。网关正常排空退出0；截止后中断请求且关键收尾已完成退出2；失锁、关键暂存未完成或异常退出1。部署按旧容器自身已核验的实际配置计算等待预算，先确认网关退出，再停Worker；候选失败同样先确认收尾，再移除已停止的容器。Docker超时、身份或退出状态不明、诊断无法持久化时保留组合与材料，不自动回退。首切已停止的旧SQLite须由root显式配置 `postgres_legacy_source_engine: "sqlite"` 并通过固定入口及镜像身份检查，不能据此自动重新启动SQLite；旧PG缺少关闭合同也只接受已停且来源验证完整的状态。兼容回退仅使用同数据、schema、设备CA及关闭合同的旧PG组合，不覆盖新请求或密钥。隔离真实PG信号排空已有验证；实际Docker发布／首迁／失败恢复仍需演练，不据单元测试认定正式发布通过。

使用Node 24.21.0、PostgreSQL 18.6及锁定的`pg 8.23.1`。执行`npm ci`安装依赖，通过进程环境注入`HARBOR_DATABASE_URL`，再执行`npm run db:migrate`。连接地址及密码不写入源码、命令参数或输出。跨主机默认使用`verify-full`认证加密，禁止关闭证书校验；同机Unix socket可用于隔离开发。显式允许同机私有容器网络的能力仅由服务端配置或测试代码控制，客户端请求不能覆盖。

连接池默认12条连接（可设1–64）、建连超时3秒（可设100ms–30秒）、语句超时10秒（可设100ms–10分钟），空闲事务30秒超时。迁移命令使用120秒语句超时，以事务锁串行执行，保存SQL校验值；已应用迁移的内容不允许静默改写。独立`test_postgres`作业用真实PostgreSQL验证，普通回归未提供隔离数据库时明确跳过相应集成项，不将跳过写为通过。

连接配置先按驱动规则解析URL、显式参数及环境，再按最终主机校验传输；显式CA和客户端证书不会被URL的再次解析覆盖。迁移函数显式提供连接时不回退到其他环境URL。同一父事务内的并行子事务及查询按上下文排队，子层独立SAVEPOINT；父失败先排空已接受操作再回滚，关闭后的异步上下文不能借用已归还连接。

每条物理连接从建立到退出均观察驱动的连接错误，覆盖借出、执行、两次SQL之间及归还交界，避免原生未处理错误使进程退出。根事务与保存点共享首个连接故障，排队SQL和提交前重新检查；回调即使捕获查询异常，也不能将坏连接事务报为成功。不自动重试，回滚仅尽力且不覆盖原始错误；已确认的COMMIT仍算成功，未获确认的提交保持原有未知处理。`pool_errors`统计观测到的物理client错误，同一client去重并涵盖借出及空闲连接，不代表所有失败SQL或建连尝试；守卫连接仍沿既有失锁保护停机，无数据库迁移或新开关。

Codex上游HTTP响应提供`x-request-id`时，网关仅保存有界且通过凭据过滤的编号；请求详情显示“上游请求编号”。编号随成功、拒绝或中断结果及计量恢复保存，不以响应正文ID、客户端头或会话ID代替；缺失时不推测。此字段仅用于排障，不参与鉴权、路由或计价。复用现有列和恢复格式，旧版回退仍可结算，但可能忽略尚未回放的新增编号。

PostgreSQL Codex新请求记录显式传入的推理强度与服务档位，详情标为“请求推理强度／请求服务档位”，不代表上游实际采用的值；未传、非法或疑似凭据值不记录，不推测历史默认值。参数不参与Standard参考USD计价，不修改原始转发内容；列表展示已有采集状态，不为此读取正文。复用现有列，无数据库迁移，旧版本回退可忽略新增记录值。

请求列表同时展示已采集的输入摘要：复用既有加密预览，最多200字符，仅批量读取当前页请求对应的小密文，不读取正文事件或触发全文整理；纯统计查询不读取摘要。仍要求管理员身份，并保留原分页和筛选范围。未保存、过期及解密失败分别显示状态；单条摘要损坏不影响其余请求，关联不确定时明确标注。页面将摘要作为纯文本呈现并限制为两行，完整正文仍在详情按需读取。无数据库迁移，回退仅停止列表摘要展示，既有加密内容及保留期不变。

PostgreSQL请求列表／统计已接入`GET /api/admin/requests`，复用原筛选参数及`view=list|summary|all`，列表只取当前页及下一页标识，汇总用同一读事务的SQL分组精确计算，不在应用层复制整窗请求。读取固定价格投影，投影／版本缺失显示缺价，不能当0；原始计量和数据库内部事务标识不进入DTO。`002-request-queries.sql`只追加插入事务标识及短期分页快照表，既有001校验和不变。

类型统计适配器`postgres-type-statistics-admin.mjs`提供管理员`GET /api/admin/type-statistics`，按请求受理时保存的类型归属统计，支持`group_by=type|model|member`、类型／员工／模型／业务结果组合筛选和有界分页。只查近7个北京时间自然日，同一事务中的总计和分组保持相同水位；新请求的未分类与历史无类型快照分开。失败率仅使用已确认成功与失败，取消／拒绝／中断／未知单列，人工核对费用不改变业务结果；金额继续使用固定费率版本的精确Standard参考USD，不输出原始Token统计。 价格版本预算先在同一只读快照中查询有界的版本目录；目录总数未超限时不重复扫描请求，目录超限才按原条件检查实际使用版本。未使用的历史版本不会误拒绝查询，实际版本超限仍整笔拒绝；无数据库迁移或统计口径变化。

PG请求解析器通过服务端`usageTypes`能力接受`type`、`device`、`outcome`、`from/to`及缺失模型／员工筛选，旧SQLite解析器保持原契约；设备仅可指定稳定UUID或`unknown`，不是IP。`outcome`查业务结果，旧`status`查原记录状态，两者不能混用；新版PG查询构造时显式启用`businessOutcomes`，旧库适配器默认保持旧汇总语义。类型统计每行的`requestFilters`可用于请求页和显式正文后台导出，筛选身份纳入持久分页快照，改变类型、设备或时间须重新读取第一页。导出追加`type_snapshot_version/type_source`，明确未分类／历史未知，保留CSV防公式与两位参考USD。适配器已接共享PG网关及类型统计页面；总览类型请求与全公司健康指标分开，费用核对不改判业务结果。默认SQLite保留原查询契约，正式容量与发布仍须验收。

分页用数据库原生可见性固定“首次读取前已提交的插入”，早取得序号但后提交的请求不会混入旧页；后续结算／删除仍按当前数据展示。快照保存在数据库，默认有效1小时且不跨北京时间日界，默认按4096份缓存清理，过期／筛选变化返回`invalid_request_snapshot`并可刷新第一页。TTL和缓存规模由服务端构造参数控制（1分钟–24小时、8–65536份），客户端不能指定。读取水位是PG可见性快照的摘要，仅说明读取快照不同，不据此声称请求一定变化；分组与总计来自同一事务。

管理查询默认同时执行2项、排队8项、排队15秒超时；范围分别为1–8项、0–64项及100ms–60秒，执行沿用数据库语句超时。等待／执行超时返回`query_timeout`，满队返回`query_queue_full`，关闭拒绝等待项并排空执行项。可显式注入独立查询适配器；新开发应用已分离查询／核心／正文连接池，高基数分组限界与全规模验收仍待完成。逻辑恢复到新PG集群时，插入事务标识及旧快照不能直接沿用，须在停写恢复流程中清除快照并重新建立派生可见性；物理备份／WAL恢复保持事务空间。相关正式恢复工具仍待交付，不能直接把此开发查询当作切流依据。

018迁移注册套餐目录、不可变配置版本、账号归属及固定目标预览；升级前已有账号只标记`legacy`，不按套餐名称猜倍率，迁移后新账号无选择时是`unknown`。套餐策略默认关闭仅作开发／切换保护；开启后直接启用容量预算，不依赖旧24小时预算开关，也不改写旧开关。关闭后保留套餐元数据并恢复原兼容算法，已产生的调用、计量及人工配置不回滚。

账号池的套餐入口使用普通表单维护平台、协议／模型范围、多窗口容量、共同单位换算及依据，官方百分比与绝对余量、人工调度估算分开显示。模板修改先固定目标预览再确认，个体覆盖默认保留；账号选择固定模板版本，官方识别代次和配置版本共同防止迟到确认覆盖新状态。策略启用前列出未知、冲突及不可比较组；账号池可按平台、套餐、状态、额度及异常筛选，显示自动参考、人工比例、最终生效值和排除原因。

新会话、预览及定时转移共用已确认单位的有效预算，保持24小时平滑，多窗口换算后取最紧约束；可信官方绝对余量优先，不能被取整后的百分比误判覆盖，任一真实窗口耗尽仍排除自动分配。未知或过期不冒充零；多组不可比较时明确拒绝自动新分配，已有健康绑定继续使用，不因此强制换号。旧CLI文本通道仅使用明确适用`chat`及`*`或`codex`粗入口的套餐，不推断实际模型；原本地预算、待处理量和并发保护保留。按量API的人工份额独立，不混作订阅容量。

临时人工比例有效期间保留人工选择，后台可以刷新自动参考，定时批次明确按人工权重跳过；已等待的旧自动整组交接也会失效。下次北京时间02:00真正到期后使用最新有效预算。人工正比例或固定绑定也不能单独豁免耗尽：须同时满足全局额外额度许可、账号未禁止、独立核验的有效权益和预算；人工零比例仍限制订阅账号。账号停用、登录失效及模型／协议限制继续生效。套餐增量仍须与正式候选发布组合及完整容量共同验收，页面可用不代表已部署。

隔离存储演练使用[迁移与恢复工具](ci/storage-recovery/README.md)，支持固定SQLite来源及完整文件材料的加密迁移、私有PG18物理备份与WAL恢复点验证。checkpoint绑定真实来源和目标，完整性校验贯通清单、分片及文件，原Key／账号凭据／正文／设备策略和事务快照保留；只有真实恢复并重新连库核对后才形成存储兼容证明。该证明始终不批准部署，旧SQLite镜像不能直接承接新PG写入；真实候选镜像、wrapper、正式规模RPO／RTO及最终环境组合仍待验收，不自动读取线上源或切换服务。

迁移控制器开发入口为 `node storage-operator.mjs run /etc/harbor-storage-operator.json`，固定Linux/root配置示例见[operator配置](ci/storage-recovery/operator.example.json)。它区分真实来源停写、SQLite写锁与只读快照、分页导入、逐行／文件复核及最小权限角色晋升；不会把同路径实例锁当成跨容器停写证明。当前正式`run`入口因完整恢复及部署消费能力未接齐，在连接PG、创建目标或停源之前只读返回blocked；任务自有真实进程／PG验证路径只签认证pending，不能批准部署。管理连接只从root私有的 `/etc/harbor-storage-operator.env` 读取，备份与状态签名分别使用独立的 `/etc/harbor-storage-backup.key`、`/etc/harbor-storage-activation.key`，不与员工／正文密钥混用；不自动安装这些配置或切换服务。

首跑迁移准备使用独立命令`node storage-operator.mjs prepare /etc/harbor-storage-operator.json`。它先按下述固定root材料重验原PG基线、本机PITR及指定异机接收签名，在同一受控回调内保持备份上下文存活，取得operator锁后再次完整核验，才创建认证状态及目标、停住原来源并执行迁移。候选`publicOrigin`沿用部署批准的HTTP或HTTPS公共入口，独立`deviceOrigin`仍必须HTTPS；两者均拒绝凭据、路径、查询、片段及多值，不因迁移擅自更换员工入口。基线摘要写入加密认证状态，但磁盘摘要或JSON副本不能替代进程内能力；已有operator状态明确拒绝首跑入口，须保留材料并使用下述resume恢复；独立cleanup负责回收许可。该命令会改变迁移目标并停止来源，不能当只读检查运行；不自动安装或执行。成功仍为pending，运行角色回收至NOLOGIN，不启用候选服务、不消费activation、不批准部署。原`run`与cleanup行为保持，撤销新prepare命令即可关闭此首跑入口；认证状态多出基线说明，业务数据库格式不变。正式root／Docker与异机完整组合仍待验收，任务内真实迁移不能代替。


迁移准备中断后，`node storage-operator.mjs resume /etc/harbor-storage-operator.json`只处理原认证状态及原配置绑定的同一run。它以只读注册库、实际库／角色OID和晋升记录核对当前集群，只允许本次迁移已有见证的增量；不认领同名新对象，不接受未知数据库创建结果。原基线清单保持不变，重新执行原包PITR与指定接收端签名核验，摘要须与认证状态中的原基线完全一致；取得operator锁后再重验，随后复用原迁移幂等流程。恢复上下文不能制作新basebackup／checkpoint，状态推进或关闭后旧恢复能力失效；未带原基线的历史状态不能自动升级。命令可能继续停源及写迁移目标，成功仍pending，不启用服务或消费activation。取消／失败保留原材料，独立cleanup不依赖恢复校验成功；删除resume入口可关闭该能力，既有业务格式不变。真实task中断／续跑验证不代替正式root、Docker和异机组合验收。


原迁移到达pending后，`node storage-operator.mjs backup-final /etc/harbor-storage-operator.json`重开并核验认证状态绑定的原基线，完成原包PITR及原接收签名核验后，在operator锁与SQLite写锁内生成最终数据库备份、应用文件及检查点；实际本机恢复确认原凭据、精确金额和材料后才发布`pending-final`传输包。只消费该验证能力内部的原backup／restore配对，不接受替换的JSON或另一个公开摘要相同的配对；锁内再次验证原恢复state，防止预检与取得写锁之间的状态变化。命令只读取固定备份密钥，不读取或使用activation签名密钥，输出仍`deploymentAllowed:false`、`applicationMaterialsVerified:false`、`offhostVerified:false`。该包须交给正式接收端验证实际候选、原设备及异机恢复，不能据本机文件／数据库恢复替代这些证明；不自动传输、启动服务或部署。取消时保留已形成的备份及原状态，等待实际恢复进程结束并释放写锁。删除新增命令及包装入口可关闭该能力，原底层备份API与包格式保持兼容。

`node storage-receiver.mjs restore-and-attest-application`在固定root接收配置下执行真实恢复，并为实际批准的OCI网关、独立Worker、原Key、TLS和新鲜存储报告组合签发独立应用收据；原数据库恢复签名接口保持。签前按原认证handoff清单重验全部应用档案，篡改、删除或清理失败均拒绝。有设备但缺原客户端恢复挑战时仍拒绝，只有实际恢复库的设备及凭据均为空才能按空集合处理。该收据不单独批准源主机部署。

PostgreSQL发布复用受控准备与activation已核准的预装镜像，不在切换阶段重新`load`或`pull`。SSH传来的归档仍按原结构校验，但不导入或执行；实际镜像缺失、摘要或Descriptor／RepoDigest不匹配时，在预检、消费许可和切换前拒绝。这样避免Docker snapshotter把docker-archive转换成不同manifest；缺镜像须恢复批准OCI材料后重试。SQLite的原归档导入／拉取流程不变，客户端请求仍只有action/image。

Docker镜像身份分别保留OCI config摘要和manifest摘要：classic后端的镜像ID对应config，containerd snapshotter后端可对应manifest。部署只接受已签名的这两种强身份；snapshotter还核实际Descriptor与批准的RepoDigest，tag不独立授权。预检回执使用实际`imageId`，撤销／恢复仍核原代次及不可变摘要，不因tag变化误认或误删其他镜像。此兼容不修改签名facts或数据库结构。

源端`node storage-operator.mjs activate /etc/harbor-storage-operator.json`读取固定root公钥与`runDirectory/final-receiver-application-attestation.json`，重验原基线、pending最终包、当前材料与实际候选预检；operator锁、源SQLite写锁和私有资格都保持到签发结束。许可只加密保存为`activation-approved.enc`，不打印签名或凭据，不启用LOGIN、不启动服务；出现已有部署秘密阶段文件时拒绝重签，保留原恢复材料。首次停源的容器ID及StartedAt/FinishedAt写入认证状态，每次重入和末次校验都比较原代次，来源重启、替代容器及缺原代次的历史pending不能自动升级；独立cleanup不依赖源容器仍存在。部署消费仍须走后续GitLab入口并验证实际组合，当前组件／内存生命周期结果不代表正式root、Docker及异机完整组合通过。回退可移除新增命令并保留未消费材料，业务数据库格式不变。

仅显式配置`infrastructurePublisher:{mode:"installed-bootstrap-v1"}`时，晋升允许在共享单调期限内等待所有其他客户端真正离开；不按角色或应用名豁免，不终止外来连接。发布器每次报告后释放连接，事务内刷新统计快照；角色锁等待、报告绑定与候选预检共用预算，登录许可按锁后剩余时间收窄，提交前到期则回滚。默认仍立即拒绝其他连接。

控制器的私有预检许可最长120秒，使用数据库时间、SCRAM认证的密码有效期及语句／空闲／事务上限；密码到期仅拒绝新连接，不会自动终止已存在会话。预检结束撤销LOGIN并只回收该run、DB、随机application及时间窗内的连接；中断恢复先认证旧状态并回收旧许可，再拒绝不匹配的新配置，不能误停新来源或其他库的会话。材料顶层保持root所有，仅挂载子树交UID1000。pending不是可消费许可，任何要求activation的校验仍拒绝；跨库PUBLIC CONNECT不能靠本库权限抵消，正式激活前仍须真实验证。当前已有私有PG、原Key／精确金额、源写锁及中断恢复证据，实际root布局、Docker来源／候选、完整备份／离机恢复和单次激活消费仍需后续验收。

迁移创建代次由独立控制注册库认证：root配置必须明确批准 `registry.database/databaseOid` 和元数据写入，并配置 `backupKeyVersion`；不会隐式选择业务库。角色创建与OID见证同事务，数据库创建前先记认证意图，成功后记录实际OID；控制记录提交但本地状态未确认可恢复。若数据库已创建而首次OID尚未见证，保持unknown并保留来源与材料，只撤销已知代次角色的许可，不认领或自动删除同名对象；须使用新批准目录与全新随机名称重新准备。运行角色同名重建也拒绝，避免误撤外来角色权限。

迁移前现有PG集群可独立制作`cluster-baseline`认证包，无需先创建迁移目标或`operator-final.enc`。在原固定root备份配置及WAL归档已准备的前提下，先执行`node storage-backup.mjs backup`和`checkpoint <备份UUID>`，再执行`node storage-backup.mjs baseline-handoff <备份UUID>`；最后一步真实本机PITR成功、源集群清单及启动代次未变才发布`handoff/`。接收端固定配置须显式设置`handoffKind: "cluster-baseline"`；省略时保持`pending-final`，旧无类型最终包继续兼容。两类包不能互换：基线包仅验证原数据库／WAL及恢复点，不含最终应用材料证明，不批准部署。包和密钥分别交付，接收端仍可在源数据库离线时恢复；真实异机、root配置及最终激活消费需另行验收。撤销新增命令和类型即可回退，旧最终包及业务数据库格式不变。

接收端可显式执行`node storage-receiver.mjs restore-and-attest`，在真实恢复、退出recovery及写读核验后签发Ed25519结果；默认`restore`仍只返回原恢复结果。独立接收端签名私钥须由管理员预先放在固定`/etc/harbor-storage-receiver-signing.key`（root、0600、PKCS8 PEM），不复用共享备份加密密钥；工具不创建或安装密钥。源端只分发经独立批准的对应公钥，并固定核对类型、备份UUID、源／接收机器身份、配置／集群及包／检查点摘要，不能信任结果自带的公钥或布尔值。签名API只消费尚未关闭的真实receiver／restore配对，JSON副本、task scope及同机恢复不能通过正式异机校验。签名仅证明对应范围的恢复结果，不能替代最终应用组合、正式容量或单次激活；正式operator／部署消费尚待接线，部署许可保持关闭。

迁移前可运行`node storage-operator.mjs check-baseline /etc/harbor-storage-operator.json`。管理员先在该固定配置中批准`baselineReceiverMachine`，将接收端对应的Ed25519公钥以root、0600放到固定`/etc/harbor-storage-receiver.pub`，将其签名结果以root、0600保存为已配置runDirectory下的`baseline-receiver-attestation.json`；不能采用结果自带的公钥。命令按现有固定root备份配置验证原集群备份并重新执行真实本机PITR，核对当前集群／控制库、postmaster及timeline、包／检查点摘要与指定异机签名；不停止SQLite源、不创建迁移目标或角色、不改原数据。该核对只输出范围明确的诊断结果，仍`deploymentAllowed:false`，不会改变`run`的既有就绪门禁或替代最终应用备份／候选恢复／单次激活。未配置新字段时原`run`行为不变，新增检查明确拒绝缺材料；撤销该命令与可选字段即可回退，无schema变化。任务内同机诊断始终不能取得正式异机资格。

中断后可独立运行 `node storage-operator.mjs cleanup /etc/harbor-storage-operator.json` 回收既有私有预检登录，不受 `run` 的发布就绪门禁阻挡。命令只读取受认证状态及原分配／角色OID见证，分别锁定并复核角色代次与pending激活记录，先提交撤销登录，再结束精确原租约会话；已提交的撤销不会因后续取消／回收失败而恢复；同名重建角色、已激活目标或缺少见证明确拒绝。外来会话保持运行并返回 `partial`，不会当作已完整回收；可在其自行结束后重试。无状态时不创建目标／注册表，不停来源、不推进迁移或开放部署，原 `run` 默认行为不变。回退此入口无需改库或删材料。

`withPendingStorageOperatorBackup` 仅在真实context及持续持有的来源写锁内提供私有回调能力，每次消费重新核对认证状态、实际数据库／角色代次、无登录会话、原行金额和材料；回调完成或关闭后失效，返回JSON不能延长许可。它仍标记完整候选配置与设备材料未验收，不替代完整备份／恢复、异机恢复或最终激活，正式入口继续blocked。

Docker迁移配置必须显式将 `data`、`runtime` 与 `secrets` 纳入文件备份，三根不得重叠；`data` 对应固定来源数据目录，`runtime.destinationPrefix` 为候选的 `/app/runtime`，原账号路径前缀仍可独立配置，`secrets.source` 必须等于批准的 `candidate.secretsDirectory`。缺失或路径不匹配在停源前拒绝，不隐式读取额外目录。设备TLS／CA、数据库CA与存储报告密钥需预先准备在该secrets根；候选只读挂载实际恢复出的三个子树，不再借用原secrets目录通过预检。该材料覆盖检查不等于证书有效性、完整恢复或部署资格；真实候选仍执行既有密钥、设备信任及数据库预检，无自动生成或替换原密钥。

PG发布准备见[操作前置清单](ci/postgres-operator-steps.txt)、[受控配置模板](ci/postgres-operator.example.json)及[数据库环境模板](ci/postgres-env.example)。默认仍为SQLite；只有管理员维护的root所有、0600服务器配置能选择`storage_engine=postgres`，客户端发布请求仍仅含action/image。发布前须完成对应schema、真实数据迁移、原密钥／账号目录恢复、数据库verify-full TLS、独立设备HTTPS材料及可信磁盘报告发布器准备；脚本不代建库、迁移、生成替代密钥或安装定时器。

PG候选和回退镜像均先实际检查源码清单、schema、数据库、原材料、新鲜存储报告及设备信任链；主密钥须在只读快照内分页解开现存员工及上游凭据并通过摘要核验，错误或缺失证明在写标记及停止旧服务前拒绝，不输出明文。同候选启动网关与独立Worker，并检查HTTP／HTTPS健康与鉴权、自然后台任务进展。首次PG启动前持久化存储模式标记，此后拒绝SQLite镜像及配置回退；失败保留新写入，仅实际兼容的上一PG组合可以恢复，否则保持停止。默认应用资源为网关3CPU／6GiB加Worker1CPU／2GiB，外部DB建议独立4CPU／8GiB，仅为配置起点。

CI在候选镜像之后构建专用PG18验证服务，再由候选镜像内`/app`代码验证真实数据库TLS、报告、独立Worker、设备mTLS及固定Codex0.160.1多轮／图像MCP链路，使用随机测试库、临时材料及受控上游；验证服务镜像禁止部署。源码与发布保护测试不替代实际候选CI，CI亦不替代正式数据规模、长时容量、RPO／RTO或真实付费上游兼容验收；部署作业继续为手动，本期不执行。

PG正文管理HTTP与独立Worker已接同一`recording-encryption.key`（32字节、当前用户、0600），不使用员工Key主密钥替代。网关仅在无旧密文／暂存的空安装创建材料，Worker从不生成替代Key；数据库绑定指纹前验证旧正文和有界暂存，错Key／缺Key明确拒绝。`npm run worker:postgres`默认启用正文任务，必须与网关使用同一`HARBOR_DATA_DIR`和数据库；仅维护型旧调用才显式设置`HARBOR_RECORDING_WORKER_ENABLED=0`。网关`HARBOR_RECORDING_ENABLED=0`只停止新采集，不影响转发、计量或旧正文保留；两个环境开关只接受0／1，重启生效。

健康PG的正文缓冲按既有字节和命令预算取有界前缀，直接在同一数据库事务中依序写入，确认提交后才移除队列；每次追加事件在请求行锁内批量查重与插入，保留原顺序、密文、截断及幂等语义。已有加密暂存仍先恢复，事务失败或提交确认不明保留前缀并退回原加密文件恢复路径；不增加内存／队列／磁盘上限，不更改暂存格式。未注入事务能力的旧缓冲仍按原流程运行。回滚不需要数据迁移，未提交的内存命令仍遵守原进程中断边界，不能视为已持久化；故障后完整性须另行核验。

正文管理入口统一在`/api/admin/recordings`，管理Cookie覆盖搜索、会话、分段、导出、重建及保留期预览／确认。HTTP与管理JSON分别有界排队，等待断开可移除；下载等待真实背压和完成，错误不能把不完整前缀作为完整文件，断开不取消后台生成任务。启用正文Worker后处理整理、加密索引、导出及清理，与夜间／员工任务共享租约；磁盘不足暂停index/export且不消耗尝试次数，保留单个purge恢复槽，总活动任务上限只增加该槽，普通任务上限不放宽。

使用数据的“正文管理”按需搜索近7天脱敏正文，可明确选择保留期内的范围；字面中文／代码搜索、会话关联与分段输入／回复／工具过程使用独立游标，不执行正文HTML。重整索引版本变化要求重新读取，正文或片段到期及时清除；只在可见且仍处理中时增量刷新，编辑、关闭、切页、隐藏与退出停止轮询，迟到读写不会覆盖新视图。

`GET /api/admin/recordings/exports?before=<游标>&limit=25`返回近7个北京时间自然日的`items/next_cursor`，默认25、最多100及64KiB预算；刷新后可找回任务，019仅追加历史排序索引。列表不读取正文／私有筛选，状态与后台jobs一致；保留期计划也返回真实job失败状态，避免永远显示排队。导出明确固定结构化条件，默认CSV不含正文，显式JSONL才带正文；关键词搜索命中集当前不支持直接导出，界面须明确确认，不能静默丢弃关键词。附件确认完整后下载；保留期先预览影响再确认固定版本，预览本身不修改期限，并发版本冲突或过期须重新核对，清理不删除计量元数据。

可信存储状态来自实际PG盘及应用spool的采样，模型路径只读缓存，不等待磁盘IO；管理查询也不隐式触发慢探针。当前严格同机入口`HARBOR_RECORDING_PG_DATA_DIR`须通过Unix socket、实际backend目录、PG版本与cluster system_identifier核验，并检查真实relation／tablespace／WAL所在文件系统，不能用应用盘冒充数据库盘。`HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES`默认1GiB、`HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO`默认10%，按每个实际卷检查，允许服务端调整，客户端不可覆盖。独立probe池查询超时固定1秒，不能被URL参数扩大；未知、过期、权限不足、远程无可信证明一律停止新正文采集及大导出，模型和核心计量继续。远程DB可显式启用下述可信报告模式；正式宿主调度和最终部署组合仍须验收，不能仅凭本机探针或签名接口存在宣称远程存储已受监控。

远程数据库使用服务端`HARBOR_RECORDING_STORAGE_MODE=report`及独立`HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE`；默认`local`保留严格同机核验，不能自动降级为只看应用盘。DB主机一次性执行`node postgres-storage-report.mjs`，从`HARBOR_STORAGE_REPORT_DATABASE_URL`读取专用发布身份，核对实际PGDATA／backend／control和全部正文关系／WAL后签发，不常驻、不输出报告身份或密钥。签名材料为两端各自用户所有、0600、非符号链接的独立32字节文件，不能替代正文或员工Key材料；代码不生成真实材料、不自动改权限。020只增加有界单行报告及数据库单调时间保护。

发布者需要schema USAGE、报告表SELECT／INSERT／UPDATE、`pg_control_system()`执行及读取data_directory的`pg_read_all_settings`，不授业务表／settings写入、DELETE、`pg_read_server_files`或超级权限；接收者只需报告表SELECT及`pg_control_system()`执行。具体角色由数据库管理员在目标库显式授予，迁移不自动建角色或授权；DB宿主OS仍须合法读取实际PGDATA／proc／control。接收端以当前数据库身份、DB时钟和HMAC核对来源、完整字段、每卷数据及TTL，并实时合并自己的data／spool卷；错签、错库／集群、未来、重放覆盖、缺盘、响应延迟越过到期均unknown，不刷新旧报告的采样期限。

报告默认15秒、上限30秒，由`HARBOR_STORAGE_REPORT_TTL_MS`控制；正式DB宿主需以短于TTL的可信调度持续调用一次性发布器，例如5秒周期。`sampled_at/checked_at/expires_at`分别表示采样、核验和到期，轮询不能延长旧报告。当前已完成私有PG的跨进程及真实第二集群验签、最小权限和恢复验证，正式宿主调度／材料分发尚未执行；关闭report回local后仍须同机证明，不会恢复伪健康。Worker和网关的监控都必须包含data与recording-spool实际卷，不能漏掉独立挂载的spool。

容量工具入口为`node tools/capacity/cli.mjs plan|preflight|selfcheck|seed|verify|run`，真实TLS自验需要OpenSSL；普通及PG两个CI测试作业均显式安装该依赖。`plan`只列正式标准；`preflight --manifest <受控清单>`核对真实进程资源、数据量、TLS／设备辅助及逐API绑定，缺证据返回不可执行。`selfcheck --private-pg --pg-bin <PG18工具目录> --pg-lib <库目录>`只验证工具及小规模私有PG／mTLS链路，不是正式容量通过。`seed/verify/run`必须由`--adapter <本任务适配模块>`创建受所有权约束的私有目标；错误数据库／集群在迁移前拒绝，禁止传线上库。结果用`--output <新的私有文件>`保存，凭据不进入测量回调。

内置业务资格入口为`run --profile selfcheck --adapter tools/capacity/gateway-adapter.mjs --pg-bin <PG18工具目录>`，需要独立库文件时追加`--pg-lib <库目录>`；`--windows-helper`显式启用真实Windows CNG／WSL桥，使用独立临时状态，不修改活动客户端配置。它贯通真实TLS、加密正文、管理操作库存及独立计量／内容对账，提交类操作另核真正的Worker结果；报告仅含摘要和统计，未知不写成零。`seed/verify`同样每次新建并在命令结束清理自己的实验库，不用于复用上一命令或校验线上库。`run`在同一实验生命周期内造数、验数和请求；当前内置适配器的正式能力仍未接齐，formal模式在创建实验资源前明确拒绝，CLI未给`--execute-long`更会在导入适配器前拒绝，不先灌大量数据再报条件不足。

容量运行可显式提供 `--resource-limits-file <JSON文件>`，通过现有 `--owner-entry` 和 `--delegated-cgroup` 路径交给 native owner；声明资源但未提供委派时拒绝，不把声明当实测。省略配置仍为网关／Worker共享4 vCPU／8 GiB、数据库独立4 vCPU／8 GiB。配置只包含 `gatewayWorker`、`database` 两角色的 `cpuQuota` 与 `memoryLimitBytes`，可附完整 `processLimits.gateway/worker` 及可选 `processLimits.database`；未附时叶子继承所属角色上限，并继续受共享组约束。CPU须能按100000微秒周期精确表示且不低于内核1000微秒最小quota，内存为正安全整数字节。非法值、缺字段、未知字段及超过角色的叶子限额在创建资源前拒绝。

`run/seed/verify` 的 `--disk-budget-file <JSON文件>` 沿相同执行链传入显式磁盘计划，固定包含 `dataBytes`、`metadataBytes`、`indexBytes`、`walBytes`、`backupBytes`、`temporaryBytes`、`growthBytes` 七项正安全整数字节；未知字段、缺项、零值或非法类型在导入适配器／分配资源前拒绝。实际formal执行仍按 `--storage-root` 核选定目录的真实可用空间、完整正文源大小及各项合计，不把计划当作全量实测。WSL等稀疏虚拟磁盘还须核实际宿主卷空间，不能只根据虚拟文件系统的逻辑余量开始全量造数。省略该参数不生成虚构预算，formal仍因缺证拒绝；既有selfcheck默认不变。

formal `run` 通过CLI和native owner时自动接通原有物理恢复阶段，不能显式关闭；`seed/verify` 及selfcheck默认不启用恢复。它让同factory的原Key、TLS、正文与设备恢复评估可达，不会自动安装runtime trust、批准拓扑或把其他运行的恢复收据当成本轮资格。正式数据量和性能门槛保持不变，其他前置缺失仍在造数前拒绝。

例如，按当前独立preview的实际限额复现实验，可使用以下声明（文件由执行者明确选择，不来自客户端请求）：

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

声明随报告保留；allocator只在自身委派后代写入角色、共享总额和叶子限额，启动、观测及恢复后的每个epoch均重核实际值、PID归属、祖先、CPU亲和性及组合可用配额。不改变宿主委派或既有服务。配置变化必须新建一次实验，不能中途放宽报告口径。`preflight --manifest`可在原清单中提供同名 `resourceLimits`，但普通JSON或配置本身不能替代真实cgroup证据。五分钟CPU／内存80%与所有接口、并发、时长、计价、126万／540万元数据及正文覆盖标准保持原值；较低配额通过与否仍取决于实际结果。此配置不会把CLI接到现有preview数据库，也不会补齐LAN、代理或不可变运行证明。


启用`--windows-helper`时，阶段测量消费自有辅助进程stdout产生的QPC事件，以事件前后真实时钟样本建立控制端时间上下界；不将QPC当作Linux纳秒，也不假设固定跨机时差。已登记actor、设备准入与最终结果交叉核验，网关／辅助程序／Node按trace、顺序、哈希、字节及内容类型三方匹配后，firstRelay／middleRelay采用辅助程序socket写完的区间；该点不代表客户端应用已读取。Node结束／取消及正文、用量可见性锚点保持原口径，原50／100ms和每组1000样本门槛不变。每trace观察FIFO及待处理数有界，缺来源、超时、丢失或身份冲突不回退Node充数。认证请求的`probeCreated:false`终态只说明辅助程序未建立SSE探针，不能证明上游未派发。不开启参数时保持原Linux测量链；正式helper／clock来源、LAN／部署入口及资源证明仍须完整接线和验收。

私有证据账本使用独立SQLite WAL／NORMAL，避免逐事件FULL刷盘阻塞控制程序；生产业务PostgreSQL配置不变。工具构造参数`maxDiskBytes`须显式配置且至少256KiB，`checkpointPages`默认128、合法1–4096并随磁盘预算缩小。生成对账报告、`flush`及`close`前执行FULL／TRUNCATE检查点，核busy和页数后才标记durable；实际与峰值空间包括DB、WAL、SHM。检查点或预算失败永久保留缺证，controller将不可置信计数置null。控制程序崩溃后不续用本次实验；回滚旧工具须先结束实验、清理自有资源，再重新造数，不能复用未完成持久屏障的材料。此策略不是主机断电持久承诺，也不代替正式容量验证。

容量探针通过真实处理器返回的Promise产生`gateway-handler-settled`，按trace、wireId和规范化路径关联；连接提前关闭时仍等待实际处理器收尾，不能凭socket事件认定事务回滚。无法观察Promise时明确发出缺证，后台job的最终业务结果仍需独立核验。该事件只用于请求处理器收尾，不证明模型全部资源已释放；事件不含正文或凭据。私有sink按同步合同调用，异步或异常结果计为缺证而不改变请求结果。业务入口仅透传已有处理器收尾Promise，鉴权、路由、事务及HTTP输出保持原规则。

私有容量实验的PG透明代理由任务自建的独立Node helper转发，避免与controller同步SQLite观测账本共用事件循环。仅连接同一owned目录下已核验属主的Unix socket或固定loopback目标；保双向背压，不解析、记录或重放PG报文。启动、断连、恢复、恢复点切换和关闭均等待helper真实ACK，故障窗口在断连ACK后计时；关闭还等待连接、监听器和helper实际退出，父进程退出则helper关闭连接自清。源码摘要、PID及任务运行时能力校验不由外部DTO自证；异常或控制结果不明明确缺证，不回退共环代理或自动重启。helper CPU、RSS及cgroup单列为测量设施，不能算作额外DUT配额，也不补齐正式LAN／代理入口门槛。该内部异步合同回滚需同步调用方，无数据格式迁移，不代表正式容量已通过。

`remote-launcher.mjs`与`remote-transport.mjs`提供容量控制通道的隔离模块，当前仅监听／连接loopback，不是已上线的远程服务，也未接入正式factory。固定双向证书及对端证书摘要认证，只执行预设目标、方法和路径；请求不能提供shell、任意地址或TLS覆盖。帧、并发和缓冲有界并为取消预留空间，单次会话拒绝重放；取消ACK不等于请求终态，断链保留投递未知且不重发。浏览器现支持固定profile下的受限场景RPC，复用本机同一core与五管理员／五视图／各分页变体断言，不接受任意URL、脚本或命令；每个请求须取得控制器真实trace ACK才放行，混合负载前后读取实际上游active，未知不补零。认证通道只证明受限端点身份，本机launcher另有owned子进程证明；对端自报PID或源码摘要不能证明跨机归属、不可变运行、时钟、真实LAN或部署代理。关闭／租约到期清理自有执行资源，缺失确认仍报unknown；该模块无默认服务开关，不修改业务请求或数据库格式。

`createCapacityRequestDriver({remote,...})`现可通过已认证的固定slot／alias执行JSON、SSE、二进制和CSV下载，消费实际响应头及阶段事件；控制端不能提供新目标地址、私钥路径或命令。结果保留执行端epoch、原始时间与前后采样边界，原控制端时间字段明确为空，不用固定偏移伪装同钟。响应头仅走私有回调，结果只保留头部收据。异步观察从调用前占位到真实settle始终受全driver计数及字节预算控制；正常关闭等待回调，超时／取消保留unknown和占用，重试不累积等待链。普通关闭排空已接受HTTP，取消ACK不代替终态，断连不重发。完整远端浏览器场景与请求放行ACK已在受限认证链实际验证，异步观察、快照与关闭结果均受整组driver预算约束；取消／stop后仍等真实回调settle，背压丢失可见，终态快照缺失不等于资源未释放。浏览器连接失败仅在server关闭确认后释放槽位，启动中断保留unknown及恢复目录。正式factory的allocation／pre-seed／final／restore远端消费、大请求分片及正式账本时钟消费仍未完成；真实LAN、不可变运行与原部署代理仍需验证，`formalQualified:false`保持。

设备helper owner在同一factory的pre-seed阶段，用原有合成员工执行真实CNG登记、Key设备范围收窄及TLS模型目录挑战，不发模型请求；临时CNG／证书、Key范围和精确登记清理全部完成后才seed，审计保留。数据库超时与提交回执丢失可重试，不删除其它设备；final／restore仍要求原模型验证与原身份续用。正式helper与独立helperClock producer核固定root受信C#源码及预编译程序，QPC到controller仅提供因果时间包络，不能替代remote Linux时钟、TPM证明或完整正式资格；默认本机selfcheck仍现场编译，未安装宿主信任配置。

控制通道可协商 `clock-sampling-v1`，显式调用 `clockSample({timeoutMs})` 采集两端真实单调时钟的四个时间点；默认期限1000ms、允许100–30000ms，旧端点未声明能力时只拒绝采样，不发送新消息或影响普通请求。样本私有身份绑定run／TLS会话／证书和端点clock epoch，复制DTO、外部时间函数、坏时间／时钟回退及断链不能保留有效证明。`clock-mapping.mjs`仅根据事件前后真实样本的因果与单调顺序，给出控制端时间的保守上下界；不假设两机时钟同速、不取往返中点、不外推未夹住的事件。窗口默认1024个样本（2–100000）、保留30000ms（100–600000），按需采样，不新增全局定时器；关闭立即撤销并等待自己尚未完成的有界采样。事件时间输入仍须由后续真实来源核验，返回明确 `eventSourceVerified:false`、`formalQualified:false`；当前loopback证据不等同跨机、LAN或正式clock消费桥通过。回退撤销该可选诊断协议及映射模块，无数据库或服务配置变化。

`preparation-context.mjs`提供独立的本机selfcheck准备原型：由自身唯一manifest创建私有PG及最小网关／Worker，只执行一次真实TLS `GET /ready`，结合私有IPC的同trace／wire开始、响应结束和处理器收尾签发有限收据。检查／消费在该次校验窗口前后重新核各角色内核状态、启动身份及SQL／IPC／TLS响应，副本、跨context、重复消费或缺证均拒绝；不生成请求正文，不允许正式造数。Linux入口须先显式调用`buildCapacityPreparationEntry({directory})`，用现有`/usr/bin/cc`在管理员持有的空目录制作固定Node／模块的subreaper工具产物；目录与binary为0700、清单0600，制作中断的产物不能运行。`createCapacityPreparationContext({entryDirectory,...})`仅验证预构建文件的属主、权限、类型、非符号链接及实际摘要，不在运行期编译或创建native目录；工具产物独立复用、更新和清理，校验摘要不证明不可变运行。PG组长先退出时，内核将孤儿backend交回当前owner，再按真实直接子关系、已核PG可执行文件inode和进程启动身份恢复／清理；controller退出也须等待实际PG及后代停止。构造项`livenessTimeoutMs`默认1000ms、合法100–10000，`closeGraceMs`默认5000ms、合法100–30000；关闭在等待初始化前即可撤销，预算到期终止自己的实验PG。此诊断收据不能用于另建的target；真实LAN、部署代理、不可变产物、跨机时钟及设备辅助程序等证据仍缺失，不代表正式容量通过。

`factory-context.mjs`已接入实际`gateway-adapter.mjs`：同一canonical manifest、私有PG、store和密钥先启动最小网关／Worker，核验只读TLS就绪收据；撤销收据并停止准备进程后，单次执行种子及完整验证，最后启动新epoch的最终进程并重新校验就绪，才交给原runner。诊断收据和JSON不能代替这个私有阶段。`runCapacityOwnedFactory`沿同一预构建native入口，在固定owner内创建target、导入实际浏览器launcher及执行runner，父进程只负责取消、有界进度和最终报告；builtin正式CLI需`--owner-entry`，selfcheck提供该参数可走相同owner链，未提供时保留旧直接入口。`formal-proof-catalogue.mjs`列出runtime／remote／LAN／proxy／clock／helper的固定`allocation → pre-seed → final/restore`合同；factory/owner来源由固定native入口的私有registry与context内私有binding双重核验，冻结对象、外部回调或复制DTO不能成为来源证明。allocation不证明后续实测，缺producer、真实启动／TLS／远端driver／browser／helper／clock消费桥明确列入`softwareMissing`并在正式造数前拒绝。目前仅真实小规模链闭合，不代表正式环境准备或正式容量验收完成。

原代理／局域网预检已接入上述factory的当前epoch：保留管理员已批准的原Nginx stream TLS透传适配，核对实际binary、master／worker代次、配置及include／module与原会话端点；磁盘配置摘要默认只证明disk-only，不等于进程已加载。既有`capacity-formal-topology-v1`的`proxy`也可显式配置`{type:"direct",publicOrigin:"https://<批准主机>:<TLS端口>",listenAddress:"<本机具体IPv4>"}`，复现实际无转发层的设备TLS入口，不新建Nginx。省略direct类型继续沿原Nginx校验；direct仅允许当前factory自己创建的GW在同一netns的具体本机非loopback IPv4直接监听，核当前PID／startTicks、唯一socket inode、客户端实际目标／CA／证书指纹及同epoch trace／wire处理器挑战；共享端口、监听换代、地址或端口映射不符及跨netns均拒绝，不能指向另一部署或用JSON声明通过。TLS监听地址只取上述批准配置，HTTP管理监听仍限loopback；direct不提供DNAT／跨netns端口映射适配。报告明确入口类型与是否实际存在代理，不把direct写成测过Nginx。TLS、路由、mTLS、流式及取消仍按实际测到的属性分别记录，未触发的设置继续unknown，不安装或重载现有代理。LAN另需批准的网段／接口边界、双方原生实际选路和实际客户端socket端点一致，direct复用现有原生客户端与同GW处理器证据，不增加连接日志；不能仅凭不同IP、低RTT或TLS成功认定同LAN。原生观察代码按批准字节核验后执行，关闭等待其真实进程退出。恢复后先完成当前driver、Cookie与进程epoch校验再发布新snapshot，旧driver或旧网络结果不能贴到新进程。新建或主动关闭的TLS连接在同一原生socket仍有效时保存端点证据，结束时核对socket身份后消费，避免关闭后字段丢失；不接受请求声明补齐。回滚可撤销direct配置使该路径拒绝预检，或回退本次harness；没有数据库迁移或线上服务变更，TLS材料及远端授权要求保持。

远端控制／驱动／事件时钟已增加独立根签名、mTLS控制与数据通道、私有租约及释放核验；TLS只复制本lease实际使用的白名单材料，拒绝混入签名私钥或额外编码内容，清理unknown不能冒充已释放。事件经有界接收、内容及次序核验进入原阶段账本，缺采样或时钟区间过宽仍保持unknown。动态browser activation只接受root预先批准的origin、SPKI和预算模板，运行期仅更新当前epoch与五个管理员cookie；缺模板保持关闭，换代先清理旧会话。正式模型负载使用同factory的Windows helper真实请求、QPC及阶段账本，远端driver单独承担异机browser／LAN链，不要求Windows helper运行在Linux机器。normal本轮私有收据在同轮UI身份刷新后有效，下一轮normal或恢复换网关后失效；UI完成后才汇总实际消费，readiness与复制DTO均不能代替执行。相关真实Windows、PG、TLS和Chromium组件已有定向验证；新候选与harness的root批准、正式异机／LAN及完整规模与长时场景仍未完成，不能据组件通过宣称formal验收通过。

容量工具的`capacityAgent`由原生入口和本地独立驱动共用，`timeout`构造选项默认沿用已有HTTP预算`600000ms`；省略或显式`undefined`采用默认，显式`0`及正值保留。正值使Node能够根据对端`Keep-Alive`提示及自身安全余量提前回收空闲连接，未提供有效提示时沿用Node的Agent行为。该设置不改变每个请求的Abort期限，不增加重试，也不是未派发证明；活动流跨过socket计时仍按原请求期限执行。新TLS与连接复用继续依据实际握手和复用证据分别测量，不能因连接池配置就认定全部请求复用了连接。回滚仅撤销此工具默认，无数据库或服务器配置迁移。

正式资源底座位于`tools/capacity/resource-allocator.mjs`，仅面向Linux独立压测环境，需要预先委派给当前用户、支持`cgroup.kill`且已向下启用cpu／memory／cpuset的cgroup v2目录及`/usr/bin/cc`（gcc与libc开发头文件）；普通CI安装编译依赖并验证原生入口拒绝路径。底座只创建自己的随机子组，不修改委派目录现有配额、控制器或进程；网关／Worker共享4vCPU与8GiB，数据库另占4vCPU与8GiB，并核算祖先扣除其他任务占用后的剩余内存。每次启动另有私有子组，原生入口先入组再执行程序，后代继承配额；组长退出后按该私有组清理遗留后代，不盲杀可能复用的数字PGID。`maxChildGroups`仅由工具构造配置，默认128、合法1–10000，限制单次底座生命周期内启动尝试；客户端请求不能覆盖。角色观察另核实际存活进程及有效祖先资源，角色暂缺保持未知，替代进程可重新核验。普通目录、错误身份或配额拒绝执行载荷；关闭只处理本底座启动的进程和自己的空组，异常材料保留报错。正向内核测试须显式提供`HARBOR_CAPACITY_DELEGATED_CGROUP`及足够资源，缺失明确跳过；本机拒绝不能充当隔离成功。该模块尚未接入内置正式factory，磁盘预算、代理与局域网等门禁仍保留，不是正式容量通过证明。

正式`run --profile formal --execute-long`同时要求资源／入口／数据前置证据及完整真实适配器，不能缩时长或降低门槛冒充通过。追加`--browser-launcher <受控本机模块>`，模块导出`launchBrowser()`，每次返回没有既有context的全新真实浏览器；并显式填写`--ui-max-duration-ms <毫秒>`作为独立UI阶段上限，不默认借用原8小时。缺少、错误或不足样本基础预算的配置，在创建实验资源前拒绝。原API场景保持独立，结束后另跑浏览器附加载荷：10QPS模型与5QPS管理基线外叠加5名管理员的页面操作。当前11种页面／分页组合分别至少1000成功样本，默认每秒1次页面动作，成功样本下界约3.06小时；翻页到底等跳过不算成功，需要另计，达到全部样本和身份覆盖后才停止发压并排空，上限内未达标则未完成。准备和收尾另计，不挤占原8小时。报告分别保留原API结果和UI结果，UI通过不能覆盖任何原API失败或其他缺证；附加阶段异常或取消仍保留原报告和已确定的失败。不提供浏览器的selfcheck仍保持原资格输出。

正式运行还须显式填写`--api-max-duration-ms <毫秒>`（1–86400000），在原场景之后、UI之前独立补齐接口／变体覆盖；缺少或非法值在导入分配资源的适配器前拒绝。该阶段保留10QPS模型及5QPS管理背景，准备和测量各自记账，默认另以1QPS执行预备好的真实操作。每种有效变体保留正式最低样本与最严用途预算；仅完整正式补覆盖结果可补原报告中同用途／同变体的接口缺项，原场景的模型、管理样本及已确定失败不被替换。达到上限、证据不足或取消保留未完成，报告单列`phases.apiCoverage`并保留原API与UI结果。selfcheck默认不启动此附加阶段，显式传参才开启；内置factory、阶段源及正式资源仍须完成各自接线与验收，配置时限不等于容量已达标。

正式运行还须显式填写工具总预算`--run-lifetime-ms <毫秒>`，程序化`createTarget({runLifetimeMs})`与`runCapacityExecution`沿用同一计时，不能在准备后重开。总预算必须严格大于全部原场景时长及API、UI各自声明上限之和；差额单列为准备／排空／收尾余量，不表示各阶段原上限包含这些等待。计时从目标前置检查及准备开始，截止只通过既有取消信号停止继续工作，等待实际回调和清理完成；执行或CLI清理逾期均不能报告正式通过。合成CA、服务器、工具客户端及真实设备叶证书统一按总预算加X509一秒精度余量向上取整为有效天数，工具最长总预算为365天减1秒；不配置的selfcheck仍用1天，不改变产品设备证书默认30天。开压前、阶段切换、新候选设备及恢复后检查真实X509全链和剩余预算，准备耗尽余量或任一证书覆盖不足明确拒绝，不自动续CA或关闭TLS。

完整验收覆盖目标数据、60员工与5管理员、各API分位、长流／120并发／突发、8小时稳态、重任务干扰与故障恢复；逐场景校验实际身份、样本数、取消及正常释放端点、资源采样缺口和独立金额对账。只有工具自验、空对账或一致截断都不能通过正式门禁。当前本机缺独立资源及最终正式适配证据，前置检查不可执行；正式数据造数、长时负载、镜像及异机恢复仍须继续完成，本工具提交不代表这些已经达标。

受理观察在最外层数据库事务确认提交后发布，保存点／外层回滚不能被统计为已受理；事务确认异常保留未确认事实，不能据异常断言已回滚。已取得的正常及浏览器阶段实测不会因后续取消、查询或清理异常而丢失，已有确定失败优先保留；最终资源清理失败仍输出报告、标记失败并以非零状态退出，错误只保留安全代码。

PostgreSQL请求受理事务内对当前员工和Key加共享行锁，模型权限／限额检查与请求入库之间的配置不能被并发改写；提交后释放，不持有到模型执行结束。已受理请求继续完成，随后请求使用已变更权限。首次请求体读取前的鉴权仍为普通读取，避免长时间上传占用这些锁。

`postgres-member-limits.mjs`提供共享限额异步适配器，沿用原规则校验、模型归属和精确金额，按员工完成时间索引在SQL中聚合滚动24／168小时用量，仅返回每条规则的汇总。全部Key共享，缺计量／费率／计价投影保守阻断对应模型；已知零消耗不阻断。配置保存和审计同事务、版本冲突返回409。隔离Responses HTTP测试已接入该适配器；并发已受理调用仍可完成，不预占未来USD。日常默认SQLite入口未切换，完整生产路由及管理页仍在开发。

`postgres-routing.mjs`提供Responses会话路由适配器，与旧服务共用自动权重、API人工占比和持久轮转计算。`resolve`必须位于受理事务内；事务级路由锁覆盖账号选择、轮转记账与员工隔离的会话绑定，模型执行前释放。同客户端并发只建立一个绑定；正权重会话续用，零权重／登录失效转移仍检查在途保护。临时权重到02:00失效；人工正比例不能替代独立额外额度许可、有效权益及预算，人工零比例在受理和最终派发时均核验，API固定会话保留独立人工绑定。`block`使用账号代次拒绝旧错误证据；处理器通过`onRouteFailure`显式接入，失败请求不重发。结算同步累计账号用量，只有绑定版本吻合时更新会话状态；员工先于Key加锁，避免与受理互锁。已接管理写入、额度恢复和夜间Worker，断库／失锁时停止接纳及不重放；套餐容量统一口径与正式默认入口切换仍须继续完成。

处理器对员工收包阶段维护引用计数，通过路由回调第四参数`{preparing}`传入；多个上传只完成一个时仍保留保护，读取完成、失败或断连均清理自身计数。`resolve`同时检查该计数及外部准备状态，不在其他请求仍上传时换绑。自定义路由包装须透传第四参数；这不延长锁到上传或模型执行阶段。

当前新存储已覆盖原Key哈希鉴权、短事务请求计量、固定费率和精确计价事实；结算与计数同事务，重复结算不重复计量，旧负载的投影修复会重新锁定最新请求。正文／事件及持久任务表已建立，采集、请求详情和员工管理已接入开发应用；Worker及其余管理功能继续开发。

正文开发链路已添加旁路采集、加密事件仓储及有配额的加密持久缓冲。本轮采集覆盖可识别的末尾用户输入、function／custom工具调用与回传，以及公开reasoning summary；无法确认本轮时标为不确定，未支持可见类型明确标为部分缺失，不因模型成功而标为内容完整。custom命令／补丁按原始文本脱敏，不强制转换JSON。隐藏reasoning、不透明加密reasoning和附件原始二进制不保存。首次指令／上下文基线、增量及工具配置的可复用存储仍在实施，当前版本不声明全量工作审计已完成。流式片段与完整输出按字段合并；采集副本过滤凭据，不改推理原文。事件采用AES-256-GCM及请求／序号绑定，正文默认保留30天（仓储配置支持1–365天），每请求最多2MiB、单事件128KiB，超限明确标记截断。过期读取不返回正文，实际保留期清理Worker仍待实现。

上下文开发接线默认关闭，服务端 `HARBOR_RECORDING_CONTEXT_ENABLED=1` 或构造参数 `recording.contextEnabled:true` 显式开启，重启生效；环境变量只接受0／1，构造参数只接受布尔值并优先于合法环境值。开启要求已核验的结构23，默认迁移仍停留22，不自动改库。Responses、旧Chat及平台Chat在认证、受理和正文初始化后提取实际收到的上下文，沿原存储许可和加密缓冲写入；命令封装计入同一容量预算。上下文失败只标记正文不完整，不关闭普通事件采集、不改变模型结果或重复结算。图像等JSON通道继续使用原结构化摘要采集，不将其伪装为对话基线。

关闭上下文开关只停止新增快照；已知结构23仍恢复遗留上下文命令并保留已存数据。过期来源只确认丢弃，不复活正文；来源不明、密文损坏或未知命令继续保留并明确失败。管理员Cookie可按需访问 `GET /api/admin/requests/:id/context`，使用 `after_component`／`after_part`／`limit` 分页，读取受原查询队列及密文字节预算限制；员工Key无权读取。结构24提供Worker索引、受控搜索及带有序引用的JSONL导出；上下文恢复校验已有实现。正式启用、恢复、容量及设备组合仍须实际验收，不能仅据接口存在宣称完整上下文交付。

附件元数据缺失时明确为`null`／`unknown`，大小区分`client_declared`与`received_bytes`。显式开启的上下文采集可对预算内、规范Base64实际解码字节计算`hmac-sha256`指纹，密钥与服务端请求ID限定作用域；同请求可核对，跨请求不用于关联文件或员工。整请求共享附件解码预算；引用、非法／不支持编码、超限只保存不可验证原因，不下载URL，不采信客户端摘要，不保存原始附件或Base64。文件名和MIME来自声明，MIME不代表内容格式已验证；实际字节长度与声明长度分开保留。没有新增开关或迁移，关闭上下文时不新增这类摘要。

采集覆盖新增可选`redaction`事实：固定策略版本和实际观察到的凭据、敏感字段、私钥、附件省略类别，最多四类，不保留敏感值、位置或原文长度，不推算次数。类别仅描述已扫描部分；空数组不能证明被截断／未支持内容没有敏感信息，历史无此字段按未知处理。Responses、旧Chat与平台Chat均将覆盖边界沿原加密缓冲保存，失败出口也保留已采集事实；业务结果、采集完整性与持久化序号分别读取。无schema迁移，默认22及上下文关闭行为不变；已开启23／24并产生新覆盖后，回退及恢复工具须保留可选脱敏字段的校验能力，不直接换成不识别该字段的旧工具。

上下文支持Codex内联`additional_tools`工具目录及嵌套namespace，保留输入位置、工具schema、调用／回传namespace和消息phase；这些仅是收到的协议元信息，不作为权限或执行成功依据。`output_text`的引用与概率元信息按类型过滤：保留标题、文件名、位置和概率，引用URL／文件标识省略，实际概率token按同一消息跨正文块连续脱敏，互斥候选token仅保留脱敏标记及概率，原始token字节仅保留省略事实及元素数，防止从数字字节还原凭据。未知字段、非法结构和超出预算仍明确不完整，不通过忽略原因伪造完整记录；本次不放宽容量限制，不改原转发请求、调度、计价或schema。真实CLI合成多轮协议纳入候选检查，普通事件同时保留namespace和终态phase。

管理员显式迁移使用`npm run db:migrate -- --recording-context=storage`（23，仅对象存储／读取）或`npm run db:migrate -- --recording-context=index`（24，含存储及索引）；无参数保持默认22，已到23／24不会降级，参数未知或重复直接拒绝。迁移与开启采集分开执行，不由启动、HTTP请求或夜间任务触发。先安装匹配的网关／Worker及备份恢复工具，保全可验证备份，再在独立维护步骤显式迁移。宿主受保护的`secrets/postgres.env`可选`HARBOR_RECORDING_CONTEXT_ENABLED=0|1`，缺省关闭，重建服务生效；预检在结构不足而开关为1时拒绝，不能进入停源／切换。同票据重入须核对实际容器配置，不以健康代替开关生效。关闭采集用0并沿原发布流程重建，不删除数据或降级schema；回退宿主工具至不认识此字段的版本前先移除此可选行，且业务／恢复工具仍须认识实际23／24及已有覆盖格式。

部署在原有`pg-data/settings.json`内维护内部布尔许可`recordingContextAllowed`，缺省兼容原启动行为，false只否决新增上下文采集，不影响普通正文、计量、旧缓冲恢复和已有上下文读取。开启或关闭时，在停止旧组合前原子保存许可并保留其余配置、属主及权限；关闭失败时，可恢复的原容器即使保留Env=1，重启后仍遵守false许可。旧镜像须通过真实预检声明`context_capture_guard_contract=v1`才支持此关闭回退；未支持的已开启来源在停源前拒绝，不能冒充安全回退。开启失败允许回退至原先关闭的组合，并在恢复原容器前保留原false许可；许可恢复无法确认时保留现场，不启动可能重新采集的旧配置。此字段随原应用材料备份恢复，不新增用户配置入口；回退到不认识许可的业务代码前，必须确保其环境采集开关为0，不能以旧Env=1容器替代。

已完成本机保障迁移的环境保留原连接配置摘要：`check`仅允许新增或移除一个合法的上下文0／1开关，数据库URL和存储模式等原始字节仍须匹配加密状态，拒绝重复开关、未知配置及原连接变化，不重签旧状态。受限运行账号没有建表或未来表授权；管理员须先从固定root配置、加密迁移状态与实际容器核对库OID、所有者和运行角色代次，再调用维护模块`ci/upgrade-recording-context.mjs`的`upgradeOwnedContext(store,binding)`。该操作在一笔事务内锁定原角色、以原所有者显式迁移到24，仅给原运行角色新增上下文表的DML权限，并以该角色核对可读与原DDL禁令；授权失败整笔回滚，不启用采集、不停源、不改变角色属性或成员关系。此模块不是HTTP入口，不能接受客户端提交的binding代替管理员核验。

Responses请求详情新增`streamTiming`，以同一进程单调时钟区分请求体读取、派发前等待、上游响应头／原始分片／解码事件／可见内容、下游socket写入和实际HTTP结束。常规请求只保存一次有界汇总，不逐片写库、不保存正文或账号；客户端不能开启逐片trace。上游读取按下游消费节奏推进，分片间隔可能包含前片写入／采集／解码等待，不能直接等同于模型生成停顿。socket写入回调不证明客户端收到，缺失观测点返回`null`及固定原因码，不将整轮`queued → ok`解释为排队。摘要在HTTP finish／close和业务收尾均完成后进入原有加密有界恢复队列；重复重放只补充终态元数据，不重复计量。无schema迁移，旧版可忽略此可选字段；关闭观测需回退业务版本，现有转发、调度及计量口径不变。固定速率HTTP回放用于比较直出和经过真实transport／writeOutput的转发、背压及取消；真实上游配对数据仍须发布后取得，不能据模拟回放宣布已消除现网慢出字。


`postgres-account-admin.mjs`已接通开发入口的订阅／API账号创建、Key加密更新、人工API占比、启停、移除及检查。占比校验与审计同事务，创建失败回滚账号并清理本次空目录；检查按账号代次去重、独立限界排队，换Key或移除后的旧结果不覆盖新配置。默认最多同时检查2项、等待8项，排队和网络各15秒；构造参数`accountChecks`沿用查询队列的合法范围，重启生效。API余额不可得时明确提示官方未提供，不推测余额。

停用在同一路由事务中分批迁移全部绑定，每批256条，仅取必要字段；无可接收账号或中途审计失败整组回滚。正在执行的Responses继续原账号完成，后续请求使用新绑定，旧结算不能覆盖新绑定版本；旧文本会话清空上游上下文。API人工权重0不迁移固定会话，停用才迁出。移除账号覆盖全部queued／running请求的保护（补齐原入口未覆盖的Responses保护），保留目录及历史，仅清除平台保存的API密文。004迁移仅增加按账号遍历绑定的索引，已应用迁移不改写。其余平台管理仍须接通，不能将基础管理适配视为完整账号功能或正式切流。

`postgres-account-login.mjs`接入原官方登录器、设备码读取及取消接口；每个网关最多一条活动登录，同账号重复提交复用流程，不持有事务等待授权。开始前拒绝在途账号、清旧额度快照并增加账号／会话版本；官方授权后再核验登录，结果按账号代次应用。取消、停用、移除及关闭终止等待，旧核验结果不重新启用账号。重启把遗留login状态改为offline并记录中断，允许人工重试。设备码只保存在内存并返回已认证管理员，不写审计；错误不保存上游原文。API／平台账号拒绝订阅登录，远程Claude浏览器登录限制保持。额度读取、凭据刷新和CLI完整资源隔离继续实施，未进行真实订阅重新授权。

PG开发入口通过`postgres-platform-admin.mjs`接通原平台账号管理：`GET /api/admin/platform/providers`、`POST /api/admin/platform/accounts`、账号凭据`PATCH`及`/check`、WorkBuddy的`/login`与`/login/cancel`。Qwen／DeepSeek使用官方Key，Qoder使用PAT与服务端官方CLI，WorkBuddy使用原官方浏览器授权流程；此处只复用现有适配器，不新增登录协议。独立凭据仍采用原上游账号域加密，管理员DTO不回显；创建／更换与审计原子提交，变更后须重新检查。

平台模型检查与WorkBuddy过期令牌刷新共用账号检查队列，沿`accountChecks`默认2并发、8等待和15秒排队期限；检查／刷新按账号代次去重，上游调用另有15秒取消信号。刷新在取得模型检查槽之前完成，不嵌套占用同一槽；队列满或等待超时不将原就绪账号标为掉线。WorkBuddy最多一条活动平台登录、10分钟到期，同账号重复操作复用挑战；授权完成后仍须检查模型，凭据与ready状态一起写入。停用／移除／取消／重新登录及关闭阻止迟到结果恢复旧账号，数据库事务不等待上游授权。无新schema或客户端可覆盖的资源开关，回退不改写历史凭据；旧版本不具备PG平台管理能力。实际浏览器与HTTP验证使用合成账号、受控上游，尚未新增真实平台授权，不能把受控验收当作真实平台可用性结论。

PG开发应用现已通过`postgres-platform-service.mjs`接通`GET /v1/platform/models`与`POST /v1/platform/chat/completions`。原员工Key鉴权，模型目录按该Key权限过滤；纯文本消息仍最多128KiB、100条，无附件／工具，模型使用`平台/实际模型ID`。新会话在同平台已检查且支持模型的账号中按`last_used`及ID排序选择，后续保持账号绑定，每轮由客户端发送完整历史。受理和派发分别核对Key、模型、共享USD限额与绑定／账号代次，凭据刷新在事务外等待；配置变化则执行前拒绝，不静默更换模型或重放请求。共用全局／员工并发、准备交接、在途与排空保护，仍不将旧Chat预测容量策略用于平台通道。

平台请求以`source_channel=platform`保存实际模型、阶段、计量及参考USD，未知计量不记零；断库成功暂存返回`X-Harbor-Usage: pending`与空参考用量，重启只恢复结算。只采可识别本轮输入及可见回复，不复制旧历史；WorkBuddy逐块解析有界上游响应，UTF-8分片完整，旁路采集可保留中断前缀且回调异常不影响推理。Qwen／DeepSeek仍解析完整JSON，Qoder仍使用隔离、禁工具的官方CLI；员工侧SSE沿原行为完成后返回，不冒充逐Token流。采集关闭不影响计量。默认SQLite入口未切换，全局字节／账号／CLI资源预算仍须继续实施。

`GET /api/me`已接通PG只读员工查询，使用同一有界查询队列与一致快照，返回当前员工及当前Key的七日Standard参考USD、模型权限与共享限额；Cookie管理登录不能代替员工Key。身份只来自鉴权，不接受查询参数指定员工或Key；投影与价格版本不一致显示缺价，不借用错误金额。此接口补齐原测试页的发送前身份检查，不向员工开放管理员正文查询。页面刷新只清空本地显示，不自动恢复聊天历史；管理员采集记录不等于客户端续聊上下文恢复。

PG开发应用的`request-body-budget.mjs`为Responses／图片、旧Chat、平台对话、管理及登录共用同一请求体字节账本。仅服务端`options.requestBodies`可配置：`maxBufferedBytes`默认512MiB（256B–8GiB）、`maxBufferedBytesPerOwner`默认全局预算与384MiB的较小值（至少256B且不超过全局）；`maxReaders`默认16（1–128）、`maxReadersPerOwner`默认全局读取槽与8的较小值（1至全局读取槽）。384MiB对应单个128MiB请求的解码缓冲加最坏UTF-16文本转换占用。配置重建应用生效；无客户端开关，不通过修改单请求协议上限绕过预算。

推理先鉴权，再按可信员工ID合并全部Key的占用；管理与登录各用独立内部归属，外部请求头／正文不能指定归属。已知Content-Length在读入前预留，分块上传逐块计量；解压块、拼接副本及UTF-16转换在保留／分配前检查预算。读取／解析完成即归还读取槽，但按解析前序列化字节数保留payload占用，直到整个处理返回或失败；双层收尾幂等释放。预算不足返回503 `request_body_busy`并提示稍后重试，不排无界等待、不截断输入、不产生模型重试；单请求128MiB、100倍展开比和原413／400错误保持。状态只返回计数、已预留／占用及峰值、上限和拒绝次数，不返回归属标识或正文。默认保护不设置长任务总超时，不修改调度权重。

这里的字节账本不等同于进程RSS／真实JS堆：对象结构、GC、网络内部缓冲、上游输出及采集队列另有开销。输入、输出与CLI分别限界，正式容量仍继续实施，不能把字节账本通过当作60人容量结论。日常SQLite入口沿原读取行为，回退不需数据迁移；回退到没有预算的PG版本会撤掉这层资源保护。

PG开发应用将全部请求体的JSON解码／解析交给独立`json-parser.mjs`线程池，管理与登录也不例外；不按字节阈值猜测对象复杂度，不在失败时同步回退。仅服务端`options.jsonParsing`可配置：`maxWorkers`默认2（1–8）、`maxQueued`默认16（0–128）、`waitMs`默认15000（100–60000ms）、`timeoutMs`默认30000（100–300000ms）、`heapMb`默认512（64–2048MiB的单线程V8老生代上限）、`rebuildMs`默认5（1–25ms的主线程重建让出预算），重建应用生效。等待／执行期限仅约束解析阶段，不限制模型长任务；解析线程不继承环境变量和启动参数，不读取业务配置、不打印原文或异常细节。

线程按需启动、空闲复用；每线程最多一个任务，等待仍占原请求体预算。解码后的缓冲使用独占ArrayBuffer转移，不转移Node共享Buffer池；原始字节长度在转移前保存，不能因Buffer已脱离而把占用误记为0。原始Buffer池输入只复制自身范围，并计入临时副本；线程内把JSON容器编码为扁平节点／边，回传不依赖原对象嵌套深度；主线程迭代重建并按预算让出事件循环，不添加隐式深度上限。原型敏感键用自有数据属性恢复，保留数组、数值和Unicode语义。取消、执行超时或异常退出不重跑任务，等待线程实际退出及重建收尾，再归还线程槽／补建；无界排队和同步回退均禁止。优雅停服允许已接纳解析完成，强制排空取消它，随后等待全部线程退出。直接组合底层处理器时，异步解析需由调用方注入统一读包器并管理线程池；纯字节账本及原SQLite读取默认仍可独立使用。

无效JSON保持400 `invalid_json`；排队满、等待超时、执行超时及线程故障分别返回503 `json_parser_busy`／`json_parser_queue_timeout`／`json_parser_timeout`／`json_parser_failed`，解析失败发生在模型受理前，不生成计量记录。取消沿499或服务停止原因处理。状态只含线程／活动／重建中／退出中／等待数量、累计结果和配置；关闭后线程归零。上传未结束时的服务端取消会终止该输入连接，解压器也使用同一取消信号，不能等客户端自行结束才回收资源；取消不误报为坏zstd，正常413／400不变。该实现减少主线程直接解析，但扁平表示的接收、重建、验证及后续序列化仍会占主线程，单次分配不受让出预算的硬实时保证；高结构对象会增加总耗时，仍需完整负载验证。无新依赖或schema，默认部署未切换，回退先沿现有排空流程处理在途请求。

`postgres-account-quotas.mjs`已接入单账号／整池刷新及登录成功后的额度读取；开发应用每30秒巡检，和账号检查／登录核验共用默认2执行、8等待的有界队列。整池按256条分页，最多按队列并发数提交查询，同账号同代次复用；重复整池请求复用扫描。005迁移仅新增`quota_attempted_at`，30秒失败节流跨重启保留，不当作官方快照时间或成功记录。主动刷新可跳过节流，但不能绕过去重和队列上限；API账号不查询订阅额度，未验证的订阅平台明确提示缺口。

官方额度子进程继续从原账号的 `CODEX_HOME` 读取登录材料，每次查询使用独立私有临时 `CODEX_SQLITE_HOME`，不与原 CLI 会话共用 SQLite 状态。完整收到结果后通过 stdin EOF 退出，等待实际进程关闭并清理本次缓存后才结束查询；取消／超时同样等待关闭。被迫强杀时不把结果当成功，也不删除仍可能被原生子进程使用的目录。该隔离不改变账号凭据、官方额度口径、会话绑定或配额策略；回滚恢复原额度查询实现，保留原账号状态。

员工列表可用 `GET /api/admin/members?view=list` 只读员工配置、Key 数和登记设备数，不扫描请求用量或计算限额剩余；此时返回 `usagePending`，不以零值替代尚未读取的统计。页面在服务端声明 `memberListMetadata` 后先展示可操作列表，再通过原完整列表接口补齐近7天、近12小时用量及限额状态。后台统计失败保留管理功能并明确未加载；切换筛选或离开页面取消旧查询。默认接口、统计价格口径及版本冲突校验不变，无数据库迁移；回滚后自动沿用原完整列表读取方式。

额度及计量校准短事务应用，核对启用、状态和账号代次；停用、移除或重新登录后的旧查询不更新新状态。查询失败保留上次官方快照，只保存泛化错误。官方明确拒绝保存为锁存事实：普通额度拒绝只允许在独立额外额度条件完整时继续；workspace或消费控制拒绝不能由人工权重或额外额度绕过。后续未知读取、百分比回升或到重置时间不自行清除拒绝，须对应的明确官方允许／解除信号。恢复／阻断仍更新自动参考权重，普通余额变化不强制重算，临时人工比例保持；不迁移会话、不重发模型。关闭停止定时巡检、取消额度读取，默认官方额度子进程支持取消信号；CLI资源隔离和夜间Worker已接入；套餐容量新口径与正式全规模验收仍须继续完成。日常SQLite入口未切换。

`postgres-session-admin.mjs`已接通开发管理入口：为启用员工创建已检查API账号的固定Responses会话，人工换绑、会话启停及临时参与夜间调整。写入按员工→路由→会话加锁，与受理／结算保持顺序，审计失败整笔回滚。人工换绑拒绝在途请求和同员工慢上传、旧版本、禁用员工／会话及旧CLI协议；成功只影响后续调用，不改写历史请求归属。订阅临时固定到下次北京时间02:00，API固定不自动到期；禁止访问允许当前请求完成。

`routing-preparation.mjs`为单活动PG网关提供请求与迁移的进程内交接：请求从读包前取得准备保护，直到受理事务真正提交才释放，随后由数据库queued/running记录接续在途保护。人工换绑在原版本／状态校验后一次预留员工，若已有准备中请求则按原session_busy拒绝；预留保持到绑定、审计事务提交或回滚完成。期间新请求在读包和受理前等待，完成后使用提交后的绑定，回滚则沿用原绑定，不重放模型。等待请求仍计入原全局／员工并发上限，队列上限沿用`maxConcurrent`（默认120），取消／排空释放等待项，交接后先同步登记等待读者，避免被另一迁移抢先。

该交接对象只在活动网关内使用，不是数据库业务主数据，不可由独立Worker或客户端直接预留；`/api/admin/status`仅提供读者、等待数与预留员工数，不暴露身份。它不能替代数据库在途、账号、权限与版本检查，也不改变账号停用时原有强制迁出规则。夜间执行器同样持有交接到事务结束，并复验整组当前状态。无新增schema、环境变量或页面选项，回退此适配不改已保存数据；默认SQLite入口保持。

新增`session-request-handoff.mjs`已接入PG开发Responses及共会话图片JSON请求的收包与真实结束路径。票据从可信员工鉴权后、收包前保留到真实结束；旧未知上传与该会话旧请求形成完成边界，新请求按会话等待，已识别其他会话不受阻。预受理查询与路由事务复用同一客户端标识优先级、员工归属和固定会话校验；等待不持数据库事务，真正受理时再次核验。`postgres-session-handoffs.mjs`提供持久化人工管理接口及PG会话管理弹窗，五时点原整组执行已接入，活跃会话自动交接继续实施；旧PATCH换绑仍保持原忙碌拒绝规则，不把这部分接口验收当作完整功能交付。

交接等待默认30秒（合法1毫秒至120秒），决定登记最长15分钟（合法等待时长至24小时）；PG接线使用原请求并发作为票据上限，总等候／员工等候分别最多占原并发预算的一半、向下取整且至少1，默认60／4，单会话等候默认4，人工待交接仍最多64；全局待交接会话默认20064，覆盖默认20000会话规划与64个人工决定。模块独立使用时票据、待交接和总等候默认各120，单员工／会话等候默认各4，票据与等候数量配置合法1至10000，待交接数量合法1至200000；网关构造参数`handoffs.maxPending`控制全局待交接上限，重启生效，无客户端覆盖或部署环境变量。降低上限可能拒绝整组登记，但不会只登记一部分或扩大实际模型并发。超出等候预算或期限时取消尚未应用的决定，释放新请求；若已进入事务，则发出取消信号并等真实提交／回滚，超时请求明确退出，不提前放行其他请求。取消信号不伪造旧请求已结束；与成功提交竞态时以实际成功为准。替代待交接决定须匹配旧决定标识，保留原请求边界和等候期限，不把等候者重新算作旧请求。新登记提交前旧决定暂停，登记失败则恢复原回调及等待；提交成功才将旧决定终结为被替代。登记期间原决定到期、等待超时、超容量或停服会取消整个预留，不复活旧过期工作。正常排空仍允许此前已接纳的上传／解析继续执行，强制中断及失锁则按原机制取消。原02:00临时配置到期语义不变。

人工交接使用管理员Cookie：`POST /api/admin/sessions/:id/handoffs`提交`account_id`与`expected_route_version`，替换已有决定另带`replace_handoff_id`；返回202及决定ID只表示已登记。`GET`同路径读取近7个北京时间自然日内最新决定、源／目标、状态和原因；`DELETE /api/admin/sessions/:id/handoffs/:handoffId`可在等待或事务外目标核验阶段取消决定；真正进入绑定事务后拒绝撤销，并等待实际结果。只有绑定、版本及完成审计同事务提交后才显示`completed`；目录与凭据读取在事务外，以2执行、64等待、15秒限制，应用前再次检查员工／Key模型权限、会话版本、源目标账号代次和状态、当前模型、上下文及在途结算。登记期间发生的取消须等待事务提交／回滚结果可见再落终态，错误返回前收尾，避免留下无本地屏障的待转移记录；目录检查因等待超时中止时保留真实超时原因。订阅临时固定沿登记时的下次02:00，超过该时点即取消，不顺延到次日；API固定仍由人工控制。停止时保留未提交的人工决定供重启重新核验，已完成决定不重复执行；最终结果落库失败保留待核验状态并在管理状态中报错，不伪造成功。

`request-handoff-group.mjs`已接入PG实际自动整组执行器，共用Responses／图片请求票据；每个待移动会话先预留屏障，全部旧上传／请求真正结束后才运行一次整组事务，不在等待期间持有数据库连接或路由锁。其他会话仍可继续。任一成员取消、超时、停服或被替代时取消整组；如果已经运行事务，则等待真实提交／回滚后才释放各屏障，实际提交成功仍以成功为准。登记冲突或容量不足撤销本组已登记部分，保留此前决定。组数量仅以计数加入状态，不暴露会话身份。停服再次检查读取计划后的状态，避免关闭快照之后补装新屏障。

自动执行器在共享组屏障路径不再另占员工级准备预留，避免新请求已持准备读保护又在会话屏障等候时自锁；旧独立调用未传共享票据时仍保留原准备保护。当前自动策略的静默期／数据库在途复验尚未解除，目标模型／不透明上下文验证、自动待转移持久记录及人工优先接管继续实施。这一组门控与真实HTTP／PG事务验证不等于完整活跃会话自动交接已交付，也不替代真实跨官方账号上下文兼容测试。

PG会话管理弹窗分开显示当前实际绑定与最近转移结果，两者由同一SQL快照读取；历史完成目标不覆盖后来真正绑定。无历史返回明确空状态。仅可见弹窗内等待结果每4秒刷新，无变化不重画；编辑、错误、终态、隐藏、关闭、切页及退出停止轮询并拒绝迟到返回。登记202仅表示接受，不提前显示成功；允许替换等待目标、取消核验前及核验中的决定，开始提交后操作禁用。人工转移历史只显示会话、已知账号和白名单原因，不展示决定私有数据。旧SQLite／CLI管理保持原行为。

`010-session-handoffs.sql`只追加决定与能力元数据，历史请求初始化为未知；不能据此假定历史上下文可跨账号使用。输入、上游事件及结果分类只保存可重放／不透明／未知枚举，不保存加密推理或文件内容、不修改上游载荷。普通文本及客户端完整重放的函数调用可通过受控协议验收；文件引用、加密推理、托管工具及未知类型暂明确阻止新式转移，真实官方账号兼容仍须另行验证。较早并行请求的不透明结果不能被最新安全请求掩盖；客户端提交新的完整可重放上下文后可再次核验。转移后的请求携带未支持上下文时明确409，不静默删历史。能力状态在终态响应前落库，失败保留未知、原用量仍准确；不在响应后增加纯元数据等待尾巴占住下一次请求容量。旧CLI不支持平滑转移，图片通道仅共用票据，未开放其跨账号上下文迁移。回退不能让仅识别旧schema的二进制直接连接010；须使用识别新schema的兼容入口或按正式备份恢复流程核验，已完成绑定和历史用量不得用旧数据覆盖。正式切库／回退工具及五时点活跃会话交接继续实施。

均衡开关和临时订阅比例已接通原接口，复用既有校验、版本竞争及02:00到期；等待路由锁后及事务提交前复核排空／失锁状态，拒绝时设置、自动参考及审计一起回滚；不修改API人工占比。总览同一查询快照内提供调度状态，说明接口与总览共用管理查询预算和独立查询池，纯读取不写配置、不推进轮转。恢复自动和实际路由写入仍使用同源权重计算。夜间规划与整组执行已接入下述PG开发链，完整历史页面和套餐容量新算法仍在开发；默认SQLite入口未切换。

`postgres-member-admin.mjs`已接通开发处理器的员工／Key创建、启停、续期、名称和模型权限修改、逻辑删除及共享限额管理；创建员工、首Key和审计同事务，缺少凭据加密材料或审计失败整笔回滚。写入按员工→Key加锁，模型策略版本冲突拒绝覆盖；已派发请求继续完成，删除仍保留收包／在途保护。费率目录只读接口供模型和限额弹窗使用，与请求查询共享资源预算。正式PG入口、完整账号／会话／费率写入及其余管理业务仍须继续接通。

`postgres-state-query.mjs`提供总览、员工／Key及分页会话的PostgreSQL开发查询，七个北京时间自然日与近十二小时分别在SQL中精确汇总；请求列表、统计与总览复用同一计价投影和有界查询队列。员工Key恢复须显式传入既有员工凭据加密密钥，不能误用正文密钥；密文、完整摘要及内部分页字段不返回前端。总览仅返回审计时间和动作，完整结构化历史通过下述专用分页接口读取。账号列表顺序使用稳定ID，不依赖旧SQLite rowid。这仍是默认服务切换前的开发适配，员工服务端分页／按需读取Key及其余管理写入继续实施；独立连接池已由新开发应用提供。

独立后台开发入口为`npm run worker:postgres`，使用与PG网关相同的`HARBOR_DATABASE_URL`及TLS校验，仅连接已迁移数据库，不自动切流或创建结构。Worker默认独立4连接池、2个处理槽、每秒轮询；每次执行独立登记，即使同任务失租重认领也分别计入并发与关闭等待；新任务调度失败仍消费已入队任务，避免满队后无法排空；构造参数`poolSize`（必须至少为执行并发的两倍）、`runtime.concurrency`（1–8）及`runtime.pollMs`（100ms–60秒）重启生效。CLI错误只输出固定提示，SIGINT／SIGTERM停止认领、取消当前处理并关闭自有池；`status()`提供活跃、完成／失败／失租计数及池状态，独立HTTP健康和部署编排仍待交付。

`postgres-jobs.mjs`使用PG行锁／SKIP LOCKED认领、有代次的30秒租约及续租；默认最多1000个未完成任务、每任务最多5次尝试，构造参数`jobs.leaseMs`（1秒–5分钟）、`jobs.maxPending`（1–100000）及任务`maxAttempts`（1–20）控制。任务体／结果各不超过64KiB，调用方只提交服务端元数据；不会持久化模型凭据或重发模型。时钟默认以数据库为准。任务标识重复时核对类型／目标／数据／尝试上限，旧代次不能续租、提交效果或确认；续租取得行锁后重新读取数据库时间，等锁期间过期不能恢复租约；效果与结果可同事务提交，确认丢失后复用已保存结果，不重复执行。取消不会撤销已提交效果，已保存最终结果的任务拒绝取消并按已完成收尾。006迁移追加租约代次、尝试上限与未完成／到期索引，旧迁移不改写。

Worker周期任务`routing.expiry`：以北京时间02:00周期去重，启动或进入新周期恢复已到期临时固定和手动权重。会话每批256条短事务，审计及进度同批提交，批次间释放路由锁；后批失败保留已提交进度，重试不重复审计。API永久固定保持，不改变绑定，不调用上游模型。自动参考复用原同源权重计算，周期使用数据库时间。夜间历史页面、正文整理／清理／搜索／导出及部署健康仍在开发，不能将该Worker入口直接当作完整夜间功能或生产交付。

`postgres-planning-input.mjs`已提供独立规划输入适配：同一只读可重复读快照中读取候选账号、费率版本及每会话汇总，不向应用返回请求明细或原始Token样本。已完成参考金额精确相加，未完成用量按对应版本、模型及样本时间在SQL内重计价取中位数；缺价或无样本沿原规则保护整组，人工校准不充当原始计量样本。SQLite和PG输入复用同一规划策略，重会话、偏差方向、最少迁移及5个百分点容差保持。默认最多20000会话、1000候选账号、1000费率版本，构造参数可分别调至100000、100000、10000；超限整体拒绝，不生成截断计划，仍受数据库语句超时约束。合成十万条请求验证应用仅接收会话汇总，查询使用既有窗口／在途索引；此为本机微基准，不是正式容量验收。规划输入已接入下述Worker预览；夜间绑定应用由活动网关执行，见下文。

PG预览与夜间计划另存`dispatch_planning`资格，不修改官方额度快照。按原套餐比较组内持久请求的原始`model`集合核验额外权益，不能拿计费别名或未知模型当通配，请求协议取已存`source_channel`，存量legacy仅在服务端已证明上下文可重放时按Responses核验，未知协议不给额外迁入资格。核验格式不支持的模型只排除所在组的额外迁入，不让无关组整体失败；图片等不可重放上下文在请求间模式沿原保护保持固定，不冒充Responses范围。同一只读快照每批最多1000账号，模型集合上限沿已核验权益的200项范围，未知或无法覆盖时在跳过原因中说明。包内目标优先，明确耗尽或禁止的源账号仍可迁出、不能迁入；组内没有包内目标时才使用已许可额外预算，多个候选只比较同口径参考USD，不以同名credits假设单位可比，也不套用套餐倍率。过期或刷新失败的正向额度不作新鲜依据，但已锁存的官方拒绝继续保留。迁移前按实际最新模型及协议复核，提交前再核身份、权限、预算与核验版本／金额事实，变化整组跳过；规划和迁移不预支额外预算，真正请求仍须最终授权。重会话、偏差方向、5个百分点容差、最少迁移及人工期跳过保持。参考USD为管理估算，原始Token仍只作计量事实，不与官方余额／credits混算。

新增按需`routing.preview`任务，管理员`POST /api/admin/balancing/plans`以空对象提交后返回202及任务ID，独立Worker计算；`GET /api/admin/balancing/plans/:id`读取状态和计划概要，`GET /api/admin/balancing/plans/:id/items?section=moves&after=0&limit=25`读取分节游标页，公开节为`moves/skipped/groups/fixed`。分组中的固定会话拆到`fixed`节，按`group`关联；不通过接口暴露内部账号／会话输入快照。管理员Cookie鉴权、现有查询队列及独立查询池保持；员工Key不能访问。PG开发应用通过`capabilities.asyncPlanning`启用后台预览页面，未声明此能力的旧入口沿用原预览；不按接口404猜测并偷偷切换实现。

后台预览弹窗支持排队／计算状态及分组负载、预计迁移、跳过原因、固定会话四节，每页最多25项，按服务端字节预算可少于25项。仅保留当前页和返回游标；客户端会话标识随计划保存，不依赖当前会话列表页，缺失时明确标为平台会话。只有弹窗打开、页面可见且任务未完成时每2秒查询，完成／终止／读取失败停止自动查询；失败可重试读取原任务，重新生成由人工点击。关闭、ESC、原生关闭、换页签、替换弹窗及退出均取消页面请求和定时器，后台任务不因此取消。到期清空结果并允许重新生成；静态完成页不自动刷新。顶部关闭沿用原弹窗布局，桌面及窄屏均可用。

完整计划及输入快照由007迁移的独立表保存，默认16MiB总上限、256KiB单项上限，任一超限整体拒绝；每次SQL最多128项并按512KiB软预算分批，计划和任务最终结果同事务提交，不发布部分计划。任务结果只存计划ID，确认丢失或新代次复用已有结果，不重算重写。读取先取页内大小，再取选定内容，默认128KiB软页预算，单项较大时仍前进；每页1–100项。Worker构造参数`planning.ttlMs`（1分钟–24小时，默认1小时）、`maxPlanBytes`（1KiB–64MiB）、`maxItemBytes`（256B–2MiB且不超过总预算）、`pageBytes`（256B–2MiB）及`input`资源配置重启生效；网关读取默认页预算。状态仅查近7个北京自然日，计划到期返回410，旧计划不因此删除，存储清理任务仍待交付。

预览同一只读快照捕获均衡配置和聚合输入，不查询或更新上游额度，也不改绑定、权重及轮转。快照可能缺少新鲜额度而跳过组；`preparationChecked:false`明确独立Worker尚未核验活动网关的内存慢上传状态，预览不能直接应用。夜间执行已在网关重验当前配置／账号／会话版本、员工、静默与在途状态；套餐新策略接入后继续扩充对应版本依据。007只追加计划表，旧迁移不可变；旧PG开发版本的精确schema校验会拒绝新结构，回退须使用对应数据库备份，不直接让旧版本启动，默认SQLite入口保持。

PG开发入口的`routing.nightly`现按北京时间默认02:00、10:00、13:00、16:00、20:00五个实际时点规划并由网关按原整组保护应用，开启沿用`settings.nightly`总开关。`transfer_schedule`服务端设置保存`times/timezone/version/effective`，管理员`PATCH /api/admin/balancing/schedule`提交`times`及`expected_version`，HH:mm去重排序，只支持Asia/Shanghai；缺省为上述五点。保存后仅未来时点生效，不立即补跑、不自动开启总开关，版本冲突拒绝覆盖，配置与审计同事务。页面显示时点与下一次时间，错误保留输入；SQLite保留每日02:00原策略。

任务按每个时点去重：02:00沿用日期ID，其余使用日期-HHmm，排序和过期按时间戳，历史日期记录不改写为五次。错过时点时只为当前时段最近的时点生成任务，不补中间时点；以规划实际时间向前16小时计算用量，另存原时点boundary。计划有效期为实际规划时间加`planning.ttlMs`（默认1小时），且不越过下一时点；晚启动不因原时点加1小时已过而直接拒绝。当前时段内已生成但过期的计划会保留原件并生成新修订，重新读取当前条件；不执行陈旧绑定，也不为已过时点追加修订。

各时点依赖最近一次02:00的`routing.expiry`，不在白天额外重置临时权重或人工固定；仍先等待网关强制额度刷新／自动参考权重更新屏障，再以同一快照规划。本轮失败或仍为时点前快照的账号不参与可比较规划，不能把未知当0。屏障过5分钟新鲜期会重新强刷，不调用模型。停止／恢复沿原任务代次保护；保存完整计划前短暂取得路由锁重验配置与当前时段，配置变化不发布旧计划。Worker每轮最多取消256个停用／过时且未提交效果的自动任务，手工预览及已提交效果保留。配置回退不覆盖已完成绑定或历史；新旧Worker不能同时按不同调度设置执行，正式回退组合仍待交付。

016迁移后，PG默认以`request_handoff_policy.mode=request-boundary`执行活跃Responses整组交接，五时点与人工换绑共享持久决定及票据屏障。普通活跃和旧静默期不再阻止可重放会话参与；全部旧上传／受理请求真实结束后，事务外核验目标目录，短事务复验完整组并一次提交绑定、审计和检查点。等待期间不持数据库或路由锁，超时／取消／停服释放屏障；不重放旧模型请求。人工接管优先，登记或后续核验失败恢复原自动组；人工实际提交成功才终止旧组。关闭自动不取消已接受的人工决定。旧CLI与未知不透明上下文保持保护，不能用正文记录拼造完整上下文。默认SQLite入口仍未切换。

009迁移为后台任务追加显式前置依赖，并为存量夜间任务补齐周期恢复依赖；不修改001—008历史SQL。前置任务未完成时，夜间任务保持排队、不占执行槽和尝试次数；前置失败或取消则明确失败。已经提交的任务效果仍可恢复确认，不会重复执行。执行中跨过截止时间会回滚当前组或最终汇总，日记录标失败，已提交组保留且不重复迁移；跨时点恢复补记尚未开始应用的历史计划为中断，不补执行旧绑定。

活动网关每秒检查轻量任务指针，由`postgres-nightly.mjs`应用已完成的专用夜间计划，拒绝预览计划。兼容保守模式下整组先预留涉及员工，再按员工→路由→账号／会话锁核验：当前配置与计划相同、账号代次／可比较额度依据和新鲜度保持、员工及会话有效、绑定版本一致、自动参与、无在途／近期活动且目标有额度。刷新时间变化本身不构成额度依据变化。任一条件失败整组不迁移并保存原因；绑定、逐会话审计与组检查点同事务提交，双向迁移不发布半组。确认丢失或进程重启从未完成组继续，已处理组不重复；SQL／审计失败整组回滚，应用尝试数持久保存并沿用对应任务`max_attempts`（默认5），耗尽后标失败，不无限重试。停止／失锁先回滚再释放准备交接，已受理请求不重放。

008只追加执行关联、应用尝试、分组结果及周期屏障／查询索引，不改001–007。`balance_runs`保存计划ID和紧凑汇总，完整计划保留于独立计划表，结果不足时不拼凑完整成功；过期计划失败、跨日未完成批次标中断，近7天失败／取消的规划任务亦补记日状态，保留任务原完成时间。总览能读取实际最后批次状态，分组迁移事实／原因已入库，完整历史分页与页面已接入下述开发查询。网关`/api/admin/status`提供夜间运行和固定错误码状态，不返回原错误或身份。默认SQLite入口／部署组合不切换；旧PG开发版本因精确schema校验不能直接读008，回退须使用对应数据库备份，已发生绑定不自动倒回。

`postgres-history-query.mjs`已接入开发应用的`GET /api/admin/history`与既有筛选参数，按当前页读取结构化白名单元数据，同一只读快照完成筛选／计数及当前页，不返回原始审计文本。管理员Cookie鉴权，员工Key不可读；复用管理查询预算与独立查询池，近7个北京时间自然日可见，更早数据保留。未知事件只返回动作和时间，不补造详情；单项原文超过2MiB或结构化展示触及脱敏器资源上限，明确返回`history_capacity`，不静默截断为完整结果。

夜间`GET /api/admin/history/nightly/YYYY-MM-DD`只返回批次／计划概要和实际组结果汇总；`.../items?section=moves&after=0&limit=25`分页读取`groups/moves/skipped/fixed`四节，不开放私有输入快照。沿用1–100项与默认128KiB软页预算，单个较大项仍保证游标前进。记录在执行计划到期后仍可按七日窗口查看，不能用预览TTL隐藏历史；实际迁移只由已提交的组检查点确认，计划后负载不表示全部执行。旧摘要在数据库内拆页，缺少分组结果／旧价格依据时显示未记录及隐藏旧金额，单项超过2MiB明确拒绝并保留数据库记录。

016在启动接受请求前先恢复人工决定，再恢复自动组，旧自动等待重新核验并为当前时段生成不可变修订；原已提交迁移事实跨修订保留，本时点已迁移会话不再次迁移。`nightly_plan_applications`及`nightly_plan_group_results`按plan_id保留实际结果，`balance_runs`仍每时点一条累计摘要；旧检查点迁移仅复制真实事实，不把计划当实际。旧无计划摘要继续兼容。当前schema严格校验，回退须使用能读取新结构并继续执行设备／权限门禁的兼容版本，不能直接启动旧镜像或用旧库覆盖新调用。

批次概要的`results`是同一时点跨修订累计实际结果，`counts`及`selectedResults`属于当前所选不可变计划；`plannedTotal`为各修订计划量合计，可重复规划，不能当作独立会话数。`GET .../nightly/:day?revision=0`和`.../items?revision=0&section=moves`可读取指定已应用修订；邻接修订来自实际存在的计划，不假定编号连续。`.../revisions?after=0&limit=25`按位置分页列出修订结果，默认25、最多100及同一字节预算。未应用的后续修订另按后台任务实际状态显示排队／计算／待应用／失败／取消，不把失败作业写成正在等待。账户／会话历史筛选覆盖时点全部修订。页面可逐份查看，关闭／替换后迟到结果不会覆盖新弹窗，不增加历史轮询。

017注册员工及Key访问版本，旧单项API实际改变启停／到期／删除状态时自动递增，防止先改回原值的ABA覆盖。管理员`POST /api/admin/member-action-batches/preview`提交`{kind:'member_access',enabled,member_ids:[...]}`或`{kind:'key_renewal',expires,key_ids:[...]}`生成固定目标预览；只处理明确ID，不在后台扩大筛选。`POST .../:id/execute`只接受空对象返回202，由独立Worker的`member.actions`逐员工原子执行；受理不是完成。员工启停不改各Key自身启停，未派发请求按当前策略拒绝，已派发完成计量；续期只延长，永久或更长有效期保留，不借批量缩短期限或清零用量。版本／集合变化项跳过。`retry/undo`生成新预览，分别仅处理失败／已成功项，撤销复验应用后完整集合及版本；取消剩余项不回滚已成功项。列表／详情近7天可见，更早只保留数据库，管理员Cookie与查询预算沿现有入口；员工多选与逐Key批量入口仍待页面接线。

开发页面通过`capabilities.pagedHistory`选择分页批次视图，旧SQLite入口仍保留原展示。弹窗只保留当前页和返回游标，显示计划与确认数量、整组跳过原因及重会话说明；没有新增历史轮询，手动刷新查询当前状态。关闭／ESC／退出／替换弹窗中止页面读取并保护迟到结果，七日可见期届满清空当前明细；不会因查看历史提交新规划或迁移。

`postgres-responses.mjs`是可组合的异步HTTP处理器，尚未替换`server.mjs`默认入口。路由、共享限额、上游通道和管理员鉴权适配器必须显式提供，不能缺省跳过现有策略；隔离HTTP已使用真实路由和共享限额适配，默认服务及完整管理功能尚待接通。管理员元数据入口为`GET /api/admin/requests/:id`，保持近7个北京时间自然日范围和Standard参考USD；正文入口为`GET /api/admin/requests/:id/recording?after=0&limit=32`，使用独立正文保留期，员工Key无权查询。正文游标为已保存事件序号，每页1–100个事件；仓储内部同时按密文字节限页，默认128KiB（配置256B–2MiB），先读少量大小元数据，再取选中密文；单个较大事件仍返回一条保证游标前进，不会整批读出所有大字段。

PG开发应用通过`postgres-chat-service.mjs`接通旧文本`POST /v1/chat/completions`、串行等待队列及原Codex／Claude CLI执行器。沿原`accountCapacity`与最近成功样本估计，不套Responses分配权重；同员工跨Key排他准入、员工／Key预占和账号待核对预测量保留。完整历史按原摘要校验后只传新增消息，未知会话拒绝续用，不自动换号；派发前再次核实Key、权限、共享限额、绑定版本与账号容量。内部`reserveTokens`默认32000、须为正安全整数，仅构造时注入，尚无客户端设置入口。

Chat与Responses／图片共用请求并发、准备交接和排空状态；CLI等账号现有请求结束后派发，同账号CLI串行，Responses仍保留原并行语义。已受理Chat不因后来另一账号的同员工Responses取消。账号停用后，排队Chat仅可跟随已提交且已清空旧上下文的新绑定，返回`context_reset`；已执行请求按原账号结算，旧结果不覆盖新上下文。人工换绑／夜间在途保护不变，不实现正在输出请求的跨账号接力。

仅服务端`options.chat`可配置Chat资源：`maxRequests`默认16（1–64）、`maxQueued`默认5（1–64）、`waitMs`默认120000（100ms–1小时）、`timeoutMs`默认120000（100ms–24小时），重建应用生效；客户端参数不可覆盖，128KiB纯文本输入和CLI禁工具规则保持。排队超时／取消须先完成派发事务回滚再释放账号，不启动CLI；关闭取消排队并等待执行，强制排空保存中断与未知预占，不重跑模型。采集只保留本轮输入及可识别的回复片段，旁路回调错误不影响推理；SSE沿旧行为在CLI完成后发送，不是逐Token流。计量暂存成功而数据库未结算时，返回`X-Harbor-Usage: pending`和空参考用量，重启恢复游标与计量，不补造零用量。默认SQLite入口不变，平台通道及全局字节／进程预算继续实施；受控子进程验证不能替代真实订阅验收。

PG开发应用通过`inference-budget.mjs`统一预留模型执行容量，Responses／图片、旧Chat及平台对话共用全局、员工和账号账本；员工身份来自认证，全部Key共享。既有`options.maxConcurrent`默认120、`maxPerMember`默认8继续约束已接纳请求；`options.execution.maxPerAccount`默认32，`maxCli`默认4，`maxCliPerMember`／`maxCliPerAccount`默认2。上限均为1–4096的整数，子项不得高于父项，省略的子项按父项向下适配；执行账本的全局／员工上限使用前述原参数，不能通过`execution`覆盖。服务端重建应用生效，客户端不能提高容量，无无限制开关。

Responses在受理短事务提交前预留；账号容量不足返回503 `inference_capacity_busy`并回滚本次新绑定、路由记账和请求记录，不自动换号、重试或改变权重。平台在凭据就绪及派发复核时预留，饱和时保留已受理的已知零消耗拒绝记录，不把账号标掉线。旧Chat沿原有界队列等待，共享CLI容量释放后唤醒；同账号已有执行时不启动Chat，但已启动Chat不额外禁止后续Responses并行。Chat及Qoder占模型CLI槽，HTTP模型通道只占通用执行槽；失败、取消和事务回滚收尾后幂等释放，`/api/admin/status.execution`仅返回数量／峰值和配置，不含员工或账号标识。

此账本是模型执行预约，不是OS进程计数；实际CLI启动及退出另由下述进程账本约束。默认SQLite服务未接入本账本；回退本批PG源码无需迁移或删除数据，会恢复此前没有单账号／模型CLI预算的行为。没有改变权重、模型权限、计价或在途换绑规则，也没有缩短模型长任务期限；正式容量仍以完整压测为准。

PG开发应用的`cli-process-budget.mjs`另统一限制实际CLI启动：原Codex／Claude文本执行器、官方登录、订阅状态检查、额度RPC、凭据刷新、版本读取及Qoder目录／推理均经服务端受控spawn。按可信账号运行目录归属，版本检查使用内部归属；只统计数量，不返回目录、PID、参数、凭据或子进程输出。默认`options.cliProcesses.maxConcurrent=8`、`maxPerAccount=2`、`maxMaintenance=4`；维护类有独立子上限，模型仍受前述模型预算。三项为1–256整数，子项不能超过总上限，省略子项随总上限缩小；`killGraceMs`默认2000、范围10–30000ms。配置只由服务端构造传入，重建应用生效，无客户端覆盖或无限制开关。

进程槽从预留开始，未启动可幂等取消，启动后必须等实际`close`才归还；RPC结果提前返回不释放容量。取消／超时终止独立进程组，宽限期后强杀；父进程先退出时结束遗留子进程，等待继承管道关闭。正常排空保留已接纳推理，强制排空或失锁终止CLI；释放数据库所有权前等待进程收尾。Linux受控真实父子进程已覆盖；Windows沿`taskkill /T /F`实现进程树终止，本批没有Windows实机生命周期验收，不能据此声称正式跨平台容量完成。预算约束CLI启动实例及其受控进程组，不是操作系统全部进程数或RSS限额。

实际进程容量不足时，旧Chat在原有界队列中等待，实际进程释放后唤醒；登录在改写账号／会话前预留，503 `cli_capacity_busy`不改登录状态或额度依据，事务失败释放未用预约。订阅与平台检查返回暂时繁忙，保留已有认证状态；额度RPC沿原失败结果保留上次快照。Qoder进程未启动时的容量拒绝记录已知零消耗，不变成未知扣费或掉线。版本读取并发单飞并缓存成功，失败可重试；同账号凭据刷新复用原单飞，RPC返回后继续持槽至真实退出。没有后台自动重试模型。构造测试替身须自行遵守spawn契约，不能将完全绕过默认执行器的自定义传输当作生产进程约束证明。默认SQLite入口仍未切换；旧函数签名兼容，回退本批PG源码无需schema变更，会恢复此前缺少统一实际进程预算的行为。推理输出另受下述共享字节预算约束，正式容量验收继续实施。

PG开发应用的`output-budget.mjs`为Responses流式／非流式、图片JSON、旧Chat和四个平台推理共用输出字节账本，员工全部Key共享可信员工归属；模型目录使用独立内部归属读取。仅服务端`options.outputBuffers`可配置：`maxBufferedBytes`默认512MiB，`maxBytesPerOwner`默认384MiB与全局的较小值，均为256B–8GiB且子项不超过全局；`maxScopes`默认256，`maxScopesPerOwner`默认32与全局的较小值，均为1–4096且子项不超过全局。构造应用时读取、重建生效，无客户端覆盖或无限制开关。账本计算当前保留的编码载荷及显式缓冲／UTF-16／发送副本；JSON对象结构、GC及网络内部缓存仍需实测，不能将账本当作RSS或真实堆上限。

在拼接、解码、解析前检查临时占用；非流式保留的done项、终态结果和CLI／平台回复持续计量。流式事件处理完释放临时缓冲，Responses累计生成量可以超过共享预算，不增加模型生成总超时；旧通道原有单次限制保持。图片沿原128MiB单响应上限，临时副本也占共享预算，不保证每个上限大小结果都能在当前容量下接纳。超载返回503 `upstream_buffer_busy`，中断本次读取／CLI，不截断成成功结果、不自动重发，不把资源忙当账号耗尽或掉线。已解析可靠用量继续结算；WorkBuddy已取得完成标记及有效最终计量后，即使后续结束标记读取失败也保留计量与实际模型，中间或无效用量不据此当作最终值。中断前尚无可靠用量仍标未知，不补造零。

模型响应先按JSON实际转义规则计算UTF-16长度，再为序列化和编码预留；在下游写入回调完成前保留发送字节，逐段写入不无限入队。写入错误可能先于close，先销毁连接，等实际关闭后再归还容量并让取消信号完成传播；强制排空／失锁沿原中断规则处理，正常长任务不加短总期限。此处背压测试使用原生socket的受控刷新及显式TCP重置；HTTP半关闭不直接当作取消。管理详情、导出等其他响应仍按各自查询／分页规则，不属于推理输出账本。

Responses传输器模型目录新增`maxCatalogResponseBytes`默认4MiB（256B–128MiB）与`maxCatalogEntries`默认8（1–256），成功目录按LRU与原60秒TTL缓存；读取临时占用接共享账本，缓存另受条数和单响应上限约束，保留原UTF-8 BOM兼容。原Codex订阅状态stdout／stderr合计通过`checkLogin(...,{maxOutputBytes})`限制，默认2MiB（256B–128MiB），超过后停止追加、终止CLI并返回读取上限错误，已连接账号不被误标未登录。其余管理CLI沿原各自响应上限，未统一计入推理账本，缓存／对象实际占用与完整容量继续验收。无schema或依赖变更；默认SQLite入口不切换，公共传输器的新目录／状态读取上限也作用于原入口。回退本批源码无需数据迁移，会撤去新增输出预算与读取保护；已产生计量和绑定保留。

PG结算已为可信`source_channel=chat`保留完整历史摘要、上游会话与累计计量游标，和请求、员工／Key及账号用量同一事务；当前账号／路由版本不符则不回写旧上下文，累计值或身份矛盾标为不确定。未计量文本结果统一为unknown／interrupted并保留原预占；执行前拒绝可释放预占，不能当作Responses追踪记录。排他鉴权是服务端事务选项，按员工→Key顺序，默认Responses共享读锁不变。

关键计量缓冲仅新增白名单游标、摘要和累计值，不含消息、回复、路径或任意异常。带文本游标的加密文件使用`settle-chat`命令，新读取器恢复后转为普通结算调用；旧读取器只接受`settle`，会拒绝新文件并由启动恢复检查阻止接流，避免回滚时静默丢游标。旧普通计量文件继续可读，无schema变化；启用文本流量后，存在此类待恢复文件时须用识别新格式的版本恢复，不能只回退旧镜像。日常默认服务仍未切换。

PG开发入口已接入现有`POST /v1/images/generations`与`POST /v1/images/edits` JSON通道，复用原图片参数校验、管理员无关的员工Key鉴权、模型权限、共享限额、短事务受理及准备／在途保护。新建图片绑定只选订阅账号；已有官方API绑定明确拒绝，不静默迁入订阅。仍以员工隔离的Responses会话关联，实际请求`source_channel`分别记录`images/generations`和`images/edits`；不增加schema或新推理接口。已有流式／非流式Responses保持原协议，旧CLI Chat已另接开发入口，平台通道继续实施，默认服务尚未切换。

图片原始请求和结果仍完整转发，采集只存脱敏提示、结构化结果说明及附件类型／编码字节数，不保存Base64、图片URL或文件凭据，不把非对话工具请求标成普通用户／助手消息。采集关闭仍计量；用量缺失显示未知，错误响应中已取得的用量保留，取消／超限不重发。图片在执行时仍禁止人工换绑，取消保留已保存的输入前缀与部分缺失状态。

公共订阅传输器的图片JSON结果读取新增`maxJsonResponseBytes`构造参数，默认128MiB、合法256B–512MiB，仅服务端注入并重建传输器生效；不接受客户端覆盖，不影响SSE输出上限。检查声明长度并逐块累计实际读取，超限取消读取、返回`upstream_response_too_large`且不重试，尚未解析出的用量仍未知。此安全边界也作用于原SQLite图片入口；单请求限界不等同于全局内存或60人容量验收，整组资源预算及完整容量仍待实施。回退不需删库或重写历史，默认部署入口保持。

现有请求详情弹窗已增加正文与工具记录，按`content.field/mode`合并追加／替换，不重复展示最终完整回复；大内容点击“加载更多”，仅正在采集且没有未读页时增量轮询。部分缺失记录可手动刷新已保存正文，从当前游标读取缓冲恢复的前缀，保留已有内容且不自动轮询。历史无正文、关闭采集、关联不完整、部分缺失、截断及过期分别说明；关闭／切换／退出取消迟到读取，过期清空已显示正文。内容作为转义文本展示，不执行模型HTML或工具参数。请求受理时即标记待采集，避免缓冲尚未落库时误报未启用。真实Chromium在桌面和窄屏验证原Key→受控Responses上游→真实PG／加密缓冲→Cookie管理员详情；总览／列表／统计／详情均已接通PG开发适配，不代表默认PG入口或正式模型已验收。

`metering-recovery.mjs`将关键计量恢复与正文缓冲分离：处理器必须显式提供独立恢复实例，拒绝共用实际目录；默认独立内存1MiB／1024命令、磁盘16MiB／8192文件，复用有界加密落盘与有序重放。密钥经独立域派生，恢复文件仅含请求ID、结束时间、状态、模型及计量事实，不含回复、工具或任意错误。正文损坏／占满不阻塞计量恢复，数据库恢复后沿原幂等结算入口处理，不重发模型。关闭正文采集不关闭计量。采集初始化、输入／事件／结束及入队同步异常只标记部分缺失，不改变成功模型结果。新开发应用已接入启动编排和独立数据库连接池，全通道准入预留及完整故障恢复验收仍待完成。旧开发正文暂存中的settle兼容读取仍保留，正式历史迁移必须先排空，不能直接删除。

持久缓冲使用独立目录（Unix要求属主当前用户、目录0700、文件0600），默认内存4MiB／256命令、磁盘64MiB／1024文件，每250ms批处理。仅存已脱敏副本的加密命令，提交后删除；数据库故障时按顺序恢复且不重发模型，损坏文件保留并阻止后续完成标记。结算故障只暂存白名单计量事实及原结束时间，不暂存完整模型响应。队列／容量失败显示部分缺失，不宣称完整保存。正常排空、异常启动识别、后台告警、密钥备份和恢复演练仍须在正式切换前补齐；此开发入口不能作为已完成发布使用。

采集在复制／脱敏前按剩余字节、节点和事件数量限界；同一用户消息的文本块连续过滤，合法JSON字段转义同样识别，工具回传的JSON副本先剔除结构化附件。初始化未入队时不再提交依赖事件；拥塞标为部分缺失后仍可保存此前已接纳的前缀，不能再提升为完整。重启会验证未完成重命名的缓冲文件：完整加密命令恢复，不完整文件保留、占用配额并阻止后续完成标记，需管理处置。

用户和工具消息的文本块使用同一过滤状态，并保留安全输出的来源位置；延迟输出的词也能按原文本／附件顺序读取。JSON中的敏感数字、布尔、null及嵌套容器替换成字符串标记，后续非敏感字段继续保留；普通Header及环境变量仍按原行边界过滤。输入遍历按实际访问节点计数，忽略的内容类型也占预算，不因未生成事件绕过限界。

工具参数使用有界的流式JSON过滤器，按键和值的结构处理敏感内容；字符串先解码过滤再编码，普通文本仍保持其原格式。过滤结果在进程内标记并冻结，后续层只做结构及容量校验；此标记不来自请求字段、不会序列化，只有认证解密成功的数据才能恢复该状态，避免重复解释片段而破坏JSON。加密前拒绝超界对象，不将损坏的缓冲命令当作成功持久化。

普通Header值按整行过滤，包括引号内的Cookie属性；正文中的撇号或未配对引号不会缩短该范围。仅在已识别“引号键＋冒号＋字符串值”的结构时保留JSON字符串边界。凭据词首独立于滑动窗口及长词裁剪维护，普通长代码标识不因窗口移动而被误判为Key。

大段文本按不超过4096个UTF-16单元分块（不拆有效代理对），每块独立保存并按同一字段合并；最终替换的第一块用replace，后续块用append。大工具结果以`content.output`保存片段，`output_format=string|json`表示合并后的表示类型；短结果沿用原output值。正文仍受每请求总大小约束；输出节点、字段及标识同样限界，截断后停止采集工作。


## 调度依据、操作历史与请求阶段

账号池的“调度依据”展示实际生效的系统/临时手动权重、参与或排除原因，以及下一条新会话的分配试算。试算与真实分配共用计算，不推进轮转顺序。系统重算时保存额度、重置时间及计算时刻；普通额度刷新不会用新数据改写旧计算依据。升级前的权重没有快照时明确提示，等下一次正常重算补齐，不为展示强制重算。

“使用数据”集中会话绑定、Key 用量、请求明细、使用类型统计和正文管理；“系统管理”集中接入说明、请求测试及系统操作历史。请求明细按模型、用户及状态筛选近7个北京时间自然日的全部请求，支持服务端分页（每页25/50/100条）。汇总及模型/用户分组统计覆盖全部筛选结果，不受当前页限制；缓存包含在输入中，未计量请求单独计数。列表详情按阶段展示凭据准备、连接上游/等待响应、接收数据及统计收尾；未结束详情每4秒刷新。连接与等待响应合并观察，CLI只展示可观察的执行阶段，旧请求不会补造历史。阶段记录仅包含固定阶段名和时间，不保存正文或标题，不增加任务总时限，也不重放请求。重启时记录在途请求的已知中断，后续回调不能改写已结束记录。

请求明细通过 `view=list` 先加载数据库当前页，再以 `view=summary` 独立加载汇总；省略 `view` 保留旧完整响应。列表和汇总使用独立只读查询线程，单线程最多8个待处理任务、128 MiB堆上限，查询15秒超时后返回503并重建该查询线程，不影响推理任务。相同在途查询合并，结果缓存最多8份并随数据库数据版本失效。页面展示统计时间；分组和合计来自同一读事务，列表与统计水位不同时明确提示。查询线程不会读取账号凭据或保存正文。

翻页返回 `snapshot` 和下一页 `cursor`，客户端连续翻页携带原快照与游标；快照绑定筛选条件及北京时间窗口，并排除加载第一页后插入的请求（包括回填时间的新记录）。快照固定插入范围，已有请求的完成或状态变化仍会反映；不承诺冻结在途请求。跨日或筛选不匹配返回400，刷新重建快照。旧 `page/page_size` 仍在数据库执行分页。前端筛选、切页或退出会取消旧请求，晚到统计不能覆盖当前结果；隐藏页面及非实时记录页沿用停止自动轮询规则。

游标模式按连续遍历页次显示，下一页由 `nextCursor` 决定，不因已读记录被删除而提前停止；“当前匹配”数量反映查询时仍存在的记录。显式刷新回第一页重建快照，上一／下一页保留原快照。统计晚到时若正在编辑或页面隐藏，暂存结果，结束编辑或重新显示后再绘制，不替换正在操作的筛选控件。

该兼容优化新增请求插入序号、查询版本表及SQL触发器，不修改原计量事实。旧镜像可继续读写；外部数据库维护同样触发版本更新，避免缓存沿用已删或已核对的数据。回退前端恢复旧加载行为；其余规模化存储、正文、批量管理和套餐能力以对应版本交付说明为准。

操作历史及夜间批次只提供近7个北京时间自然日（今天及前6天），与用量窗口一致；更早记录完整保留数据库，仅通过数据库查询，不在页面或导出开放。支持事件类型、涉及账号、会话标识、时间及每页25条的服务端分页；时间按开始含、结束不含查询。夜间详情展示当时的负载计划、已执行迁移及跳过原因，“计划后”负载不表示所有迁移已执行。历史缺失前值、原因或执行结果时如实提示，接口仅返回白名单结构化元数据。

管理接口均要求管理员身份：`GET /api/admin/balancing/explanation`、`GET /api/admin/history`（`kind=audit|nightly`、`page`、`page_size`、`action`、`account`、`session`、毫秒时间戳 `start/end`）、`GET /api/admin/history/nightly/YYYY-MM-DD`、`GET /api/admin/requests`（`member`、`model`、`status`、`page`、`page_size`）、`GET /api/admin/requests/:id`。请求表新增可空 `phase/phase_at` 及默认空数组的 `phase_history`；配置中的计算快照为新增元数据，旧镜像可忽略，回滚不需删除记录。既有权重触发、会话绑定、夜间优先级及用量口径不变。历史列表强制夹取7天窗口，直接访问窗口外请求或夜间批次详情返回404；最近批次、日期控件及事件筛选也遵守窗口。账号、员工、Key及当前配置不按创建日期隐藏。

员工列表在用户名下逐把展示完整 Key、复制快捷按钮、北京时间到期时间和状态；永久、过期、停用及历史未保存全文分别显示。多Key不会只选一把代表。表头沿用原有左对齐样式，与正文保持一致。

员工详情仅展示一次员工身份与每把 Key：顶部修改名称，共享模型限额单独一行；每张 Key 卡片集中展示状态、到期时间、模型权限及来源 IP、续期、启停和删除。底部单独操作整位员工的访问开关。Key 权限仍逐把生效，限额仍按员工共享。所有弹窗采用固定顶栏与独立滚动内容区，右上角关闭按钮不覆盖滚动条或正文；关闭沿用原有取消与敏感内容清理机制。不改变接口、数据库或调度，回退前端即可恢复旧布局。

员工详情中可“删除”单个 Key，确认后永久失效并从列表隐藏，无法续期或重新启用；历史调用、用量和待核对预占仍保留，需要继续使用时新增 Key。同员工仍有请求准备、排队或运行时拒绝删除。删除仅限管理接口，不影响员工、订阅账号或其他 Key。数据库兼容新增 `keys.removed_at`，同时撤销原认证摘要；回滚旧镜像也不能恢复原 Key。

清理员工使用管理员接口 `DELETE /api/admin/members/:id`：从员工列表移除并停用该员工，永久撤销其全部 Key，保留历史调用和用量。准备、排队或运行中的员工不能移除；移除后不能重新启用或向原员工发放 Key。数据库兼容新增 `members.removed_at`；旧版本回滚可能再次显示停用员工，但旧 Key 仍已失效。单独删除 Key 不会移除员工。

授权清理的历史验收员工、Key和请求已删除；真实记录保留。账号须在账号池点击官方登录完成授权，当前状态以账号池显示为准。

## 员工Key模型权限

管理员在“员工与密钥”创建员工/Key时可设置模型权限，也可在员工详情逐Key打开“模型权限”，从已知模型选择或填写精确ID（每行一个）。默认不限制；限制时至少一个模型，不支持通配符或`codex/claude`旧路由别名。已知列表来自费率表，不保证上游账号拥有该模型。权限长期保持，由管理员修改；夜间权重不会改变它。

服务端按当前Key检查Responses、图片生成/编辑和旧网页聊天，在调用上游前拒绝未授权模型（403 `model_not_allowed`）。已有会话也受约束；开始执行的请求继续完成，修改后尚未派发的请求使用新权限。同员工不同Key互相独立；客户端请求中的权限字段不能覆盖数据库。受限Key不允许旧网页泛化聊天及Responses内隐式图片模型工具；需通过明确获准模型的图片接口调用。模型ID按提交值精确匹配，不猜别名，不承诺上游内部模型实现或账单模型。

`/v1/models`、`/v1/codex/models`按认证Key过滤；`/api/me`返回`allowed_models`。Codex使用本地模型目录，员工须下载过滤后的目录、设置顶层`model_catalog_json`并重启客户端；修改权限后重新下载，默认`model`也须在允许列表内。已用真实Codex 0.165.0隔离客户端验证仅显示允许项；其他版本或旧缓存可能显示内置选项，但请求仍被服务端拒绝。目录需保留上游完整Codex元数据，普通API只返回模型名时不能保证选择器可读。

数据库新增可空`keys.allowed_models`及`model_policy_version`，旧Key默认NULL不限制。管理PATCH修改权限必须携带当前`expected_model_policy_version`，过期弹窗返回409；修改记入操作历史。旧镜像虽然可读数据库，但不会执行模型限制：存在受限Key时禁止直接回滚到本功能前版本，应发布兼容修复镜像，或经管理员明确撤销限制后再回滚。

## 官方OpenAI API Key账号

“上游账号池”可手动添加任意多个独立订阅账号或API Key。顶部按钮与卡片入口统一为“添加账户”，打开后选择“订阅账号 / API Key”：订阅类型再选择Codex或Claude服务，API类型填写OpenAI官方Key与人工占比，默认0；两类共用添加入口，可在表单切换类型。每次添加独立保存，不覆盖其他Key。Key使用独立账号标识加密保存，后台仅显示已配置状态与摘要指纹，不回显明文。保存后通过“检查Key”读取官方模型目录验证认证；这不证明有余额或某项活动赠额可抵扣。更换Key须重新检查，不会把Key写入订阅CLI目录；不同项目建议新建账号，避免混淆历史用量。

开关及API新会话占比长期由人工设置，不在凌晨恢复或重算。全部API账号人工占比之和最多100%，剩余由订阅组沿用原自动或临时规则分配；无可用订阅时，可用API正权重按相对比例归一化。API权重0不分配新会话，人工固定会话仍使用该Key；停用则立即迁出全部绑定，无目标整笔撤回。API账号和绑定不参与订阅组夜间迁移；迁出到订阅账号后恢复其02:00期限规则。原订阅权重0规则保持。

API账号卡片“创建固定会话”选择所属员工，在该员工已配置Harbor的终端运行弹窗给出的一次性 `codex -c 'model_providers.harbor.http_headers.X-Harbor-Session="..."'` 命令。参数只对专用CLI进程生效，不修改全局配置或其他会话。员工仍使用自己的Harbor Key；固定标识不能跨员工访问，首次调用绑定一条客户端对话，其他新对话复用会被拒绝。新建验证会话后启动这条专用对话；完成后退出该CLI进程即可。管理员接口为 `POST /api/admin/sessions`（`member_id/account_id`），只允许已启用且检查通过的API账号。

客户端模型名原样透传，官方API与订阅账号可用模型可能不同，验证时应明确固定到Key并选择官方模型。仅接入Responses文本/工具会话；旧网页CLI聊天及订阅图片兼容入口继续使用订阅路径。转发保留Codex客户端标识，不因网关转发改变活动资格；失败不自动重放。

普通官方调用Key未找到公开余额查询接口；组织Costs接口需Admin凭据，费用不等于余额或赠额抵扣。页面展示平台真实用量与查询限制，不把限流额度当资金，是否使用活动额度仍需官方账单证据。

兼容新增accounts的auth_mode、api_weight和密文/摘要列；存量默认为subscription。回滚到不认识API账号的旧镜像前，必须先停用API并迁出绑定，否则旧版本不能正确转发；新增Key不写入旧订阅目录，旧版本不能把它当成订阅凭据。

## 其他平台接入

“添加账户 → 其他平台”支持独立添加多个 WorkBuddy、Qwen、DeepSeek、Qoder 账号，不覆盖已有订阅或 API Key。WorkBuddy 使用官方网页授权，不收集密码；授权状态只保留在内存，成功凭据加密保存。Qwen、DeepSeek 填写官方 API Key，Qoder 填写官方个人访问令牌（PAT）。Qwen 的旧免费 OAuth 入口已于2026-04-15停用，不提供失效登录方式；Qwen 须按 Key 所属区域和标准 API / Token Plan 选择入口。Key 不代表拥有网页版订阅权益或免费额度，使用费用以对应平台为准。

账号需完成授权及模型目录检查，才显示“已连接”。模型目录来自对应上游，不硬编码模型名单；检查通过只证明认证和目录访问，不表示真实模型调用、余额或赠额抵扣已验证。没有真实平台凭据时，仅能完成协议、隔离与本地集成验证，不能代替真实账号验收。

新平台使用独立的 OpenAI Chat Completions 纯文本通道：客户端 base URL 设为 `<Harbor地址>/v1/platform`，继续使用员工 Harbor Key，通过 `GET /v1/platform/models` 取得当前可用模型，向 `POST /v1/platform/chat/completions` 发送 `model`、`messages`、可选 `session_id/stream/max_tokens/temperature`。模型 ID 带平台前缀，如 `deepseek/<实际模型ID>`。网页“系统管理 → 请求测试”（员工入口仍为“接入与测试”）支持选择这些模型；员工先填写 Key 并点击用量检查，读取自身允许的模型。每次提交完整文本历史；服务端只保存绑定、请求元数据和计量，不保存平台对话正文。`stream=true` 使用 SSE 返回完整结果，不逐字实时转发。附件、工具调用及未知参数明确拒绝；这条通道不承诺 Codex Responses 编程工具兼容，不会把不兼容的平台账号加入 Codex 自动分配、夜间迁移或模型目录。

新通道执行同一套员工身份校验、逐 Key 模型白名单和员工模型 USD 限额，历史用量进入既有统计链。新平台模型没有已核实费率时显示待定价，须通过“费率与口径”配置该带前缀模型的官方价格及来源，不能把未知费用当0；开启员工限额后，无法确认模型费率的请求拒绝派发。上游缺少有效用量时保留未计量状态，不补造 Token。旧 Codex、Claude、OpenAI API Key 的原有路由及权重保持原规则。

Qoder 使用官方 CLI `@qoder-ai/qodercli@1.1.81`，容器镜像包含固定版本；本机须安装该 CLI 或在 settings 中指定 `qoderBinary`。默认调用官方包的直接入口 `qodercli`，避开 `qoder` 的分派启动器。本机原生程序可配置绝对路径，CLI使用临时独立配置和工作目录，只通过进程环境传PAT，关闭工具、MCP、配置继承及会话持久化，不加载个人Qoder登录或插件。固定版本设置 `QODER_EXPOSE_TOKEN_USAGE=1` 请求真实计量，官方输出的input_tokens已含缓存读写，按该总数记录，缓存读取仅作为子集，不重复相加；若仍返回隐藏的全零或无效计量，明确记为未计量，不能视作免费。官方 `--list-models` 返回文本表；Harbor 为模型显示名生成稳定的 `qoder/name-…` ID，调用时还原，不猜测模型能力或价格。CLI 本身仍可能在临时目录写运行日志，进程结束后清理该目录；取消、超时和网关关闭均终止本次进程树（Linux使用独立进程组，Windows使用taskkill /T，Windows仍需本机验收）。Qoder 不支持本入口的 `temperature/max_tokens` 参数。

平台账号支持启停与逻辑移除。停用沿用整组迁移事务：绑定迁往同平台的其他已连接账号，无接收目标时拒绝整笔停用；在途请求继续完成并归原账号计量，不覆盖迁移后的绑定。新账号未通过检查、凭据更新后未重新检查或登录取消时不接收调用。WorkBuddy 临期刷新在账号内合并并发，凭据更新、取消、停用或移除后的旧异步结果不能写回。

数据库仅兼容新增 `accounts.platform_config/platform_models`，凭据复用按账号隔离的加密字段，`auth_mode=platform` 与现有账号池隔离。回滚到不识别该模式的版本前须停用并处理平台绑定；不删除历史记录。镜像增加官方 Qoder CLI 依赖，回退镜像不会自动删除数据库中的新平台账号。

官方资料：[Qwen认证](https://github.com/QwenLM/qwen-code/blob/main/docs/users/configuration/auth.md)、[DeepSeek API](https://api-docs.deepseek.com/)、[Qoder认证](https://docs.qoder.com/cli/authentication)、[Qoder非交互模式](https://docs.qoder.com/cli/run-in-scripts)、[WorkBuddy](https://www.workbuddy.cn/)。WorkBuddy 公开客户端协议仍可能变化，须使用真实账号完成授权和调用验收。

## 订阅账号登录

新增账号会创建空的独立目录，不导入桌面 Codex 的现有凭据。点击“登录账号”后，Harbor 通过官方 Codex app-server 的设备码登录接口获取授权链接和一次性验证码，在管理弹窗中显示，并尝试在当前电脑打开官方页面。若浏览器阻止新窗口，点击“打开官方授权页面”，在官方页面登录并输入验证码；Harbor 不收集账号密码。个人账号需在 ChatGPT 安全设置中启用设备码登录，工作区账号需由管理员启用该权限。流程参考 [官方无桌面设备登录说明](https://learn.chatgpt.com/docs/auth#login-on-headless-devices) 和 [app-server 设备码协议](https://learn.chatgpt.com/docs/app-server#3b-log-in-with-chatgpt-device-code-flow)。

授权链接和验证码仅由管理接口返回，存放在进程内存中，不进入数据库、审计记录或网关日志。登录、状态检查、额度查询和推理统一指定独立 `CODEX_HOME` 与文件凭据存储，只使用该目录内本次官方授权生成的凭据，不回退到其他账号的缓存。停用账号不会在启动时自动检查或刷新额度。

登录等待时间为 10 分钟，以账号池显示“已连接”为准。启动失败时显示失败原因；弹窗持续检查实际状态，也可取消后重新登录。“稍后继续”只关闭弹窗，再次点击登录可查看仍有效的验证码。服务重启会中断等待中的登录，需要重新发起。成功、失败及取消结果记录在审计日志中。

账号卡片提供“移除账号”，确认后从账号池隐藏并永久停用，取消等待中的登录、结束旧会话绑定。存在处理或排队请求时拒绝移除。移除保留历史用量、请求、会话及账号独立目录，不删除订阅凭据或对话文件；这是逻辑移除，不等于从官方账号撤销授权。已移除账号不能再通过管理接口启用或登录。数据库仅新增可空的 `removed_at` 字段，旧镜像回滚仍兼容；回滚后移除账号保持停用状态，但旧页面可能再次显示该记录。

此前来源无法确认的本机测试账号已停用，新的真实测试须经过上述官方登录。Codex 桌面版本更新导致旧 CLI 路径失效时，启动会尝试寻找仍存在的桌面安装版本；自定义 CLI 路径保持原样。

## 账号调度与会话

账号自身的额度、重置卡及权重入口集中在账号管理。订阅额度耗尽后，管理员可恢复手动正权重，直接尝试使用上游支持的额外额度；不再要求配置额外额度总许可、账号许可或本地预算。自动分配和夜间计划不再依据历史额外预算创建候选，额外调用仅由有效手动正权重开启。账号停用、登录失效、官方明确的消费禁用、员工模型权限和共享限额仍生效；权重为0不接收新请求，已派发请求正常结束。上游实际是否接受及扣除何种额度由官方决定，失败请求不自动重放。

Codex 官方额度接口返回的余额、重置卡次数和套餐窗口分别保存。余额没有明确单位时不折算为USD；卡片数量采用官方总次数，不用可能截断的详情列表长度代替。管理页面提供官方权益查询，不要求人工填写卡库存、核验或预算。当前接入协议只有读取能力，没有实际兑换重置卡接口，因此不提供无效的重置按钮；在官方使用后刷新即可读取新状态。历史许可、预算与计量记录保留，不作为手动正权重调用的前置条件。回退旧版会重新启用旧许可门槛，回退前须核对受影响账号。

Codex客户端使用Responses接口。新会话按生效权重及协议、模型、账号状态选择可用账号，已有会话保持账号绑定；不同会话可以同时调用同一账号。官方套餐资格参与路由判断，手动正权重可覆盖套餐耗尽，员工共享限额和模型权限独立校验；Standard参考USD不等于官方余额。账号停用立即将全部绑定迁移到同类型可用账号，Codex按现有生效权重分配；没有可接收目标则整笔拒绝，不产生部分迁移。在途请求继续按原账号完成及计量，晚到结果不能覆盖新绑定；排队请求使用新绑定。请求新增可空 `route_version` 保存派发版本，启动恢复同样核对账号及版本，旧记录缺版本时至少核对账号。回退镜像须通过当前存储、路由版本隔离及人工额外额度使用方式的兼容检查，不能仅因能读取新增列就判为可回退。停用须人工启用，凌晨不会自动恢复；临时权重0仍在下次北京时间02:00恢复自动。移除与认证失效沿用原规则。

“系统管理”提供接入说明、请求测试和系统操作历史三个视图；未登录员工仍从“接入与测试”进入，仅可测试，不提供管理说明或操作历史。

停用账号迁移旧网页测试会话时，清空旧账号的CLI上下文及累计游标，下次测试从新上下文开始；不复制订阅凭据或对话目录。正常Codex客户端继续携带自身历史。

旧Chat Completions纯文本入口通过服务端CLI运行，保留原有有状态会话、串行及旧额度兼容逻辑，不用于Windows/WSL编程客户端。旧配置字段不删除，也不影响新入口。

## Codex Windows / WSL 客户端

使用员工Key通过93中转，客户端可以在其他机器上运行，无需复制服务器订阅凭据。完整配置见 [Windows与WSL接入说明](clients/README.md) 及 [TOML示例](clients/harbor.example.toml)。

`POST /v1/responses` 支持HTTP Responses增量SSE、非流式响应、函数和自定义工具项、工具结果及完整历史重放；原样路由实际模型名，直接交给上游验证，不依赖缓存目录预检，也不静默改成默认模型。`GET /v1/models`提供模型名及元数据，`GET /v1/codex/models`提供可保存的模型目录；两者均需员工Key。

客户端负责文件、终端和工具执行；服务器不执行客户端工具。员工身份与客户端线程标识共同定位会话并绑定订阅账号，同名客户端标识不能跨员工访问。Responses直接并行转发，不限制同员工、同Key或同账号一次只能处理一个请求；新会话按最近使用时间选择已登录账号。员工/Key本地额度、预占、账号预测容量不参与本入口准入，官方余量不作为本地预测准入门槛；可选的故障迁移与夜间分配见下文。旧文本兼容入口保留原有CLI状态管理和额度规则。

用量仅在上游提供时累加到Key和员工计数，用于大致了解累计消耗。正常完成但缺少用量时仍返回结果，另累计未计量次数；中断不阻止其他在途请求或下一次完整历史续聊，不自动重放失败请求。内部保留不含正文的诊断记录，后台使用记录按Key显示最近7天用量；内部旧文本计数保持兼容；鉴权、有效期、启停及删除规则继续生效。

该入口采用公开Responses API的128MiB传输边界并接收zstd压缩请求，不再设置8MiB事件上限；旧聊天和管理入口仍为128KiB。订阅上游使用官方固定Codex地址，客户端不能指定上游；员工Key不转发，订阅凭据不返回客户端。过期前由官方CLI刷新选定账号凭据。模型流仅内存转发，统计库不保存请求正文和工具结果。客户端可以按其自身策略保存会话内容。

当前使用HTTP完整历史，不支持WebSocket、background、store=true或previous_response_id增量续传，不伪装为完整OpenAI公共API。新增会话protocol/client_key兼容字段和唯一索引，旧版本仍可忽略；回滚会保留新链路的历史用量。

## 旧客户端文本协议

以下为原有 OpenAI Chat Completions **纯文本子集**。Codex编程客户端使用上一节的Responses入口；这里的codex是提供方别名。

```text
Base URL: http://127.0.0.1:43127/v1
API Key:  员工 Key（hg_ 开头）
Model:    codex
```

新会话请求：

```json
{"model":"codex","messages":[{"role":"user","content":"记住代号青柠47"}],"stream":false}
```

响应的 `session_id` 和 `X-Harbor-Session` 头返回本地会话 ID。后续请求必须复用它：

```json
{"model":"codex","session_id":"上次返回的ID","messages":[{"role":"user","content":"代号是什么？"}],"stream":false}
```

也可以通过请求头 `X-Harbor-Session` 传会话 ID。默认不提供 ID 即创建新会话，无法自动判断不传 ID 的客户端哪些请求属于同一对话。

续聊优先只传本轮新消息。如果传完整历史，平台会校验此前历史的摘要并只追加新消息；历史不一致返回 409，避免重复上下文。会话归属员工，同员工换 Key 后仍可继续，不同员工不能使用该会话。

`GET /v1/models` 返回本地模型名；`GET /api/me` 返回该员工额度。两者都需要 Bearer Key。`model` 可选择 `codex` 或 `claude` 提供方；每个提供方使用其 CLI 默认模型。

`stream: true` 返回 SSE 格式，但在上游完成后统一返回文本，并非逐 Token 流式输出。此旧入口不支持工具调用、图片、`max_tokens` 或 `max_completion_tokens`。需要Codex编程时使用Responses入口。

## 用量和数据

- 员工用量 = 本轮输入 + 本轮输出。缓存包含在输入里，不重复加算。
- Responses按每次响应的已知用量累加；旧文本CLI续聊按累计差值计算。
- Codex客户端不执行本地配额门禁或预占，缺失用量记为未计量次数，不以统计完整性阻断成功回复。
- 重启中断的Responses请求记为未计量，不锁住后续请求；旧文本入口仍按原规则恢复状态。
- 原有预占和逐请求记录保留在数据库供旧入口兼容；新后台展示Key最近7天值，不要求逐问题核对。
- IP 来自实际 TCP 连接，不信任客户端自行填写的转发头。本机测试显示 127.0.0.1，员工列表查看员工全部 Key 的 IP，员工详情可查看各个 Key 的独立 IP；IP 个数不等于设备数。
- `data/gateway.sqlite` 保存员工、Key 哈希及加密全文、统计、会话映射与摘要，不保存聊天正文。
- `runtime/accounts/` 保存各账号独立的官方认证及 Codex 会话上下文。为了真正续聊，官方会话文件会保留对话正文。
- `data/settings.json` 包含本机管理密钥。不要将 `data/` 或 `runtime/` 上传或分享给其他人。
- 后台“创建数据库备份”会生成 SQLite 一致性备份，位于 `data/backups/`；它不包含官方登录凭证和会话目录。完整迁移应停止服务后备份整个应用目录，单纯恢复数据库不能恢复上游上下文。

## 当前范围

服务器模式已部署93并接受受鉴权的远程客户端；本机模式默认仍只监听127.0.0.1。此前已验证一个真实 Codex 账号、文本多轮会话；多账号额度调度通过模拟不同额度的自动化测试验证。没有额外账号可用于真实多账号登录验收。

Claude 官方 CLI 适配已预留并通过模拟测试，使用独立认证目录、真实 session resume 和禁用工具模式；尚未使用真实 Claude 订阅验证登录、续聊或用量结算，界面明确标注待验证。Claude 当前只支持管理员设置的本地预算估算，不提供官方剩余额度读取。按本阶段安排，真实 Claude 验收留待公司账号可用时完成。Codex 的实测结果不能替代 Claude 验收。

## 开发验证

```powershell
node --test tests/*.test.mjs
```

测试覆盖鉴权、跨站请求、有效期、停用、限额、并发、Key 轮换、IP、防止未知用量丢失、备份、额度优先调度、会话绑定、跨员工隔离、历史校验和累计用量转换。测试数据库使用独立临时目录，不访问真实订阅。

2026-10-02 安全复查补充：管理 Token 缺失时拒绝 Bearer 鉴权；退出或登录过期后清除界面保留的 Key、聊天和统计，拒绝显示旧登录周期的迟到响应；CSV 导出补充前导空白和控制字符后的公式转义。详见 `安全复查.md`。这些防护针对当前仅本机部署；同一操作系统用户可读取本地数据文件，不属于应用登录所能隔离的边界。

参考：[Codex 身份验证](https://learn.chatgpt.com/docs/auth)、[CLI 非交互调用](https://learn.chatgpt.com/docs/non-interactive-mode)、[官方额度查询协议](https://learn.chatgpt.com/docs/app-server)。

### 并行转发版本的回滚

新增兼容字段requests.tracking_only和keys.unmetered，用于区分累计统计与旧文本预占。旧镜像可读取新增字段，但回滚将恢复旧的串行和额度准入限制；不会返还上游实际消耗。新版本重启会保留累计计数，并将缺少最终用量的在途请求标记为未计量。

### 最近7天统计与账号状态

管理页面、`/api/me`、来源IP次数、会话请求数和CSV导出统一为北京时间今天及前6个自然日，不按周重置。已完成请求按完成时间归属日，在途请求暂按开始时间。所有用量和夜间负载统一为Standard参考消耗USD：按模型普通输入、缓存读取、输出的官方Standard短上下文单价分别计算，非缓存输入先扣缓存，缓存不重复加算。底层保留原始计量和固定费率版本，统计DTO使用具名standard对象；页面仅显示参考USD，微小值显示低于展示精度，未知显示“—”。员工/账号数量及在途请求是实时状态，官方额度百分比保持官方窗口。

复用请求表按窗口聚合，新增`requests.usage_unknown`和时间表达式索引；启动、管理读取及30秒维护任务不再清零或删除窗口外计量、未知状态、人工金额及关联记录；页面窗口只限制查询。历史未知/中断迁移为未计量；旧版成功但缺失用量没有逐请求标记，不能可靠恢复其日期，不将无日期的累计次数分配到今天。旧CLI续聊差值、旧限额内部计数与协议返回的单次usage保留，不影响Responses并发准入。

新Key使用AES-256-GCM加密全文，`data/key-encryption.key`是独立32字节主密钥（新建权限0600），需与数据库分别妥善备份，数据库备份自身不含主密钥。迁移/恢复服务必须保留该文件；删除或替换会导致旧密文不可解密。现有哈希Key下次有效鉴权时自动补存，未补存时明确显示旧Key提示，不自动轮换。仅管理员状态接口返回完整Key，员工接口不返回；删除Key或员工同时清除密文和撤销摘要。两张用量表支持完整展示、复制与HTTP页面的剪贴板回退。

已连接账号的官方额度每30秒后台检查是否需要刷新，管理读取也触发合并刷新；失败节流并保留上次快照，超过有效期明确标为“上次官方额度·待刷新”，不伪造最新余额。查询不阻塞Responses。单次429、取消、超时不改变账号登录状态，明确认证错误才显示“需检查登录”；成功调用或官方额度读取恢复已连接。

会话页展示客户端 Codex Session，供与 `/status` 对照；网关隔离线程仅用于上游转发，不作为客户端 Session 展示。账号分配仍按最近使用，新会话分摊、原会话固定绑定。

回滚旧镜像兼容新增字段，但恢复旧统计及状态展示；已经清零的过期Token不能凭空恢复。数据库恢复仅用于必要事故处理，禁止重新激活已撤销Key。正式发布前创建一致性备份并核对真实账号和Key仍有效。


### Codex HTTP 转发兼容约定（2026-10-08 核对）

本约定约束Windows与WSL的`wire_api = "responses"`路径，后续修改必须同时检查本文和对应行为测试。网关提供鉴权、订阅账号路由和统计；模型、上下文、工具及重试策略由客户端与上游协商。不得以优化、保护额度或保证统计完整为由重新加入以下已取消的行为。

| 检查项 | 当前约定 | 回归证据 |
| --- | --- | --- |
| 请求大小与压缩 | 接收JSON或zstd；线上字节与解压后字节均不超过128MiB，展开比不超过100倍。非法/截断zstd返回400，超限返回413 | `tests/request-body.test.mjs`覆盖字节边界、压缩双重边界和失败；`tests/responses.test.mjs`通过真实HTTP入口发送超过8MiB的普通与压缩请求 |
| 并行和用量 | 同员工、同Key、同订阅账号直接并行；不加单任务锁、预占或本地额度门禁。缺少usage不阻断成功结果或后续会话 | 五路请求同时到达上游、五路SSE在完成前均开始输出；取消隔离、未知用量续聊及零配额测试 |
| 长任务 | 不设置2分钟总执行或下游背压倒计时；客户端断开和服务停止仍取消任务 | 假时钟推进10分钟后不主动取消，显式取消仍结束；停服并行取消测试 |
| 模型和参数 | 不按缓存模型目录拒绝请求，不替换模型；input、tools、instructions及新增字段交由上游验证，不截断历史或工具结果 | 目录外模型、扩展参数、完整工具循环、不透明推理/压缩项及大请求内容一致性测试 |
| 事件流 | 增量转发SSE，取消8MiB单事件上限；成功终止事件及用量保留 | 大事件、UTF-8/CRLF分片、增量输出及终止事件测试 |
| 上下文与重试 | 不新增Token上限、压缩阈值、摘要、裁剪或网关自动重放；不透明上下文项不解读、不改写 | 大请求中context_management与compaction项保留；函数调用/结果历史原样断言 |
| 身份和路由 | 员工Key只用于网关鉴权，上游凭据由服务器选定；线程与prompt_cache_key映射为员工隔离的稳定标识 | 凭据替换、跨员工隔离、会话亲和及注销/停用测试 |

依据：[官方网关兼容要求](https://learn.chatgpt.com/docs/enterprise/gateway-compatibility)、[公开Responses压缩传输规范](https://developers.openai.com/api/docs/guides/production-best-practices#compress-request-bodies)、[Codex配置参考](https://learn.chatgpt.com/docs/config-file/config-reference)。128MiB与100倍是公开API的传输规范，采用它们修复本平台过小的8MiB门槛；它们不是订阅后端或任意模型上下文窗口的保证，实际模型参数、Token窗口和官方限流由上游决定。压缩网络字节不减少模型Token。

时间边界须区分：Codex文档中的`stream_idle_timeout_ms`默认300000是**空闲**时间，不是总生成时间。平台已取消自加的2分钟生成限制和15秒收包限制，使用Node HTTP/fetch的底层连接默认行为；不宣称网络连接永不超时。模型目录、凭据刷新、管理登录等独立操作仍有超时，它们不作为生成时长或并发门禁。以后变更Node版本、代理或负载均衡时，须重新检查请求体、SSE缓冲、连接空闲与上传超时，不能只测健康接口。

本实现仍是无状态HTTP完整历史适配：要求JSON对象、非空model和布尔stream；发往订阅上游时使用`store=false`、`stream=true`，缺省instructions为空，客户端非流式请求在完成后返回JSON。显式store=true、background及previous_response_id模式未实现；WebSocket、独立`/v1/responses/compact`和正式Claude Messages协议也尚未支持。客户端通过现有Responses路径发来的压缩结果和参数保持原样。不得把这些范围外协议、旧Chat Completions的CLI桥接或仅模型列表可用，写成完整Codex/Claude兼容。

所有上述测试由`test_gateway`执行，且`image_gateway`依赖其通过。修改公共转发链路须运行`node --test tests/*.test.mjs`；新增代理、协议或客户端接入时补真实链路验证。测试只用独立临时数据库和模拟上游，不以大正文消耗真实订阅；已有Windows/WSL真实并行证据只覆盖当时版本，不能替代未来协议升级验收。禁止删除或放宽这些断言来接受回归。回滚沿用提交SHA镜像机制，本次无数据库迁移；回滚至旧镜像会恢复8MiB和2分钟限制，必须在回滚说明中明确。

## image2 图片适配

本轮仅新增生图与改图，通过本地 MCP 工具使用现有员工 Key，服务端固定转发 Codex 订阅图片接口，复用账号池、员工隔离、并发取消和用量统计。没有独立 OpenAI API Key 或付费备用入口，网页版聊天、记忆仍不接入。官方客户端原生生图的身份限制不通过修改客户端或伪造身份解除。

安装与停用方法见 [客户端图片 MCP 配置](clients/README.md#安装图片-mcp)。新增鉴权接口为 `GET /v1/codex/image-mcp.mjs`、`POST /v1/images/generations` 与 `POST /v1/images/edits`；改图仅接受内联图片，不接受跨账号 file_id 或远程图片地址。已通过现有订阅的真实生图、参考图片编辑、文件保存及用量统计验收。

本地检查：`node --test tests/*.test.mjs`；指定已安装官方 CLI 后运行 `HARBOR_CODEX_BINARY=/absolute/path/to/codex node --test tests/codex-client.test.mjs` 可验证真实客户端与模拟上游的完整 MCP 调用和文件保存。真实订阅生图、改图及客户端平台验证单独记录，不能由 Mock 替代。

## 会话手动路由与访问管理

“近7天活动会话”仅展示 Codex Session / 员工、绑定账号、近7天请求数、最近活动、操作。操作列始终只显示「管理会话」按钮，禁用、异常、旧协议等全部状态与说明只在管理弹窗显示。不采集、保存或展示会话名称及对话片段，也不提供改名接口。

活动会话使用服务端分页，默认每页25条，可切换50/100条并前后翻页；不再只取最近100条。`GET /api/admin/state` 支持正整数 `session_page`、`session_page_size`（最多100），返回 `sessionsPagination` 的 `page/pageSize/total/totalPages`。沿用近7个北京时间自然日活动范围，按最近活动和内部ID稳定排序；超出末页时返回有效末页，空列表为第1页。自动刷新保留页码，切换页大小回到首页；迟到响应不能覆盖新的翻页结果。分页加载期间暂不可打开旧行管理，会话弹窗打开时保留原管理对象；夜间预览直接返回每项的客户端标识，不依赖当前分页。回滚不涉及数据库结构，重新加载旧页面后恢复旧列表行为。

表格及管理弹窗中的 Codex Session 原样展示客户端请求携带的 UUID 会话标识。兼容新增可空 `sessions.client_session_id`；旧会话在下次携带同一标识请求时补齐，不反推既有哈希、不扫描对话内容。没有标识或非 UUID 时显示“未获取”，旧文本协议显示“旧 CLI 会话”，不以内部 ID 替代。客户端标识仅是展示元数据，不能作为鉴权依据；同一标识在不同员工下仍是独立会话，管理操作继续通过内部 ID 定位。切换账号不改变客户端 Session。回退到本次修改前的隐私版本仅恢复旧展示，新增列保留且可忽略。

管理员可在“管理会话”中修改绑定账号、禁止或恢复该会话访问。员工 Key 不能调用这些管理接口。禁止访问拒绝该会话的后续请求；已开始执行的请求允许完成，旧文本队列中尚未执行的请求在启动时拒绝。其他会话、员工及 Key 不受影响。恢复访问继续使用原绑定，不自动换号。

`PATCH /api/admin/sessions/:id` 接受布尔 `enabled`，或 `account_id` 与整数 `expected_route_version`。路由仅支持 Codex Responses 会话；目标须为已启用、未移除且已连接的 Codex 账号。旧 Chat Completions/CLI Session 的上下文依赖原账号目录，拒绝切换。会话或员工被停用时先恢复访问；该会话有运行/排队请求，或所属员工仍有请求正在收包准备时返回 409。路由版本过期也返回 409，要求刷新后重试；重复选择当前账号不改变版本。

默认调度仍是新会话选择账号、后续固定绑定。手动切换保持网关会话 ID、员工归属、客户端会话映射和历史请求不变，从下次请求起使用新账号，并生成新的上游隔离线程及缓存标识。历史用量不转移；不自动重发失败请求；启用负载均衡后，仅明确故障和夜间计划可以自动换绑，规则见下文。不透明推理/压缩项、工具结果和完整历史原样转发，跨订阅是否接受这些历史仍须以真实链路验收为准，不能通过丢弃上下文规避上游错误。

数据库兼容增加 `sessions.title`、`upstream_thread`、`route_version` 和 `enabled`，旧会话默认允许访问。历史 `title` 列仅作结构兼容保留，启动时清空所有非空标题，管理响应不返回该字段；旧客户端提交标题返回 400。历史备份恢复后必须先经新版清除标题，不得回滚启用会重新采集标题的版本。审计记录会话管理动作与内部 ID，不保存标题正文。新增字段不改变模型参数、额度口径或新会话分配策略。**旧镜像不认识会话禁用门禁，不能在存在禁用会话时直接回滚到功能前版本**；回滚镜像必须保留会话访问检查及路由字段的兼容读取，避免恢复被禁止的会话或错误复用上游线程。


## 会话负载均衡

适用 Codex Responses 及其会话绑定，旧 Chat/CLI 不迁移。管理员在“订阅账号池 → 负载均衡”分别开启按24小时额度预算分配、登录失效自动切换和夜间调整；新安装及升级默认均关闭，静默期默认 30 分钟。设置保存在服务端数据库，员工请求不能覆盖。关闭夜间只停止夜间迁移，权重仍在每日02:00重算；关闭登录切换保留已知登录阻断。

未开启新权重时，新会话沿用最久未使用的可用账号。开启后，新会话按下文的系统或临时手动权重分配。结构化 `usage_limit_reached` 或官方查询确认耗尽，仅作为自动权重归零的证据，不再强制阻断手动正权重账号，也不再选择0权重账号兜底。登录失效仍是硬阻断，仅接受明确的凭据失效代码或可信订阅端的 HTTP 401。普通 429、403、网络/刷新超时、取消、5xx 和未知错误不触发。凭据刷新故障与确认失效分开处理，不用错误文本匹配；原始上游错误正文不入库或返回管理界面。官方代码参考：[用量耗尽事件](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/endpoint/responses_websocket.rs)、[凭据刷新状态](https://github.com/openai/codex/blob/main/codex-rs/login/src/auth/manager.rs)。

生效权重为0的账号不接收新的 Responses 请求；已在执行的请求允许结束。绑定该账号的会话在下次空闲请求时只从正权重可用账号选择目标并持久化新绑定；正权重之间的比例变化不在白天批量改绑。失败请求不自动重放。同会话尚有在途请求返回 `session_busy`，全部权重为0或正权重账号全部不可用时返回 `no_account`，不突破手动0权重，不循环尝试。登录阻断仅在开启登录失效切换时迁移，但目标仍须有正权重。客户端 Session、员工隔离和完整历史不变；上游线程轮换，历史消耗不改归属。新鲜官方证据可恢复账号，旧代次查询不能覆盖新耗尽或登录阻断，恢复后不主动迁回原会话。

夜间按北京时间每日 02:00 执行一次，窗口为前日 10:00 至当日 02:00。正在执行（包括更早开始）、排队、收包准备、静默期内、访问停用或人工排除的会话保持固定，已有负载仍计入；计划期间新到的请求也保护。手动换绑或临时排除自动调整，仅保持至下次北京时间02:00，届时恢复参与自动规划；手动权重同样到期，夜间自动规则优先。关闭夜间迁移也不延长期限，重启或错过窗口后恢复过期状态；在途保护仍保留，不保证到期一定换号。历史无期限排除从最后一次人工操作审计推算到期时间，缺审计时使用创建时间，不使用自动迁移或请求更新时间续期。

夜间只比较同套餐及相同额度窗口的账号，先刷新额度；刷新失败、窗口已重置或数据过旧时跳过。开启新权重后使用同一套自动权重，否则保留各窗口最低余量比例。近期已知Standard参考消耗USD及同模型在途估计作为相对工作量，先计入固定负载。无可比样本的长任务不当零，而是跳过该组分配。同组账号负载占比高于目标的部分相加，得到整组偏差；不超过 **5个百分点**（含边界）即接受，不把多账号的误差分别放宽到5个百分点。已在容差内且偏差方向无需改善的组不迁移；若偏差落在低额度预算侧，仍尝试纠正，并要求完整结果在容差内且方向严格改善。超出容差时，从原过载账号按历史工作量由重到轻选择会话；每次确认包含这些会话的完整达标方案存在后，才列为必须迁移，允许其他会话反向补偿。选中会话按来源账号原超额量封顶计入释放量，覆盖必须释放的超额后停止追加。容差内纠正从原低预算超配账号按同样方式选择重会话，选中释放量覆盖其原正偏差后停止；固定负载无法释放时仍可保留方向改善的完整方案。随后在这些重会话必须迁移的前提下，先按额度预算档从低到高依次减少正偏差（同预算账号视为同档，最高档承接剩余偏差），再最小化总迁移数，最后同数量才比较整组偏差。方向优先于数量，允许多迁移；方向相同时不为追求零误差增加迁移。先计算完整计划，再执行；不会先迁移重会话再临时修补。线性候选改善后进行有计算预算的组合搜索，允许探索比现有方案更多的迁移；达到固定负载决定的方向下界后才收敛迁移数量。若重会话可行性尚未确认则跳过该组；已满足重会话优先但方向或数量优化耗尽预算时保留已找到的达标方案，分别标记是否已证明方向最优、整体优化是否完成，不宣称全局最少。已证明固定负载或会话不可拆分导致无法达标时，仅保留可改善偏差的计划，并明确标记无法达标及优先级未完成，不突破保护。仍每天只执行一批，每个会话本轮最多换绑一次；每个可比较账号组在短事务中复核并提交，候选变忙、人工改绑或额度快照跨过重置点等变化会取消该组整套迁移及对应审计，其他独立组不受影响。执行前保留静默期及路由版本检查，耗尽目标不接收新迁移，未执行项不在白天补迁。不承诺明日实际消耗均衡。参考工作量估计不作为Responses请求限额或串行门禁。

“按24小时额度预算分配”默认关闭，API 使用 `weighted` 字段，内部单独保存到 `gateway_settings.routing_strategy`，旧 `balancing` 记录只保留原三项设置，与登录切换、夜间调整分别控制。可比较的同套餐单窗口账号采用 `剩余百分比 / max(距重置小时数,24)`，再归一化为百分比；不扣固定预留，不按任务大小预测。24小时内将全部余量纳入预算，超过24小时按时间摊薄。自动比例持久化到 `routing_auto`，仅在每日北京时间02:00、确认耗尽或恢复、手动恢复自动时重算；首次启用及首次获得可靠额度完成初始化。普通正余额刷新、时间推进和服务重启不改写比例。后台沿用30秒查询，新建不额外同步启动额度查询。快照超过5分钟或越过重置点失效；重算遇到数据不足时保留已知比例及耗尽状态，待官方新证据补齐该次计算，不能仅凭时间推定满额。首次尚无可用自动比例且快照缺失或套餐不可比时显示原因并沿用原顺序，但仍排除已确认耗尽或不可用账号；夜间跳过不可比较组。正权重显示不舍入成0%或100%。

新会话平滑加权分配，选择、分配记账、会话绑定及请求归属在同一短事务内完成；同客户端正权重会话的后续请求不重复分配，保持直接并发。必要轮转状态保存在既有 `gateway_settings`，重启后继续使用；额度刷新不清空轮转状态。权重只决定路由，不冻结Token或增加本地预测准入门禁。旧 `quota_exhausted` 标记兼容解释为自动权重归零证据，手动正权重可正常使用，停用、移除及 `login_expired` 不可被权重覆盖。

管理员在账号池查看“系统权重”和“生效权重”，通过“临时设置权重”填写总和100%的百分比，最多两位小数。手动配置覆盖自动比例，允许给订阅额度为0的已登录账号设置正权重，使用官方支持自动扣除的额外额度；网关不估算或承诺额外额度余额。重复耗尽快照及错误不改写手动配置，也不强制换到手动0权重账号；官方拒绝时原样返回已脱敏错误，不自动重放。保存至下一次北京时间02:00到期且最长24小时；到期按时间独立判定，即使夜间关闭、漏跑或服务重启也不能延长。每日恢复自动后按自动权重计划，预览同样使用自动权重。停用、移除、登录失效仍不可用；全部正权重账号不可用时拒绝转发，恢复后不立即抢回已迁移会话。

`PATCH /api/admin/balancing/weights` 接受 `{mode:"manual",weights:{账号ID:百分比},expected_version:整数}`；恢复自动使用 `{mode:"auto",expected_version:整数}` 并重算自动比例。服务端生成到期时间；拒绝未知账号、无效总和、客户端伪造到期时间及过期版本。关闭 `weighted` 后新会话和夜间恢复旧顺序及余量比例策略，临时权重不生效、到期仍执行；保留现有绑定、登录失效开关和已知状态，不恢复额度耗尽绕过权重的旧兜底。设置记录为 `routing_strategy`、`routing_weights`、`routing_schedule`、`routing_auto`，不增加业务表结构；旧版可忽略新增记录，但回滚旧镜像会恢复其硬阻断及兜底语义，须先恢复自动并核对账号启停与额度，不能把新规则的手动额度覆盖视为旧版兼容行为。

“预览分配”只计算、不修改绑定，也不提交表单中尚未保存的开关。`POST /api/admin/balancing/preview` 返回计划、固定负载与跳过原因；`PATCH /api/admin/balancing` 接受 `failover`、`nightly`、`weighted`（布尔）及 `quietMinutes`（1–960 整数）；均要求管理员鉴权。会话 PATCH 支持布尔 `nightly_enabled`，false 仅临时生效，期限 `nightly_expires` 由服务端生成，客户端不可指定。定时任务有持久化日期唯一记录，重启不重复完成或中断的批次，错过 02:00 不白天补跑。

数据库兼容新增账号路由阻断与代次、会话夜间开关、服务端设置及调度记录。回滚先关闭两项自动策略；不能把已知阻断账号直接交给不识别阻断的旧镜像，须先停用对应账号。已形成的绑定继续有效，不批量倒回；回滚版本必须保留会话禁用、客户端 Session 和不采集标题的隐私契约。

### 最近用量与迁移选择

账号、员工、Key、会话、请求明细与分组统计、CSV及夜间预览统一使用Standard参考消耗USD，保留近7个北京时间自然日及滚动近12小时。服务端按请求完成时间归属，未完成请求使用开始时间；近12小时窗口为 `[当前时刻-12h,当前时刻)`。使用同一份逐请求计价数据跨维度聚合；缺计量和缺费率分别显示，不当0。问号说明可点击或用键盘打开。

夜间采用上述整组5个百分点容差和重会话优先规则，支持任意多个可比较账号；优先级为重会话、偏差方向、迁移数量。管理预览及夜间审计保留调整前后偏差、优先会话、无法纳入的原因、低预算各档调整前后正偏差、方向优化完成状态和计算受限状态，避免将“未证明可行”当成“不可行”。历史Standard参考消耗只是相对负载，不能保证未来或官方额度消耗相同比例。此次规划规则不新增配置或数据库列；旧审计没有规划说明时仍可读取。回滚到前一版本仍为5个百分点，但恢复重会话→迁移数量→偏差方向的优先级，以及容差内不迁移，已完成绑定不自动倒回。

本版新增可空 `sessions.nightly_expires`，旧镜像可读取数据库，但会忽略期限并恢复永久排除语义。回滚前通过管理接口恢复自动参与和自动权重，或优先发布修复镜像；不要把数据库列兼容视作行为兼容。

## 页面自动刷新

自动刷新只覆盖运行总览、账号池、会话绑定，以及无筛选的请求明细第一页。员工与Key配置、Key用量、操作历史、接入说明及请求测试不定时刷新；操作后仍更新结果，手动刷新始终可用。筛选或翻页后停止请求列表轮询，避免打断历史分析。

页面隐藏、弹窗打开或正在输入时暂停页面刷新；迟到的自动响应不能覆盖已切换的静态页。后台请求不先清空列表，无业务变化不重建页面。进行中的请求详情继续独立轮询，完成后停止；登录进度保留独立观察。

## Standard参考消耗与自动费率

参考消耗USD＝`((总输入－缓存读取)×普通输入价＋缓存读取×缓存价＋输出×输出价)÷1000000`，价格采用[官方Standard短上下文价](https://developers.openai.com/api/docs/pricing)。直接以USD比较模型，不中转为某个模型的等效Token。参考价条件统一，不包含Fast/长上下文、独立缓存写入附加费、工具和存储费用，不能称为实际账单、余额或订阅扣额；平台外请求仍不可见。缓存已包含在输入中；输出中的推理用量不再重复计入。

原始计量与计价分离，价格保留六位十进制精度并转换为整数，精确相加后再展示。页面金额、只读费率和CSV导出统一四舍五入保留两位小数，小额可能显示0.00；合计按未舍入数据计算，明细显示值相加可能与合计略有差异。底层计价、调度、接口精确值及费率编辑保留原精度。每个请求派发时固定不可变费率版本，优先使用上游明确模型；缺失时仅用明确的请求模型参考并标记来源，不猜`codex-auto-review`等别名。已计价请求不会因之后的费率更新重写；未知费率请求在数据补齐后可回算；执行期间补价的请求在完成时立即补算，原本有有效费率的在途请求仍保持派发版本。更早数据保留数据库，但不在页面或导出提供；旧版本已经清零的历史无法恢复，不把它当零消耗。汇总包含`known/unmetered/unpriced/pending/expired`覆盖信息，全部未知时合计为null。

PostgreSQL与原SQLite共用费率配置校验、官方解析及不可变版本生成。管理员费率入口支持查询、保存和立即检查；后台每分钟检查是否到期，官方同步仍按已保存开关和周期（默认开启、24小时）执行。官方读取失败保留上次有效版本，人工费率和有依据的模型映射不被覆盖。补价在独立有界批次进行，每次最多100条，只处理近7天中原先缺价且新版本已有明确费率的记录；已计价、人工核对、用量未知及缺失历史费率簿的记录不重写。费率状态汇总沿用管理查询池与有界队列，发布及补价使用核心短事务；关闭先等待已接纳费率操作，再释放共享查询资源。回退旧业务版本会停止新增的周期同步，已保存版本与已发生计量保留，无schema迁移。

客户端取消或下游写入失败时，如果当前已收到的同一数据缓冲中含唯一、完整且合法的终态用量，网关保留该真实用量并维持原取消／失败结果；不会读取下一分片、等待、重试或延长模型任务。半帧、冲突终态、无效用量及预算不足仍保持未知。构造参数`bufferedUsageMaxBytes`默认1MiB（0关闭）、`bufferedUsageMaxFrames`默认256，并共同受原输出预算约束。这项恢复不意味着正文完整，也不能补出历史上从未收到的用量或`codex-auto-review`实际模型。


“上游账号池→费率与口径”可查看价格、待计价模型、版本与同步状态。后台默认每24小时检查OpenAI官方HTTPS Markdown价格表，初始公开快照位于`pricing-data.mjs`；新模型在官方表有明确费率即可更新，不需要改代码。其他来源的明确模型由管理员维护费率。读取失败、格式变化或候选不完整时继续使用上次有效数据，不执行网页脚本，不接受客户端指定同步地址。关闭自动同步只停止更新；管理员仍可主动检查。

管理员可维护手动费率及明确模型映射。手动覆盖长期保留，自动来源不覆盖它，清除后恢复最后官方数据；映射必须给出有效时间和依据，默认仅影响保存后的请求，历史不能随意推断。权限及接口：`GET/PATCH /api/admin/pricing`，修改携带`expectedVersion`；`POST /api/admin/pricing/refresh`主动检查。所有变更仍受管理员鉴权。

所有统计DTO包含`standard`计价对象（`metric=openai-standard-short-usd-v1`、`currency=USD`、输入/输出/缓存/人工核对/总计、精确十进制值及版本）。缓存是输入的子集；总计＝输入＋输出＋人工核对。`/api/me`提供`standard/keyStandard`，不再返回旧原始Token统计。CSV按同口径输出全部窗口内记录，不再截取10000条；保持公式转义。客户端Responses/SSE和标准`usage`仍是官方原始协议，不能把参考美元写进token字段；旧网页聊天另带`harbor_standard_usage`元数据用于后台测试展示。

核对异常请求改传`standard_usd`（最多六位小数），记录整笔已核对参考额，旧`tokens`参数拒绝。人工核对单列，不虚构原始input或改成其已知；重复核对拒绝，窗口内预占可按原语义释放；窗口外详情和核对接口返回404，历史异常需通过数据库运维处理。旧版人工补记只记录总Token而缺少输入/输出分类，标记为不可可靠换算，可一次性用参考USD重新核对；夜间不使用其伪造分项。旧物理Token门禁/累计字段仅为旧CLI协议兼容，不进入新统计DTO；未接线的旧Token预算弹窗已移除，不把旧预算数值直接换标签。

夜间重会话排序、负载占比与迁移预览全部按参考USD；在途估计取同模型原始历史分项，按该请求固定版本换算后求中位数，不改历史账本。缺模型费率或可比样本的固定负载使该组跳过，不当零，也不退回原始Token。目标权重、5个百分点容差、迁移优先级与在途保护不变。新夜间记录标记metric与费率版本；旧记录缺少换算依据时隐藏旧负载数值，保留迁移记录和原因，不能冒充新口径。

数据库兼容增加requests的`price_version/billing_model/manual_cost_atoms`及不可变`pricing_versions`、费率设置。USD金额按固定版本从仍保留的原始分项精确推导，避免额外派生账本在旧镜像运行时失真；人工金额与原始计量、固定费率版本长期保留数据库。回滚旧镜像会恢复旧Token统计及旧负载，不代表口径兼容；原始协议与兼容列仍可读取。重新升级按原始计量和保留的版本重算，未知版本不会静默套现价。

历史保留版本无新增数据库结构。禁止直接回滚到会清零旧用量的镜像；优先发布保留此修复的兼容版本。备份保留供恢复事故，不为页面查询开放整库下载，也不新增对话正文采集。

## 员工按模型滚动用量限额

管理员在“员工与密钥→员工详情→模型用量限额”设置员工独立开关；默认关闭。开启后填写精确模型ID、滚动近24小时或近7×24小时、参考USD上限（最多两位小数）。同一模型可同时配置两个窗口，任一达到上限即拒绝后续新请求（429 `member_model_limit_exceeded`）；0表示暂停该模型。未配置的明确模型不受此限额限制，Key模型白名单仍独立执行。保存规则须使用有费率的规范模型ID，不能保存别名；存量别名按当前明确映射统一核算和拦截，映射不可确认时拒绝新请求并允许管理员修正规则或关闭。关闭保留上次保存规则，重新开启计入窗口内已有用量；不在凌晨清零，也不因更换Key、账号或会话重置。

同一员工所有Key共享消耗，按完成时间统计`(当前时刻−窗口时长, 当前时刻]`内的请求，以各请求固定费率版本精确计价；人工核对金额计入，不使用页面两位小数进行准入比较。当前规则不预估或预占未来USD：已派发请求继续完成，并发请求可能结算超额，之后的新请求会被拒绝；不承诺绝对不超额。滚动7天限额需要计算数据库中完整168小时的记录，可能包含使用记录页7个北京时间自然日之外的部分记录；历史明细查询范围保持原样。

限额开启时，请求模型须有明确费率或有证据的显式映射，未知别名及隐式图片模型工具拒绝，明确模型的图片接口仍受检查。固定版本缺失、缺计量或缺费率不当作免费：相关模型暂停新请求（409 `member_model_usage_unknown`），无法归属模型的记录会暂停该员工全部已配置限额的模型；可核对数据或等待其滚出窗口。API模型实际返回与请求不同时，完成用量按实际模型归属，准入只能根据请求时可确认的模型执行。该功能不改变账号池、夜间调度、并发转发或客户端协议。

管理接口为`GET/PATCH /api/admin/members/:id/usage-limits`，修改携带`expected_version`，规则格式`{model,hours:24|168,limit_usd:"20.00"}`；员工仅能通过`/api/me.usage_limits`查看自身限额。设置由服务端读取，客户端提交额度、员工标识或开关不能覆盖；请求准备及实际派发前都复核。配置和审计同事务，旧弹窗冲突返回409。数据库兼容新增`members.usage_limits/usage_limits_version`和请求员工完成时间索引，不删除历史。旧镜像不执行此限额，存在启用规则时不得直接回滚，应使用保留准入检查的兼容修复版本，或由管理员明确关闭后处理。

## 同库升级预检

同库 PostgreSQL 升级可由服务器 root 在 `/etc/harbor-gateway-deploy.json` 设置 `postgres_preflight_timeout_seconds`，默认 30，允许 30–300 的整数，控制每次新旧镜像预检的等待秒数。用于繁忙主机的容器启动等待，不改变关闭、健康检查及镜像身份校验；客户端不能覆盖。删除该配置恢复默认值。
