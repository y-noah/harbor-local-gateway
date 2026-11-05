# Windows / WSL 设备辅助接入

本目录是 PostgreSQL 设备绑定的本机辅助程序，需管理员配置 HTTPS/mTLS 接入、设备签发 CA 及登记入口后才能使用。它不提供上游账号，也不替代员工 Key。仅复制员工 Key 无法绕过强制设备策略。设备硬件摘要用于变化提示；不可据此承诺抵御本机管理员、虚拟机克隆或已失陷主机。

容量工具的独立测试构建可编译 `HARBOR_CAPACITY_OBSERVATION` 与 `CapacityDeliveryObservation.cs`，再显式给 `serve` 传 `--capacity-observe-delivery`；默认关闭，普通 `build.ps1` 不包含该观察模块。它仅对带合法 UUID 测量 trace 的 SSE，在实际 TCP 写入／flush 完成后采集 Windows QPC 计数、频率和进程观察 epoch，输出内容哈希、字节及顺序，不输出提问、回复、工具文本或凭据。该点表示辅助程序的 socket 写入完成，不代表客户端应用已消费，也不自动证明跨机时钟映射；输出始终 `formalQualified:false`，正式容量消费桥仍须核验实际来源。

测试观察输出使用128条有界异步队列，不等待stdout读取；拥塞或输出失败标记fault／丢失，不能视为完整测量。SSE待解析文本与JSON各限1MiB字符，单个内容限1MiB UTF-8字节，终态最多2048个内容项；坏编码、截断和超限只标记观察缺失，转发字节不改写。关闭测试构建／参数即可撤销观察，无数据库、设备身份或客户端配置变化。组件与实际CNG／WSL受控流分别验收，不能用合成组件通过替代完整链路。

已认证且带合法测量trace的请求，即使取消或返回非SSE而未建立探针，也输出`probeCreated:false`的未完成终态；它不说明上游未派发。控制器只通过自己持有的辅助进程通道取得事件，用前后QPC样本生成私有时间区间收据，并与登记设备及请求结果校验；缺收据不伪造精确时间。容量工具现可消费该收据测量socket写完阶段，完整正式来源及环境资格仍另行核验。

测试控制器如显式传 `--capacity-control-stdin`，可经自己持有的标准输入管道发送 `capacity-stop`，沿原有停止接纳和排空流程退出，再有界排空观察队列并输出终态。该入口需观察开关同时开启，只在测试构建存在，命令缓冲最多64字符，无HTTP控制路由；默认仍使用原Ctrl+C方式。stdout始终堵塞时，观察收尾最多等待1秒，缺少终态不能当完整测量。

## 构建、登记和运行

Windows x64 需要 .NET Framework 4.8，配置辅助工具另需 Node.js 24。PowerShell 运行 `./build.ps1` 将程序编译到当前用户 LocalAppData；不注册开机服务、不自动启动、不修改 Codex 配置，已存在的目标程序不会覆盖。

管理员在员工详情创建一次性邀请。将邀请安全地通过标准输入提供给 `HarborDevice.exe prepare --gateway https://你的设备接入域名 --name 设备名称`，不要把邀请写入参数、配置或命令历史。默认使用 Microsoft Platform Crypto Provider 的不可导出密钥；硬件不支持时明确报错，需要管理员接受软件保护边界后，才额外传 `--allow-software-key`。私钥留在 Windows CNG，服务端只收到 CSR、公钥和实例作用域摘要，不上传硬件原始编号。

管理员批准后执行 `HarborDevice.exe complete` 安装证书，执行 `HarborDevice.exe diagnose` 查看布尔状态与保护类型。首次调用验证成功后，管理员才可启用员工强制绑定。

`HarborDevice.exe serve` 在当前用户会话前台运行，仅监听 Windows 127.0.0.1；Ctrl+C 停止接受新连接并等待当前请求完成，最多五分钟后取消。本机请求头默认15秒超时，`--header-timeout-ms`可在1000—30000内配置，未认证慢连接不会永久占槽。没有自动直连或重新发送失败请求。辅助程序退出时客户端会报本机连接错误，重新运行并诊断即可恢复；设备批准与上游账号登录彼此独立，不要求重新登录上游账号。

设备登记状态保存在当前用户 LocalAppData/HarborDevice，目录使用当前用户ACL，登记凭据与本机认证令牌由当前Windows用户DPAPI保护。创建 CNG 密钥前会先保存受限权限的恢复状态；它不代表已登记或已批准。登记中断后，先停止辅助程序，再用相同 `--state` 执行 `remove` 清理，确认成功后重新登记；身份删除失败时保留状态文件，以便精确重试，勿直接删除目录。CA文件只用于管理员明确提供的受信任CA；仍验证服务端域名、证书链与有效期，不能使用关闭证书校验的启动方式。

## 配置 Codex

先在对应 Windows 或 WSL 终端，把 `HarborDevice.exe local-token` 的输出直接捕获到 `HARBOR_DEVICE_LOCAL_TOKEN` 环境变量，勿将其打印、粘贴到聊天或保存到工程；原员工 Key 环境变量保持不变。Windows桌面进程须在安全时机从已加载该环境的启动入口重新启动，现有任务不要强行中断。

`node configure.mjs plan --base-url http://127.0.0.1:43128/v1` 只展示目标文件、字段及变更前后摘要。核实后，用输出的原摘要执行 `node configure.mjs apply --base-url http://127.0.0.1:43128/v1 --expected-hash 摘要 --backup 受控备份路径`。默认识别实际 `CODEX_HOME/config.toml`，未设置时取当前用户 `.codex/config.toml`；可用 `--file` 指定已核实文件或 `--profile` 指定实际独立配置层。Harbor provider 非默认名时显式传 `--provider`。

只改该provider的base_url及env_http_headers.X-Harbor-Local-Token，模型、推理、其他provider、员工Key及MCP不覆盖。不支持或存在歧义的TOML写法明确拒绝，不猜测生效范围。备份只保存本工具涉及字段与摘要，路径应位于当前用户受控目录；如果旧base_url带敏感查询参数，不应分享备份。

Windows与WSL有独立的Codex配置入口，但共用同一个Windows设备身份。在WSL运行：`node wsl-bridge.mjs --helper /实际路径/HarborDevice.exe --state 'Windows状态文件绝对路径' --port 43129`，然后将WSL配置中的Harbor base_url改为 `http://127.0.0.1:43129/v1`。Windows辅助程序须同时运行。桥只监听WSL的127.0.0.1，验证本机token，拒绝浏览器Origin/Site等调用，使用Windows relay调用同一CNG私钥；不另生成WSL设备或导出PEM。

官方配置依据：[配置参考](https://developers.openai.com/codex/config-reference/)中的 `model_providers.<id>.base_url` / `env_http_headers`；[当前profile规则](https://learn.chatgpt.com/docs/config-file/config-advanced#profiles)使用 `<name>.config.toml`。实际客户端版本仍须集成验收，脚本写入成功不代表已有进程已生效。

## 模型目录和图片MCP

模型目录与固定图片MCP脚本下载也需要员工Key和设备证明，通过本机辅助入口的 `/v1/models`、`/v1/codex/models`、`/v1/codex/image-mcp.mjs` 访问。下载请求同时带员工Bearer Key和 `X-Harbor-Local-Token`；辅助程序清除本机令牌后，用同一设备证书向服务端认证。

图片MCP在Codex配置中另有独立入口：将该MCP的 `HARBOR_BASE_URL` 指向本机 `/v1` 地址，`env_vars` 同时保留 `HARBOR_API_KEY` 和 `HARBOR_DEVICE_LOCAL_TOKEN`。只修改模型provider不会同步修改已有MCP。令牌从辅助程序安全加载到进程环境，不写入TOML或工程。新版MCP仅向固定回环IP的HTTP地址发送本机令牌；WSL使用桥的本机端口，Windows使用serve端口。没有自动绕过设备策略的远程备用地址，生成／改图失败不自动重试。

## 续期、换机、恢复与移除

续期由管理员创建指定设备的邀请；标准输入传给 `HarborDevice.exe renew`，管理员重新批准后执行 `complete`。使用原不可导出密钥、保留设备ID及历史，旧代证书撤销。持续运行的辅助程序在下一请求前自动加载新证书，旧客户端仅供已派发请求完成，不重放。硬件复核只更新管理员核准的摘要，不无故更换证书；证书本身过期仍需办理续期。重装或硬件变化须重新核验，不能自动接受变化。续期响应丢失可由管理员取消待续期，再重新发邀请，保留当前有效证书及设备ID。

换机使用管理员明确关联原设备的登记邀请，新设备批准时原设备变为已替换，旧证书失效，历史不重写。设备停用立即阻止下一请求及尚未派发请求，不中断已经派发的模型生成。

`node configure.mjs restore --backup 受控备份路径` 按字段恢复，仅恢复仍与本工具写入值一致的字段，保留之后的人工修改，冲突明确报告。先安全停止辅助程序，再执行 `HarborDevice.exe remove` 删除本工具的证书、CNG密钥及状态；不操作其他账号凭据。恢复旧配置或本地移除不解除服务端强制策略，必须由管理员显式调整，不能依赖回滚旧网关绕过设备校验。

目前设备模块、真实Windows CNG及WSL桥链路使用隔离CA/PG验证；真实Codex客户端多轮工具、所有模型通道集成、安装发布包及共享服务器TLS部署须在主线完成验收，不能把模块测试当作生产已启用。

容量工具的专用编译可定义 `HARBOR_CAPACITY_OBSERVATION`，普通 `build.ps1` 不定义此符号。只有容量控制器为该专用程序显式传入 `--capacity-observe-http` 时，登记 HTTP 才通过受控标准输入／输出逐笔握手：控制器先登记方法、固定登记路径及独立 trace，返回确认后辅助程序才发送，再回报状态码和 HTTP 终态。协议不输出邀请、Cookie、正文、硬件原值或 CNG 私钥；真实 CNG 签名、TLS 校验和登记请求保持原链路。此能力用于隔离容量实验的来源取证，不作为正式客户端的配置项或容量通过证明。

专用构建还可显式开启 `--capacity-observe-delivery`，记录真实本机连接写入及 flush 完成后的 QPC、内容摘要和终态；该测点不表示客户端应用已读取。配合 `--capacity-control-stdin`，只接受有界的 `capacity-clock <UUID>` 采样与 `capacity-stop` 停止命令。容量设备适配器的 `observeDelivery` 默认关闭；开启后采集原 owned helper 的标准输出，并用事件前后的两次私有采样给出控制器时间范围，不直接换算或假设跨机时钟偏差。输出缺失、乱序、拥堵、时钟超时、进程退出及复制的事件对象均不能得到有效测量；采样和异步事件预算固定有界，正式资格始终为 false，实际镜像身份、远端拓扑和正式消费仍须组合验收。

关闭时先停止辅助程序并移除本次设备身份，再等待已接受的观测回调；回调挂起不阻止证书和 CNG 材料清理。外层观测等待默认五秒，超时明确报告未完成，底层 close 仍等待回调真正完成，不能把超时记作成功交付；回调内部触发关闭也纳入同一等待。
