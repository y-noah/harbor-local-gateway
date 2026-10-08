# Harbor · 本地 AI 网关

Harbor为Codex客户端提供API Key鉴权、独立订阅账号管理、HTTP Responses转发和最近7天用量展示。文件、终端、工具和上下文策略由客户端控制。当前实现使用订阅账号的官方登录凭据，客户端无需持有这些凭据。

## 本地运行

需要Node.js 24.21或更新版本，以及官方Codex CLI。Windows可运行“启动 Harbor.cmd”和“停止 Harbor.cmd”；也可执行`npm start`。默认仅监听本机，首次启动生成管理密码找回文件，请在本机妥善保管。运行时没有npm依赖。

管理页面中新增订阅账号，通过官方授权页面完成登录；为员工创建Key后，按[Windows与WSL接入说明](clients/README.md)配置客户端。示例地址是本机地址，远程使用时替换成你自己的网关地址。同一Key和订阅账号支持多个会话并行，已有会话保持账号绑定；账号停用、Key过期或撤销仍会拒绝访问。

## 数据与密钥

`data/`、`runtime/`、管理密码、员工Key及账号授权文件是本机运行数据，禁止提交或分享。Key全文使用AES-256-GCM加密，独立主密钥`data/key-encryption.key`应与数据库分别备份；恢复时保留原主密钥。历史哈希Key在下次有效鉴权时补存全文，删除Key会永久撤销认证并清除密文。

管理统计统一为北京时间今天及前6个自然日，输入加输出、缓存不重复计入，以亿Token固定两位小数显示；底层保存整数。过期Token统计清零，诊断状态和关联保留。订阅官方额度使用官方窗口，并保留上次查询时间；临时请求失败不会直接判定登录失效。

## 远程配置

默认只允许本机访问。远程部署需要显式设置`HARBOR_LISTEN_HOST`、`HARBOR_PUBLIC_ORIGINS`和`HARBOR_PORT`；未配置允许Origin时禁止远程监听。根据自己的环境配置网络入口和持久化目录。本仓库不附带现成服务器、账号或部署凭据。

## 开发验证

运行`node --test tests/*.test.mjs`。测试使用独立临时数据库和模拟上游，不需要真实订阅。Windows专项测试在其他系统上跳过；跨平台发布时应补充目标系统验收。

旧`/v1/chat/completions`仅是CLI纯文本兼容入口，不用于Codex编程客户端。Claude适配尚待真实订阅与正式协议验收，不宣称已经支持完整Claude客户端。

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

发布前必须运行并通过上述行为测试。修改公共转发链路须运行`node --test tests/*.test.mjs`；新增代理、协议或客户端接入时补真实链路验证。测试只用独立临时数据库和模拟上游，不以大正文消耗真实订阅；不同客户端版本需要分别验证，不能以协议声明替代真实接入验收。禁止删除或放宽这些断言来接受回归。回滚旧版本可能恢复已取消的限制，应重新核对兼容约定。
