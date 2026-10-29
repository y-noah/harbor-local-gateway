# Windows 与 WSL Codex 接入

网关负责发放和验证员工Key、选择已登录的订阅账号、转发模型协议及统计用量。客户端可以安装在其他电脑上；不需要复制服务器订阅凭据，不要求客户端登录服务器上的ChatGPT账号。上下文管理、压缩策略、文件操作、终端命令和工具调用均由客户端控制。客户端通过Responses发来的压缩结果及相关参数由网关转发，不主动压缩、生成摘要或接管客户端工作流。

## 共同配置

将 [harbor.example.toml](harbor.example.toml) 合并到客户端用户配置，不覆盖已有规则或工具配置，不重复定义同名键和表。`model`、`model_provider`、`model_catalog_json`、`web_search` 必须放在第一个表头之前。模型名从带员工Key的 `GET /v1/models` 读取，按实际可用名称填写；`codex`是旧聊天接口的提供方别名，不用于本接口。

示例默认使用 `gpt-6.1-sol`，可在 Codex 中切换；推理强度也按客户端选择透传。示例保留原联网搜索配置，关闭记忆生成和使用。

使用 `wire_api = "responses"` 和HTTP完整历史重放，关闭WebSocket。网关保留工具调用、调用结果及流式事件，不把工具操作放到服务器执行。MCP、插件及工具权限由客户端独立配置。

## Windows 桌面应用与原生命令行

用户配置位置为 `%USERPROFILE%\.codex\config.toml`。不要误放到工程的 `.codex` 目录；如果自己设置了 `CODEX_HOME`，以该目录为准。

用PowerShell在自己的电脑中录入员工Key，输入不回显且不包含在命令历史中：

```powershell
$secureKey = Read-Host "Harbor 员工 Key" -AsSecureString
$harborKey = [System.Net.NetworkCredential]::new("", $secureKey).Password
$env:HARBOR_API_KEY = $harborKey
[Environment]::SetEnvironmentVariable("HARBOR_API_KEY", $harborKey, "User")
Remove-Variable harborKey, secureKey
```

完全退出并重新启动桌面应用，使进程读取新的用户环境变量。已运行的应用或启动器可能保留旧环境；必要时重新登录Windows。原生命令行可直接在上述PowerShell窗口运行 `codex`。不要在TOML、工程或命令行参数中填写明文Key。

为使模型选择器显示网关的最新模型，在已设置Key的PowerShell中下载目录：

```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:43127/v1/codex/models' -Headers @{Authorization="Bearer $env:HARBOR_API_KEY"} -OutFile "$env:USERPROFILE\.codex\harbor-models.json"
"model_catalog_json = '$env:USERPROFILE\.codex\harbor-models.json'"
```

把第二行输出的配置放在 `config.toml` 的第一个表头之前（已有同名键则替换），然后重启Codex。示例默认使用 `gpt-6.1-sol`，可在 Codex 中切换其他可用模型，无需固定为它。目录文件不含Key；上游模型更新后重新下载。默认内置目录不会自动从网关刷新，单改 `model` 不保证下拉列表同步更新。

## WSL 命令行

用户配置位置为WSL内部的 `~/.codex/config.toml`。Windows和WSL默认使用不同的配置与环境变量，需要分别配置。

在WSL Bash中录入员工Key后启动：

```bash
read -rsp 'Harbor 员工 Key: ' HARBOR_API_KEY
printf '\n'
export HARBOR_API_KEY
codex
```

环境变量在当前终端及其子进程有效，新终端需重新加载。长期使用时通过组织批准的凭据工具加载，避免将Key写进工程或共享脚本。模型目录也需在WSL单独下载，并使用Linux绝对路径设置顶层 `model_catalog_json`：

```bash
curl --fail --silent --show-error -H "Authorization: Bearer $HARBOR_API_KEY" http://127.0.0.1:43127/v1/codex/models -o "$HOME/.codex/harbor-models.json"
printf "model_catalog_json = '%s/.codex/harbor-models.json'\n" "$HOME"
```

把输出的配置合并到第一个表头之前，然后重启客户端。

## 核验与错误定位

新建任务，确认客户端当前提供方为Harbor、模型为配置的实际模型。先发送短消息，再在可恢复的测试目录内读取文件、修改文件并运行断言，最后追问前一轮内容。管理员通过网关记录核对员工归属、模型路由与用量；仅收到文字不能证明工具或续聊可用。

401通常表示员工Key未正确传入；403检查Key停用或到期；Responses不按员工、Key额度或单账号串行限制返回429；上游429仍由管理员通过gateway_rejection日志的code识别；`upstream_authentication`由管理员检查账号池；`upstream_not_found`检查实际可用模型。同一Key和同一上游账号支持多会话直接并行。最近7天用量可能漏计中断请求，不影响其他并发请求或当前任务继续；网关不自动重放失败请求。

本接入不依赖某台开发机或单一版本号。协议兼容与已实测版本分开记录；不将未运行过的历史或未来版本写成已验证。


## 传输边界

平台不再附加8MiB请求/事件限制、2分钟生成时限或缓存模型目录白名单。Responses接收JSON与zstd，按公开API的128MiB传输边界和100倍展开比处理；上游的模型上下文限制、参数要求和429仍可能返回。完整约定、未支持模式及回归要求见[项目转发兼容约定](../README.md#codex-http-转发兼容约定2026-10-08-核对)。

管理页面统一显示最近7个北京时间自然日的Standard参考消耗（USD），按模型官方参考价计算；具体范围与未知数据处理见下方“用量展示口径”。

## 图片能力

本轮新增范围仅为 image2 生图、改图。通过本地标准 MCP 读取参考图片、保存生成结果，网关复用员工 Key、订阅账号池及用量统计转发 `/v1/images/generations` 和 `/v1/images/edits`。不需要 OPENAI_API_KEY，不接入独立计费 API；上游未返回用量时记录未计量，不估算。

2026-10-10 已用现有订阅完成 MCP 生图、参考图编辑及用量统计，并通过官方 Codex 0.160.1 在 Linux 中实际调用 MCP 生成和编辑图片。Windows 配置已通过官方 CLI 解析检查，尚未执行 Windows 原生运行验收。

仅平台 Key 登录的官方客户端无法启用原生生图工具，因此使用 MCP 工具入口，不复制订阅凭据、不伪造登录身份。网页版聊天和记忆仍不接入。搜索、模型切换、推理强度和上下文压缩不属于本轮新增，不新增独立搜索或压缩接口。

### 安装图片 MCP

每台客户端需要单独配置；原有员工 Key 未过期、停用或删除时可继续使用。仅部署网关或新开聊天不会自动添加工具。以下路径按默认 CODEX_HOME 编写；若自定义了该变量，下载位置和用户 config.toml 均改到实际目录。目录须事先存在。

客户端需要 Node.js 24。先按前文设置 `HARBOR_API_KEY`，从网关下载与服务端配套的脚本。Windows PowerShell：

```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:43127/v1/codex/image-mcp.mjs' -Headers @{Authorization="Bearer $env:HARBOR_API_KEY"} -OutFile "$env:USERPROFILE\.codex\harbor-image-mcp.mjs"
```

WSL：

```bash
curl --fail --silent --show-error -H "Authorization: Bearer $HARBOR_API_KEY" http://127.0.0.1:43127/v1/codex/image-mcp.mjs -o "$HOME/.codex/harbor-image-mcp.mjs"
```

在自己的 `config.toml` 添加以下配置。Windows 将两个路径换成自己的 Windows 绝对路径；WSL 使用 Linux 绝对路径。第二个参数为允许读写图片的工作区，目录须已存在；不建议设为整个用户目录。`command` 可填写 Node 24 的绝对路径，避免桌面应用找不到 PATH。

```toml
[mcp_servers.harbor_image]
command = "node"
args = ['C:\Users\YOUR_USER\.codex\harbor-image-mcp.mjs', 'C:\Work\YOUR_WORKSPACE']
env_vars = ["HARBOR_API_KEY"]
tool_timeout_sec = 600

[mcp_servers.harbor_image.env]
HARBOR_BASE_URL = "http://127.0.0.1:43127/v1"
```

重启 Codex 后确认 `harbor_image` 已连接。直接要求“用 Harbor 图片工具生成一张蓝色圆形图片，保存到 generated.png”，或“将 generated.png 中的圆形改成红色，保存到 edited.png”。改图时工具读取原图并提交内联图片，不依赖服务器文件 ID。结果返回预览和本地路径，输出文件必须尚不存在、父目录必须已存在，避免覆盖已有资产。工具权限仍由 Codex 管理，不自动修改用户审批策略。

完全退出并重启客户端后再新建会话。`codex mcp list` 可检查注册，实际工具列表中应出现 `harbor_image` 的 `image_generate`；仅注册成功不能替代真实调用。若仍提示“没有 image2 工具”或“缺少 OPENAI_API_KEY”，检查 MCP 是否连接、实际 CODEX_HOME、Node 路径、工作区是否存在及进程是否加载 HARBOR_API_KEY；不要误走独立 API 备用调用。

600 秒为客户端等待本次图片工具的上限，可按需要调整；取消或连接中断可能已消耗订阅额度，工具不会自动重发。检查图片是否已生成再决定是否重试。订阅额度用完、账号无权限或上游拒绝时直接返回错误，不切换付费 API。

本适配用于测试验证，技术可用不代表官方认可订阅转供。远程使用应配置 HTTPS，避免明文传输 Key 和图片；正式推广前需另行确认账号授权、平台验收和长期兼容性。

移除 `[mcp_servers.harbor_image]` 及其子表并重启 Codex即可停用客户端入口。服务端回滚上一镜像可撤回图片接口；本轮没有数据库迁移。

## 用量展示口径

Harbor后台、员工Key用量、请求统计、导出及夜间负载统一显示“Standard参考消耗（USD）”：按各模型官方Standard短上下文的普通输入、缓存读取和输出单价分别计算。它用于跨模型对比，不是实际扣款、订阅扣额或余额，不包含平台外调用及速度、长上下文、工具/存储等附加费。缺计量或费率会明确提示，不按0或相似模型猜算。

客户端与上游的`usage`、上下文窗口等协议参数仍采用官方原始单位，不修改客户端配置或把美元写进Token字段。模型元数据优先取上游明确返回值；`codex-auto-review`等别名不被自动视为Sol。后台费率可自动更新，已计价记录保留原费率版本；统计口径与自动同步状态见“上游账号池→费率与口径”。
