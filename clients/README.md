# Windows 与 WSL Codex 接入

Harbor负责发放和验证员工Key、选择已登录的订阅账号、转发模型协议及统计用量。客户端可以安装在其他电脑上；不需要复制服务器订阅凭据，不要求客户端登录服务器上的ChatGPT账号。上下文管理、压缩策略、文件操作、终端命令和工具调用均由客户端控制。客户端通过Responses发来的压缩结果及相关参数由网关转发，不主动压缩、生成摘要或接管客户端工作流；独立`/v1/responses/compact`接口尚未支持。

## 共同配置

将 [harbor.example.toml](harbor.example.toml) 合并到客户端用户配置，不覆盖已有规则或工具配置，不重复定义同名键和表。`model`、`model_provider`、`model_catalog_json`、`web_search` 必须放在第一个表头之前。模型名从带员工Key的 `GET /v1/models` 读取，按实际可用名称填写；`codex`是旧聊天接口的提供方别名，不用于本接口。

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

把第二行输出的配置放在 `config.toml` 的第一个表头之前（已有同名键则替换），并将顶层 `model` 改为 `"gpt-6.1-sol"`，然后重启Codex。目录文件不含Key；上游模型更新后重新下载。默认内置目录不会自动从网关刷新，单改 `model` 不保证下拉列表同步更新。

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

管理页面Token统一显示最近7个北京时间自然日的用量，单位亿，保留两位小数；底层整数精度不变。
