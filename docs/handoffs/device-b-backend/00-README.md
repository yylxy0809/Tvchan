# 设备 B 后端无缝交接总索引

> 审计基线：2026-07-11，分支 `master`，基准提交 `28d87f68cf6981544b0a02505ba72893905a2a7e`。
>
> 重要：当前工作树包含大量未提交开发成果。本交接包按“保留并盘点”处理，没有删除、回滚或清理任何用户修改。设备 B 不应只克隆上述基准提交，否则会丢失本交接包描述的大量代码。

## 交接结论

设备 B 将重新下载并导入历史 K 线，不迁移设备 A 的数据库。设备 A 后续主要负责前端；设备 B 承担 PostgreSQL/TimescaleDB、采集、API、Module C、策略生命周期和全量重算。

当前后端不是一个已经收口的发布版本，而是一次大型合并改造中的工作树：独立 `chan-service` 和旧 Model B 已移除，Module C 已并入 collector；窗口化图表 API、实时尾部计算和策略服务已实现大量代码，但尚未形成可供设备 B 直接拉取的提交。

## 阅读顺序

1. [01-未提交修改盘点](01-uncommitted-backend-change-inventory.md)：先确认需要带到 B 的代码范围。
2. [02-后端架构与功能状态](02-backend-architecture-feature-status.md)：理解服务边界和已知缺口。
3. [03-工具链依赖与部署](03-toolchain-dependencies-deployment.md)：在 B 上搭建 Docker 和 Python 环境。
4. [04-K线数据库与数据质量合同](04-kline-database-and-data-contract.md)：重新下载、导入和补采 K 线时必须遵守。
5. [05-Module C 与缠论存储](05-module-c-engine-and-chan-storage.md)：五级别全量重算的核心说明。
6. [06-周日共振策略与生命周期](06-strategy-weekly-daily-b2-status.md)：策略规则、Phase 1.21 结果和后续数据要求。
7. [07-前后端 API 合同](07-api-frontend-contracts.md)：设备 A 前端访问设备 B 的固定合同。
8. [08-设备 B 启动清单](08-device-b-bootstrap-and-next-work.md)：按顺序执行的落地步骤和验收门。
9. [09-策略生命周期推荐表结构](09-strategy-lifecycle-recommended-schema.md)：在 B 新库落地事件可见性和历史回放。

## 不得混淆的事实

- `klines` 是行情真相；`chart_period_bars` 只是周/月图表读缓存。
- Module C 五级别必须分别读取 `5f/30f/1d/1w/1m` 的原生周期 K 线。
- 当前有效 Module C 语义为 `bi_strict=false` 且 `bi_allow_sub_peak=false`，即“新笔”且不允许次高/次低成笔。
- 缠论结构表中的 `ts/base_ts` 是结构发生时间，不等于生命周期的 `first_seen_time/confirm_time`。
- Phase 1.21 当前结果是诊断研究，不是正式回测：官方可用标的为 0，策略有效性尚未得到证明。
- 历史数据库不迁移，因此本文件引用的 A 设备旧水位仅可用于容量估算，不能作为 B 的验收结果。

## 代码交付前置条件

在设备 B 开始开发前，设备 A 必须把当前工作树保存成一个可复现的 Git 提交或补丁包。推荐顺序：

1. 完成当前 K 线去重/缺失与前后端通信工作。
2. 将后端、协议、SQL、部署和策略服务作为一个明确提交保存。
3. 将前端另行提交，避免 B 无意覆盖 A 的 UI 开发。
4. 在 B 上从该提交创建新分支，不从旧基准提交猜测或手工拼文件。

旧文档 `docs/runbooks/2026-07-10-device-b-backend-migration-module-c-rebuild-handoff.md` 中“迁移旧数据库”的方案已经被本方案取代；其中 Docker 和部署背景仍可参考，但不得再执行数据库复制步骤。

## SMB 源码交接信息

### 当前共享

本机 Windows 已启用系统 `Users` SMB 共享，项目位于该共享之下。由于当前 Codex 进程没有管理员权限，无法新建独立的 `TVBackendSource` 共享；本次直接复用已经存在并验证可见的 `Users` 共享。

| 项目 | 值 |
|---|---|
| 设备 A 主机名 | `ZOE` |
| 设备 A 当前 WLAN IPv4 | `192.168.1.8` |
| SMB 端口 | `445` |
| 共享名 | `Users` |
| 登录账户 | `ZOE\yangyang` |
| 项目 UNC | `\\192.168.1.8\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f` |
| 主机名 UNC 备用 | `\\ZOE\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f` |

密码不写入文档。B 第一次访问时由用户输入设备 A 的 Windows 账户密码。不要使用 PIN 代替账户密码。

设备 B 之前记录的地址是 `192.168.5.197`，而设备 A 当前是 `192.168.1.8`，两者不在同一 IPv4 子网。A 到 B 的 TCP 445 可通过本机 `Meta` 虚拟网络到达，但 B 到 A 的反向访问尚未验证。B 应先执行：

```powershell
Test-NetConnection 192.168.1.8 -Port 445
```

如果失败，先让两台设备连接同一 SSID/同一子网，或配置双方都能访问的 Tailscale/现有虚拟网络；不要把 SMB 445 暴露到公网。

### B 设备迁移步骤

在 B 的 PowerShell 中：

```powershell
# 1. 验证连通性
Test-NetConnection 192.168.1.8 -Port 445

# 2. 建立临时映射；系统会安全提示输入 ZOE\yangyang 的密码
New-PSDrive -Name ABackend -PSProvider FileSystem `
  -Root "\\192.168.1.8\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f" `
  -Credential "ZOE\yangyang"

# 3. 从共享工作树的 Git 仓库克隆已提交内容到 B 本地盘
git clone "\\192.168.1.8\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f" `
  "D:\tv-backend\repo"

# 4. 核验交接分支与最新提交
Set-Location "D:\tv-backend\repo"
git status
git log -3 --oneline

# 5. 阅读交接总索引
Get-Content "docs\handoffs\device-b-backend\00-README.md"
```

若 `git clone` 因 Windows 凭据缓存失败，先在资源管理器打开项目 UNC 并完成登录，或使用：

```powershell
cmdkey /add:192.168.1.8 /user:ZOE\yangyang /pass:*
```

系统会提示输入密码。迁移完成后可执行 `cmdkey /delete:192.168.1.8` 清除凭据。

### SMB 使用边界

- 必须 `git clone` 到 B 本地磁盘后开发，不能直接在 UNC 共享目录运行 Docker、数据库或修改代码。
- 当前 `Users` 是系统级共享，B 账户通过认证后可能具备写权限。B 上的 Codex 只允许读取/克隆，不得在 UNC 原目录执行 `git checkout/reset/clean` 或文件编辑。
- Git clone 只迁移已经提交的版本，不会带走 A 上未提交的前端修改，这正是本次前后端分工所需。
- 数据库、`.env`、密码、缓存、Docker volume 和历史 K 线不通过此 SMB 源码共享迁移。
- 迁移完成并核验 B 本地仓库后，可以断开映射：`Remove-PSDrive ABackend`。
