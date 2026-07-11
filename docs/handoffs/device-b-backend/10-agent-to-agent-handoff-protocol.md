# 设备 A / B Codex Agent 交接通信协议

## 1. 能力边界

Codex agent 不能仅凭局域网 IP 自动发现另一台设备上的 agent。最优路径是两台设备登录同一 Codex 账户、设备 B 作为可用远程 host 出现在 Codex 中；此时设备 A 可以读取 B 的任务并通过 thread id 发送消息。

当前审计时，设备 A 的 Codex 只发现 `local` host，没有发现设备 B 的项目或任务，因此暂时不能使用 Codex 内建 thread messaging 直接对话。

在 B host 出现前，使用 SMB 文件邮箱作为可靠的异步通信通道。

## 2. SMB 邮箱位置

设备 A 本地：

```text
C:\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f\work\agent-handoff
```

设备 B 通过 SMB：

```text
\\192.168.1.8\Users\yangyang\Documents\Codex\2026-06-13\tradingview-tradingview-a-5f-15f-30f\work\agent-handoff
```

该目录被 Git 忽略，不会污染源码提交。B agent 只能在该目录写通信文件，不得直接修改 A 的源码工作树。

## 3. 文件协议

| 文件 | 写入方 | 用途 |
|---|---|---|
| `A-REQUEST.md` | A agent | 当前交接要求、commit、验收问题 |
| `B-RESPONSE.md` | B agent | 克隆、环境、测试、数据路径和问题回报 |
| `A-FOLLOWUP.md` | A agent | 对 B 回报的复核与下一步 |
| `B-FINAL-ACK.md` | B agent | 最终接管确认 |
| `STATUS.json` | 双方按协议更新 | 机器可读状态和序号 |

每次写入必须包含：`message_id`、`in_reply_to`、UTC 时间、agent/设备、仓库 HEAD、结论、证据、阻塞。不得写密码、token、cookie 或数据库凭据。

## 4. B agent 首条指令

用户在 B 的 Codex 中发送：

```text
你负责接管 TradingView A 股项目后端。先通过 SMB 克隆共享仓库，阅读 docs/handoffs/device-b-backend/00-README.md 和 01-10 全部文档，再读取 SMB 邮箱 work/agent-handoff/A-REQUEST.md。不要修改 A 的 UNC 源码目录，只在 B 本地 clone 开发；仅允许向 SMB 邮箱写 B-RESPONSE.md、B-FINAL-ACK.md 和 STATUS.json。完成 REQUEST 中的核验后，把实际 commit、环境、测试结果、数据库规划、发现的偏差和阻塞写入 B-RESPONSE.md。若你的 Codex task/thread id 可见，也一并写入，供设备 A 直接消息沟通。
```

## 5. 握手验收

B 的 `B-RESPONSE.md` 至少证明：

1. SMB 445 连通，仓库克隆到 B 本地磁盘。
2. HEAD 包含后端实现提交 `d2c4065` 和最新交接文档提交。
3. B 本地创建独立开发分支，初始工作树干净。
4. Docker、Compose、Python、磁盘目录和 chan.py 路径已确认。
5. 理解不迁移 A 数据库、B 重新下载 K 线。
6. 理解 `bi_strict=false`、`bi_allow_sub_peak=false` 和五级别原生 K 线合同。
7. 已列出 migration 022、published head 完整性、生命周期和 API 投影等优先问题。
8. 不会把 diagnostic 结果冒充正式回测。

A agent 复核通过后写 `A-FOLLOWUP.md`；B 完成 follow-up 后写 `B-FINAL-ACK.md`。只有 final ack 内容与 B 本地证据一致，交接才视为完成。

## 6. 切换到 Codex 内建消息

如果 B 使用相同 Codex 账户并在 A 的 `list_threads/list_projects` 中出现：

1. B 把 task/thread id 和 host id 写入 `B-RESPONSE.md`。
2. A 使用 `read_thread` 核验 B 的实际进度。
3. A 使用 `send_message_to_thread` 发送整改或确认。
4. SMB 邮箱继续作为不可丢失的审计记录，不因直接消息而停用。

不要用多 agent 子代理 id 代替跨设备 thread id；本机子代理只属于其创建线程，无法凭 agent nickname 在另一台设备发现。
