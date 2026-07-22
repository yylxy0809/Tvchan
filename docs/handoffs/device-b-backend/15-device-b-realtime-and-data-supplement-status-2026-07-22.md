# 设备 B 实时链路与数据补采状态（2026-07-22）

本文记录 2026-07-22 设备 B 的可复核状态、已经合入的开发成果，以及本轮增量 K 线补采的执行边界。运行数据、日志、数据库文件和凭据不进入 Git。

## 当前结论

- `master` 基线为 `b82afc6`（PR #114），检查时没有开放 PR。
- 前端、API、实时 K 线、Module C 缠论画线和信号生命周期链路已在受控标的上连通。
- 全量 Module C baseline batch 13 已封存：`16501` 个 eligible 任务完成、`9544` 个任务按冻结 eligibility 排除、失败为 `0`。
- 20 标 canary batch 12 已封存：`100/100` 完成、失败为 `0`。
- `605003.SH` 定向补算 batch 16 已封存：`5/5` 完成、失败为 `0`；浏览器可显示 `5f/30f/1d` 的笔、线段、中枢和信号。
- 生命周期 outbox 共 `125937` 条，全部为 `completed`；主 observer `chan-lifecycle-v1` 水位为 `125937`。
- 正式策略仍为 `NO_GO`。本轮只补 K 线，不启动 historical replay 或正式回测。

## 已完成开发

### 数据真值与门禁

- PR #79-#81：迁移、bounded claims 和 resume catalog fence 已完成。
- PR #83-#86：selection 重验、隔离数据 supersession、不可获取标的永久停用，以及 inactive 标的 fail-closed 已完成。
- 325 个经审计确认缺少必要原始数据的标的采用 append-only 方式停用；没有删除 K 线、历史运行、head 或 quarantine 证据。
- PR #105：无实际边界变化的重复轮询不再改写 catalog 时间戳。
- PR #106：严格审计接受正式的腾讯 HTTPS `source=6`，周/月来源规则保持不变。
- PR #107-#112：strict supplemental eligibility、范围绑定重验和 no-op/equivalent head 封存语义完成。

### Module C 与实时前端

- PR #89-#99：实时 API、WebSocket、Bearer 子协议、producer 取消等待和浏览器联动完成。
- PR #104：overlay 更新改为同一 TradingView study 原地刷新，失败时才执行安全回退。
- PR #113-#114：开放 K 线或刚收盘的受控宽限期内，只允许显示恰好落后一个 `change_version` 的上一版 overlay；其余版本、边界、配置或运行异常继续 fail-closed。
- 实测 API overlay 常态约 `26-31 ms`；`302132.SZ` 的 `5f` 浏览器切换约 `497 ms`，并已验证跨刷新边界不整幅空白。

## 本轮数据补采

### 范围

- 初始启动时间：2026-07-22 18:04（Asia/Shanghai）；18:19 切换为可观测的独立命名容器和分周期窗口。
- 数据源：腾讯 HTTPS；不修改或断开 VPN，不依赖不稳定的通达信/PyTDX 链路。
- 标的范围：数据库中全部 `is_active=true` 的沪深标的，共 `5209` 个，其中 SH `2315`、SZ `2894`。
- 已经永久停用的 325 个不可获取标的不进入补采。
- 周期：`5f/30f/1d`。
- 目标：从全量基线的 2026-07-17 收盘补到 2026-07-22 收盘 `2026-07-22T07:00:00Z`。
- 依次读取 `5f=180`、`30f=30`、`1d=5` 根；窗口覆盖 7 月 20、21、22 三个交易日的对应缺口，并避免对高周期重复读取 180 根历史。
- 并发：`4`，保持为受控的四路写入。
- 使用 `--skip-publish`，补采期间不向 Redis 广播，不触发全市场实时缠论重算。

### 启动前 canary

先对 `000001.SZ`、`000002.SZ`、`600000.SH`、`600519.SH`、`605003.SH` 执行相同来源和三周期补采。五个标的的 `5f/30f/1d` 均到达 2026-07-22 收盘，数据库锁等待为 `0`，随后才启动全量任务。

### 当前运行身份

补采运行在独立命名容器 `tv_market_fill_tencent_supplement_20260722` 中。三个命令顺序执行，任一时刻总并发保持为 `4`：

```text
python -m collector.market_fill
  --provider tencent
  --symbols-from-db
  --symbol-limit 0
  --timeframes 5f
  --limit 180
  --concurrency 4
  --sleep 0.05
  --skip-publish

python -m collector.market_fill
  --provider tencent
  --symbols-from-db
  --symbol-limit 0
  --timeframes 30f
  --limit 30
  --concurrency 4
  --sleep 0.05
  --skip-publish

python -m collector.market_fill
  --provider tencent
  --symbols-from-db
  --symbol-limit 0
  --timeframes 1d
  --limit 5
  --concurrency 4
  --sleep 0.05
  --skip-publish
```

相较三个周期统一读取 180 根，分周期窗口把计划读取量从约 281 万根降到约 112 万根，减少约 60%。这是幂等 upsert；若宿主机或容器在完成前重启，可使用相同冻结参数重新执行，已经写入的有效数据不得删除。

## 补采完成门禁

只有以下项目全部通过，才能把本轮补采标记为完成：

1. 5209 个 active 标的的 `5f/30f/1d` 精确水位均达到 2026-07-22 收盘；缺数据标的单独记录并保持 excluded，不伪造数据。
2. 无 future/invalid bar、时间戳格式错误或来源规则异常。
3. 原有 K 线历史未被降级覆盖，catalog generation 仍为 `2188f14c-0b35-416d-9671-fd3d227d1f75`、revision 仍为 `1`，只允许目标 scope 的边界合法推进。
4. 数据库无超过 5 秒锁等待，F 盘可用空间不少于 15 GB，服务 RSS 不超过 1.2 GB。
5. outbox 与 observer 无积压；本轮 `--skip-publish` 不应制造新的 Module C 生命周期事件。
6. 为新边界重新生成严格审计和 eligibility。旧审计不能替代新数据边界的验收证据。

## 后续顺序

1. 等待本轮补采结束，输出精确成功、缺失和失败清单。
2. 验证 K 线水位、格式、来源、catalog、锁、磁盘和历史指纹。
3. 对新边界执行五级严格审计并生成新的 strict eligibility。
4. 先做少量标的的 Module C 增量 canary，并核对唯一 observer、outbox 和生命周期 reconciliation。
5. canary 通过后，再以静态分片补算全量 Module C；正式策略仍保持 `NO_GO`，直到独立的策略门禁通过。

## 禁止事项

- 不重开已封存的 batch 6、9、12、13、16。
- 不删除或重建有效的数据库、K 线、run、head、quarantine 或 supersession 证据。
- 不恢复 Module B、`CHAN_SERVICE_URL` 或 fallback，不修改 vendored `chan.py`。
- 不提交 `.env`、密码、token、日志、数据库、outputs 或图表静态产物。
- 不运行 historical replay 或正式回测。
