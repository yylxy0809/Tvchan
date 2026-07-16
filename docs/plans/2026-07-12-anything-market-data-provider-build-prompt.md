# Anything 云端行情与资讯 Provider 开发提示词

下面整段内容可直接发送给 Anything Builder Agent。先要求它进入 Plan/Discussion 模式确认方案，再执行开发。

---

## 角色与目标

你是一名资深金融数据后端工程师。请在 Anything 云端构建一个可部署、可测试、带 OpenAPI 文档的 A 股行情与资讯聚合服务。该服务只作为数据 Provider，不开发交易前端，不做选股，不计算缠论，不生成投资建议。

服务的消费者是一个已有的 A 股 TradingView 项目。消费者需要通过一次统一请求获得：关注列表实时行情、当前图表标的资料、估值与活跃度、个股资金流、板块与概念、市场强度、强势标的、市场主题、个股新闻。

现有项目中的 K 线、缠论状态和策略信号不由本服务提供：

- K 线来自消费者本地 canonical K 线数据库。
- `chan_state` 和 `strategy_signals` 来自消费者本地 Module C/策略数据库。
- 本服务不得返回或伪造 `chan_state`、`strategy_signals`。

## 强制工程原则

1. 后端优先调用具有授权的结构化 API；只有 API 缺字段时，才抓取公开网页。
2. 抓取必须遵守目标网站服务条款、robots、访问频率和版权要求；不得绕过登录、验证码、反爬或付费墙。
3. 不返回新闻正文，只返回标题、链接、来源、时间、相关标的。
4. 不得让大模型在每次请求中临时理解网页。必须为每种来源编写确定性的 adapter、字段映射和 schema 校验。
5. 所有密钥只放 Anything Secrets；不得出现在代码、日志、响应或前端。
6. 所有时间使用带时区 ISO 8601；业务时区固定 `Asia/Shanghai`。
7. 股票代码统一为 `000001.SZ`、`600000.SH`、`920xxx.BJ`。
8. 金额统一使用人民币元，成交量统一为股，比例统一使用百分数数值，例如 `2.35` 表示 `2.35%`。
9. 缺字段必须返回 `null`，不得用 `0`、空字符串或推测值替代。
10. 每个字段必须保留来源和观测时间；不同来源冲突时不得静默覆盖。

## 数据源规划

### 第一优先级：Tushare Pro 结构化接口

使用 Tushare Token，并在启动时检查账号对每个接口的权限。至少研究和封装以下接口；接口不可用时必须在 `/v1/capabilities` 中明确标记：

- 股票身份：`stock_basic`、`stock_company`。
- 实时行情：A 股实时日线/实时分钟接口；返回最新价、昨收、涨跌额、涨跌幅、成交量、成交额、更新时间。
- 日度估值：`daily_basic`；总市值、流通市值、市盈率、市净率、换手率。
- 个股资金：`moneyflow_ths`、`moneyflow_dc`。注意 `moneyflow_ths` 是盘后更新，不能伪装成盘中实时资金流。
- 行业/概念：`ths_index`、`ths_daily`、`ths_member`，以及东方财富概念板块、概念成分、板块行情对应接口。
- 板块资金：同花顺板块/行业资金流、东方财富板块/行业资金流对应接口。
- 市场统计：实时排名、涨跌停统计、主要指数实时行情。
- 新闻：`major_news`。优先来源包括同花顺、新浪财经、第一财经、财联社、中证网、新华网、凤凰财经、华尔街见闻；只保留标题、发布时间、来源和可访问链接。
- 交易日：`trade_cal`，不得简单按周一至周五推算中国交易日。

### 第二优先级：公开网页补充

仅在合法、稳定且无需登录时使用，并为每个站点设置独立限速、User-Agent、超时和熔断：

- 同花顺个股页：`https://stockpage.10jqka.com.cn/{6位代码}/`。
  - 首页概览：最新行情摘要。
  - `资金流向`：个股资金流。
  - `新闻公告`：个股新闻和公告标题、链接、时间。
  - `公司资料`、`经营分析`、`行业分析`：行业、主营业务等低频资料。
- 同花顺概念板块中心：`https://q.10jqka.com.cn/gn/`。
  - 概念板块列表、涨跌幅、成分和资金流。
- 同花顺股票频道：`https://stock.10jqka.com.cn/`。
  - 公司新闻、行业新闻、行业资金、个股资金、涨跌排行。
- 东方财富行情中心：`https://quote.eastmoney.com/center/`。
  - A 股实时行情、涨跌排行、成交量和成交额。
- 东方财富个股行情页：`https://quote.eastmoney.com/{交易所前缀}{6位代码}.html`。
  - 个股行情、资金流向、公司资料、行业概念、个股资讯。
- 东方财富板块中心：行情中心中的行业板块、概念板块及资金流向栏目。
- 财经网：`https://finance.caijing.com.cn/`。
  - 只用于新闻标题、链接、来源和发布时间补充，不作为实时行情权威源。

禁止使用股吧帖子、自媒体正文或无法确认发布时间/来源的页面作为正式新闻。

## 来源选择与冲突规则

1. 实时价格优先级：Tushare 授权实时接口 > 东方财富公开行情 > 同花顺公开行情。
2. 昨收、成交量、成交额必须来自与最新价同一个行情快照，禁止跨来源拼接。
3. 估值优先 `daily_basic`，标记为日频数据。
4. 资金流必须带 `frequency`：`intraday` 或 `eod`；盘后 THS/DC 数据不得标成实时。
5. 行业/概念同时保留 `THS` 与 `DC` taxonomy，不强行合并同名异义板块。
6. 新闻按规范化标题、来源、发布时间和 URL 去重；保留最早抓取时间与原始来源。
7. 同一字段冲突时按优先级选主值，并在 `warnings` 中记录其他来源和差异。

## 对外 API

### 1. 健康检查

`GET /health`

返回服务状态、版本、服务器时间，不访问上游。

### 2. 能力检查

`GET /v1/capabilities`

返回各数据域是否可用、来源、频率、权限状态、最后成功时间和最近错误。不得返回 Token。

### 3. 统一侧栏快照

`POST /v1/market/sidebar/snapshot`

请求：

```json
{
  "request_id": "uuid",
  "chart_symbol": "000001.SZ",
  "watchlist_symbols": ["000001.SZ", "600000.SH"],
  "domains": ["quotes", "profile", "valuation", "capital_flow", "themes", "strength", "news"],
  "news_limit": 20,
  "max_age_seconds": {
    "quotes": 15,
    "profile": 86400,
    "valuation": 86400,
    "capital_flow": 300,
    "themes": 3600,
    "strength": 60,
    "news": 300
  }
}
```

限制：关注列表最多 500 个代码；未知代码返回逐项错误，不让整个请求失败。

响应顶层：

```json
{
  "schema_version": "1.0",
  "request_id": "uuid",
  "generated_at": "2026-07-12T10:00:00+08:00",
  "trading_date": "2026-07-10",
  "partial": false,
  "watchlist_quotes": [],
  "active_symbol_profile": {},
  "market_strength": {},
  "news": [],
  "errors": [],
  "warnings": []
}
```

### 行情数组结构

```json
{
  "symbol": "000001.SZ",
  "name": "平安银行",
  "exchange": "SZ",
  "price": 10.45,
  "previous_close": 10.49,
  "change": -0.04,
  "change_percent": -0.381316,
  "volume": 95732000,
  "amount": 1002000000,
  "turnover_rate": 0.52,
  "market_status": "closed",
  "source": "tushare_rt",
  "freshness": "fresh",
  "as_of": "2026-07-10T15:00:00+08:00",
  "trading_date": "2026-07-10"
}
```

### 当前标的资料结构

```json
{
  "symbol": "000001.SZ",
  "identity": {
    "name": "平安银行",
    "exchange": "SZ",
    "industry": "银行",
    "business_summary": null
  },
  "valuation": {
    "market_cap": 202800000000,
    "float_market_cap": 202000000000,
    "pe_ratio": 5.2,
    "pb_ratio": 0.55,
    "turnover_rate": 0.52,
    "frequency": "daily"
  },
  "capital_flow": {
    "net_inflow": null,
    "main_net_inflow": null,
    "large_net_inflow": null,
    "medium_net_inflow": null,
    "small_net_inflow": null,
    "frequency": "eod",
    "as_of": "2026-07-10T15:00:00+08:00"
  },
  "themes": [
    {
      "id": "ths:881155",
      "name": "银行",
      "taxonomy": "THS",
      "type": "industry",
      "change_percent": -0.28,
      "main_net_inflow": -180000000
    }
  ],
  "source_status": {},
  "warnings": []
}
```

### 市场强度结构

```json
{
  "score": 72.5,
  "up_count": 3200,
  "down_count": 1800,
  "limit_up_count": 68,
  "limit_down_count": 4,
  "leaders": [
    {
      "symbol": "688001.SH",
      "name": "示例股票",
      "price": 20.1,
      "change_percent": 20.01,
      "amount": 800000000,
      "industry": "电子"
    }
  ],
  "themes": [
    {
      "id": "ths:xxx",
      "name": "低空经济",
      "taxonomy": "THS",
      "change_percent": 4.4,
      "main_net_inflow": 3010000000,
      "leader_symbols": ["688001.SH"]
    }
  ],
  "formula_version": "breadth-v1",
  "source": "aggregated",
  "freshness": "fresh",
  "as_of": "2026-07-10T15:00:00+08:00"
}
```

`score` 必须使用固定、可测试公式，例如上涨家数占比、涨停/跌停比、主要指数涨跌幅的加权结果；不得让 LLM临时打分。

### 新闻结构

```json
{
  "id": "sha256-stable-id",
  "title": "新闻标题",
  "url": "https://合法原始链接",
  "source_name": "同花顺",
  "published_at": "2026-07-10T14:32:00+08:00",
  "category": "company",
  "related_symbols": [
    {
      "symbol": "000001.SZ",
      "name": "平安银行",
      "change_percent": -0.38
    }
  ],
  "fetched_at": "2026-07-10T14:35:00+08:00",
  "freshness": "fresh"
}
```

新闻标题必须可点击；URL 必须是 `http`/`https`，禁止 `javascript:`、跳转脚本和无法验证的短链。不要返回正文和摘要。

## 元数据和错误合同

每个数据域必须返回：

- `source`
- `freshness`: `fresh | stale | unavailable`
- `as_of`
- `trading_date`
- `frequency`: `realtime | intraday | daily | eod`
- `warnings`

错误数组结构：

```json
{
  "domain": "news",
  "symbol": "000001.SZ",
  "provider": "tushare_major_news",
  "code": "permission_denied",
  "retryable": false,
  "message": "provider permission unavailable"
}
```

允许的错误码：`timeout`、`rate_limited`、`permission_denied`、`upstream_schema_changed`、`not_found`、`temporarily_unavailable`。

## 缓存和性能

1. 服务为请求驱动，不设置高频轮询。
2. 相同 symbols/domains 请求采用 single-flight，禁止并发重复访问同一上游。
3. 行情交易时段缓存 10-15 秒；非交易时段缓存到下一交易日开盘前。
4. 资料与行业缓存 24 小时；估值缓存到下一交易日；新闻缓存 5 分钟；市场强度缓存 30-60 秒。
5. 上游失败时可返回最后成功快照并标记 `stale`，不能清空成假数据。
6. 缓存 key 必须包含 schema version、交易日、domain、symbol/source。
7. 缓存命中 p95 小于 150ms；200 个关注标的的统一响应 p95 小于 500ms。
8. 冷启动可异步填充：首个响应允许 `partial=true`，但不得阻塞超过 5 秒。

## 安全要求

1. 对外接口使用 Bearer Token 或 HMAC；支持密钥轮换。
2. 限制请求体、symbol 数量、news_limit 和返回大小。
3. URL allowlist；禁止调用 localhost、内网 IP、file URL 或用户任意传入 URL。
4. 日志只能记录 provider 名、耗时、状态码、字段数量；不得记录 Token、Cookie、完整新闻正文。
5. 不使用浏览器 Cookie，不要求人工登录，不依赖 Windows；必须能在 Linux 云端运行。

## 测试与交付

必须交付：

1. 可部署后端和环境变量清单。
2. OpenAPI 3.1 文档及 JSON Schema。
3. 每个 provider 的 adapter 和结构化 fixture。
4. 单元测试：代码规范化、单位转换、缺字段、重复新闻、跨来源冲突、交易日、stale fallback。
5. 集成测试：至少 `000001.SZ`、`600000.SH`、科创板和北交所各一个标的。
6. 性能报告：缓存命中/冷启动 p50、p95、最大值。
7. `/v1/capabilities` 实测报告，列出 Tushare 权限不足的接口。
8. 一个可直接调用的 curl 示例和脱敏响应样例。

在写代码前先输出：数据源能力矩阵、接口权限风险、网页抓取合法性风险、字段映射表和实施计划。未经确认不要擅自改变上述 JSON 合同。

---

## 本项目后续接入约束

Anything 服务验收后，消费者项目只新增一个 `AnythingMarketDataProvider` adapter。它必须转换为现有侧栏 canonical DTO，再由现有 Redis 交易日缓存和 WebSocket delta 发布；前端不得直接请求 Anything 服务。
