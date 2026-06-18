# DeFi 借贷协议后端设计文档

## 概述

### 项目定位

基于 EVM 兼容链的 DeFi 借贷协议后端服务，采用池子模型（类 Aave），使用 Go 技术栈开发。后端不涉及智能合约开发，作为链上合约与前端 DApp 之间的桥梁层，负责链上数据索引、利率计算、清算执行、风控监控等核心链下逻辑。

### 核心约束

- **链支持：** 仅 EVM 兼容链（Ethereum、BSC、Arbitrum、Polygon 等）
- **协议模型：** 池子模型，借贷双方共享流动性池，利率由算法根据利用率动态调整
- **不托管私钥：** 用户私钥仅在前端签名，后端不持有用户资产
- **清算钱包：** 后端管理协议清算钱包，私钥通过 KMS 管理
- **部署：** Kubernetes 容器化部署
- **面向生产：** 所有模块需考虑高可用、可观测、故障恢复

### 技术栈

| 类别 | 选型 |
|------|------|
| 语言 | Go 1.22+ |
| Web 框架 | Gin |
| 链交互 | go-ethereum (ethclient) |
| 数据库 | PostgreSQL 16 |
| 缓存 | Redis 7 |
| 消息队列 | Kafka |
| 监控 | Prometheus + Grafana |
| 日志 | 结构化日志 (zerolog) + ELK/Loki |
| 密钥管理 | HashiCorp Vault / AWS KMS |
| 容器 | Docker + Kubernetes |
| 配置管理 | Viper (YAML + 环境变量) |

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        前端 DApp                                │
└──────────────────────────┬──────────────────────────────────────┘
                           │ REST / WebSocket
┌──────────────────────────▼──────────────────────────────────────┐
│                    API Gateway 层                                │
│  路由 / 签名认证(EIP-4361) / 限流 / 请求日志                     │
└──┬──────────┬──────────┬───────────┬───────────┬───────────┬────┘
   │          │          │           │           │           │
   ▼          ▼          ▼           ▼           ▼           ▼
┌──────┐ ┌────────┐ ┌────────┐ ┌─────────┐ ┌────────┐ ┌────────┐
│市场   │ │用户    │ │清算    │ │ 价格    │ │ 风控   │ │ 通知   │
│数据   │ │资产    │ │引擎    │ │ 预言机  │ │ 引擎   │ │ 系统   │
│模块   │ │模块    │ │模块    │ │ 模块    │ │ 模块   │ │ 模块   │
└──┬───┘ └───┬────┘ └───┬────┘ └────┬────┘ └───┬────┘ └───┬────┘
   │         │          │           │           │          │
   ▼         ▼          ▼           ▼           ▼          ▼
┌─────────────────────────────────────────────────────────────────┐
│               链上数据索引层 (Event Indexer)                      │
│  区块监听 / 事件解析 / 状态同步 / 链重组处理                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │ JSON-RPC / WebSocket
┌──────────────────────────▼──────────────────────────────────────┐
│                EVM 节点 (多链: ETH/BSC/ARB/Polygon...)            │
└─────────────────────────────────────────────────────────────────┘

横切关注点: PostgreSQL / Redis / Kafka / Prometheus+Grafana / K8s
```

---

## 模块一：链上数据索引层 (Chain Indexer)

### 解决的问题

直接从链上读取数据延迟高（50-200ms/次 RPC）、有限流、且无法做复杂查询。需要将链上状态实时同步到本地数据库，为其他所有模块提供数据基础。

### 功能清单

| 功能 | 说明 |
|------|------|
| 区块监听器 | 通过 WebSocket 订阅新区块，支持多链并行监听，每条链一个独立 Goroutine |
| 事件解析器 | 解析借贷合约事件：Deposit、Withdraw、Borrow、Repay、Liquidation、ReserveDataUpdated、AccrueInterest |
| 状态同步 | 将链上池子 TVL、利率参数、用户仓位等状态同步到 PostgreSQL |
| 链重组处理 | 检测 Reorg，回滚受影响区块的数据并从分叉点重新索引 |
| 断点续传 | 持久化已索引区块高度，服务重启后从断点继续 |
| 多链适配 | 统一 ChainAdapter 接口，每条链配置各自的区块时间、确认数、RPC 端点 |

### 设计要点

- 事件写入先发送到 Kafka，再由消费者写入数据库，保证可靠性和解耦
- 确认区块数可配置：ETH 主网 12 个，L2 链 1-3 个
- 支持批量历史回填（backfill），用于首次部署或数据修复
- RPC 连接池 + 故障转移（配置多个 RPC 端点）

### 数据模型

```
blocks: chain_id, block_number, block_hash, parent_hash, timestamp, is_confirmed
events: chain_id, block_number, tx_hash, log_index, event_type, contract_address, decoded_data, created_at
sync_status: chain_id, last_indexed_block, last_confirmed_block, updated_at
```

---

## 模块二：市场数据模块 (Market Service)

### 解决的问题

前端需要快速获取借贷市场的聚合数据（池子列表、TVL、利率等），直接读链无法满足性能要求。

### 功能清单

| 功能 | 说明 |
|------|------|
| 市场列表 | 所有支持的借贷市场/池子信息（资产、链、合约地址、状态） |
| 实时利率 | 存款 APY、借款 APR、利率模型参数 |
| TVL 统计 | 每个池子的总存款、总借款、可用流动性、利用率 |
| 历史数据 | 利率历史、TVL 趋势，支持时间范围和粒度查询（用于图表） |
| 资产配置 | 资产白名单、抵押因子(LTV)、清算阈值、借款上限等参数 |
| 多链聚合 | 按资产维度聚合多链数据 |

### 设计要点

- Redis 缓存热点数据（市场列表、当前利率），TTL 10-30s
- 历史数据按小时/天粒度聚合存储到 PostgreSQL 分区表
- API 支持按链、按资产过滤和分页

### API 示例

```
GET  /api/v1/markets                     # 市场列表
GET  /api/v1/markets/{chain_id}/{asset}  # 单个市场详情
GET  /api/v1/markets/{chain_id}/{asset}/history?period=7d&interval=1h  # 历史数据
```

---

## 模块三：用户资产模块 (Account Service)

### 解决的问题

用户需要查看分散在链上多个合约中的仓位详情、借贷历史、健康因子等聚合信息。

### 功能清单

| 功能 | 说明 |
|------|------|
| 钱包认证 | 基于 EIP-4361 (Sign-In with Ethereum) 签名验证，无密码无托管 |
| 仓位概览 | 用户在各池子的存款/借款余额、抵押物价值、负债价值 |
| 健康因子 | 实时计算用户健康因子，低于阈值触发告警 |
| 交易历史 | Deposit/Withdraw/Borrow/Repay/Liquidation 历史，含链上 tx hash |
| 收益计算 | 累计存款利息收益、借款利息支出 |
| 交易构建 | 为前端构建合约调用的 calldata，用户在前端签名后广播 |

### 设计要点

- 后端不托管用户私钥，仅做签名验证和数据聚合
- 健康因子计算结合价格模块实时价格 + 本地利率计算（与清算引擎共享）
- 交易构建使用 go-ethereum 的 ABI 编码
- JWT Token 用于会话管理，Token 中包含钱包地址

### API 示例

```
POST /api/v1/auth/login          # EIP-4361 签名登录
GET  /api/v1/account/positions   # 用户仓位概览
GET  /api/v1/account/history     # 交易历史
POST /api/v1/account/tx/build    # 构建交易 calldata
```

---

## 模块四：价格预言机模块 (Price Oracle Service)

### 解决的问题

清算判断、健康因子计算、TVL 估值都依赖准确实时的价格数据。价格延迟或错误会导致错误清算或坏账。

### 功能清单

| 功能 | 说明 |
|------|------|
| Chainlink 集成 | 读取 Chainlink Price Feed 合约的最新价格和 roundId |
| 价格缓存 | Redis 缓存价格，主要资产 TTL 10s，次要资产 TTL 30s |
| 价格偏差检测 | 对比多来源价格，偏差超 5% 触发告警 |
| 历史价格 | 资产价格历史存储，用于分析和审计 |
| 心跳监控 | 监控预言机更新频率，超时未更新触发告警 |
| 降级策略 | Chainlink 不可用时降级到备用源（如 Uniswap V3 TWAP） |

### 设计要点

- 价格更新写入 Kafka topic，清算引擎和风控引擎订阅消费
- 价格源优先级：Chainlink > Uniswap TWAP > 中心化 API（仅告警参考）
- 所有价格变更带时间戳和来源标记，支持事后审计

---

## 模块五：清算引擎 (Liquidation Engine)

### 解决的问题

当借款人健康因子低于 1 时需要及时清算，避免协议产生坏账。这是最核心、最时间敏感的模块。

### 架构：三子组件设计

```
┌─────────────────────────────────────────────────────────────┐
│                 清算引擎 (Liquidation Engine)                 │
│                                                               │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────┐  │
│  │  利率计算器      │  │  仓位扫描器      │  │  清算执行器   │  │
│  │  RateComputer   │  │ PositionScanner │  │ LiqExecutor  │  │
│  │                 │  │                 │  │              │  │
│  │ - 复现链上利率  │  │ - 全量仓位遍历  │  │ - Gas 估算   │  │
│  │   模型          │──▶│   健康因子计算   │──▶│ - 交易构建   │  │
│  │ - 本地推算      │  │ - 优先级队列    │  │ - MEV 防护   │  │
│  │   borrowIndex   │  │   (按HF排序)    │  │ - Nonce 管理 │  │
│  │ - 定期校准      │  │ - 增量更新      │  │ - 状态追踪   │  │
│  └────────┬────────┘  └─────────────────┘  └──────────────┘  │
│           │                                                    │
│           ▼                                                    │
│  ┌──────────────────────────────────────────┐                 │
│  │           校准机制 (Calibration)           │                 │
│  │  - 事件驱动：Indexer 事件实时校准           │                 │
│  │  - 周期校准：每 30 个区块链上读取兜底        │                 │
│  │  - 偏差告警：> 0.01% 修正，> 0.1% 告警     │                 │
│  └──────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

### 子组件一：利率计算器 (RateComputer)

**核心职责：** 在本地复现链上利率模型，持续推算 borrowIndex/liquidityIndex，避免频繁 RPC 调用。

| 功能 | 说明 |
|------|------|
| 利率模型复现 | Go 中实现与合约相同的利率计算公式（线性/跳跃利率模型） |
| Index 本地推算 | 基于上次同步的 Index + 时间差，推算当前 borrowIndex/liquidityIndex |
| 精度处理 | 使用 `math/big` 实现 Ray (10^27) 和 Wad (10^18) 定点数运算，与 Solidity 精度对齐 |
| 事件驱动校准 | 收到 Indexer 的 Deposit/Withdraw/Borrow/Repay 事件时，更新池子状态并重新计算基准 |
| 周期性校准 | 每 30 个区块从链上读取真实 Index，与本地值对比修正（兜底机制） |
| 偏差监控 | 本地值与链上值偏差 < 0.01% 正常，0.01%-0.1% 修正+日志，> 0.1% 修正+告警 |

**计算流程：**

```
1. utilizationRate = totalBorrows / (totalDeposits + totalBorrows)
2. borrowRate = f(utilizationRate, baseRate, slope1, slope2, optimalUtilization)
3. timeDelta = currentTimestamp - lastUpdateTimestamp
4. newBorrowIndex = lastBorrowIndex × (1 + borrowRate × timeDelta / SECONDS_PER_YEAR)
5. userDebt = userPrincipal × (newBorrowIndex / userBorrowIndex)
6. healthFactor = (collateralValue × liquidationThreshold) / totalDebtValue
```

### 子组件二：仓位扫描器 (PositionScanner)

| 功能 | 说明 |
|------|------|
| 全量仓位维护 | 内存中维护所有活跃仓位（有借款的地址） |
| 优先级队列 | 按 Health Factor 排序的最小堆，堆顶为最危险仓位 |
| 增量更新 | 价格变化时只重算 HF < 1.5 的仓位；全量重算每 30s 一次 |
| 事件触发 | 收到 Borrow/Withdraw 事件时即时重算涉及仓位的 HF |
| 可清算发现 | HF < 1.0 的仓位进入待清算队列，计算最优清算数量和预期收益 |

### 子组件三：清算执行器 (LiqExecutor)

| 功能 | 说明 |
|------|------|
| Gas 估算 | 估算清算交易 Gas 费用，确保清算有利可图 |
| 收益计算 | 清算奖励 - Gas 成本 > 最低利润阈值才执行 |
| 交易构建 | 构建清算合约调用的 calldata |
| MEV 防护 | ETH 主网通过 Flashbots Bundle 提交；L2 直接提交 |
| Nonce 管理 | 管理清算钱包 Nonce，支持并发（Nonce 预分配池） |
| 交易追踪 | 追踪 Pending/Confirmed/Failed 状态，失败带指数退避重试 |
| 资金管理 | 监控清算钱包 ETH 余额（Gas）和还款资产余额，余额不足时告警 |
| 私钥安全 | 通过 Vault/KMS 签名交易，私钥不落盘不进内存 |

---

## 模块六：风控引擎 (Risk Engine)

### 解决的问题

协议层面的系统性风险监控，检测异常行为和攻击模式，保护协议安全。

### 功能清单

| 功能 | 说明 |
|------|------|
| 协议健康度 | 监控整体抵押率、坏账率、各池子利用率 |
| 大额异动检测 | 单笔存取/借还超过池子 TVL 一定比例时告警 |
| 价格操控检测 | 价格异常波动伴随借贷操作时标记为可疑 |
| 闪电贷监控 | 检测同区块内的借-操作-还模式 |
| 利用率告警 | 池子利用率接近 100% 告警（取款困难风险） |
| 规则引擎 | 可配置的风控规则，支持动态调整阈值，无需重启 |

### 设计要点

- 风控规则存储在数据库，支持管理后台动态修改
- 风控事件写入独立的审计日志表
- 严重风控告警触发自动熔断（如暂停清算、通知管理员）

---

## 模块七：通知系统 (Notification Service)

### 解决的问题

用户和管理员需要及时获知关键事件。

### 功能清单

| 功能 | 说明 |
|------|------|
| 用户通知 | 健康因子低于阈值告警、被清算通知、利率大幅变动 |
| 管理员告警 | 系统异常、风控触发、清算失败、节点故障、钱包余额不足 |
| 多通道 | WebSocket 实时推送、Telegram Bot、邮件、Webhook |
| 通知偏好 | 用户可配置通知阈值（如 HF < 1.3 时告警）和渠道偏好 |
| 去重与限频 | 相同告警在冷却期内不重复发送 |

---

## API Gateway 层

### 职责

所有外部请求的统一入口，不包含业务逻辑。

| 功能 | 说明 |
|------|------|
| 路由 | RESTful API 路由到各业务模块 |
| 认证 | EIP-4361 签名验证 + JWT 会话管理 |
| 限流 | 基于 IP 和钱包地址的速率限制（Redis 令牌桶） |
| CORS | 跨域配置 |
| 请求日志 | 请求/响应日志，脱敏处理 |
| WebSocket | 实时数据推送（价格、仓位变化、通知） |
| 健康检查 | K8s 存活/就绪探针端点 |

---

## 基础设施

| 组件 | 用途 | 配置要点 |
|------|------|---------|
| PostgreSQL 16 | 主数据库 | 主从复制，读写分离，分区表（历史数据按月分区） |
| Redis 7 | 缓存/限流/实时价格 | Sentinel 高可用，缓存 TTL 分级管理 |
| Kafka | 事件总线 | 链上事件 topic、价格更新 topic、通知 topic |
| Prometheus + Grafana | 指标监控 | 自定义 metrics：区块延迟、清算成功率、RPC 延迟 |
| ELK / Loki | 日志 | 结构化日志收集和查询 |
| HashiCorp Vault | 密钥管理 | 清算钱包私钥、API Keys、数据库密码 |
| Kubernetes | 编排 | 每个模块独立 Deployment，HPA 自动伸缩 |

---

## Go 项目结构

```
defi-lending-backend/
├── cmd/
│   ├── api/              # API 服务入口
│   ├── indexer/           # 链上索引服务入口
│   ├── liquidator/        # 清算引擎服务入口
│   └── worker/            # 后台任务服务入口（风控、通知）
├── internal/
│   ├── api/               # API Gateway + 路由 + 中间件
│   │   ├── handler/       # HTTP handlers
│   │   ├── middleware/     # 认证、限流、日志
│   │   └── websocket/     # WebSocket 管理
│   ├── market/            # 市场数据模块
│   ├── account/           # 用户资产模块
│   ├── indexer/           # 链上索引模块
│   │   ├── listener/      # 区块监听
│   │   ├── parser/        # 事件解析
│   │   └── reorg/         # 重组处理
│   ├── oracle/            # 价格预言机模块
│   ├── liquidation/       # 清算引擎
│   │   ├── ratecomputer/  # 利率计算器
│   │   ├── scanner/       # 仓位扫描器
│   │   └── executor/      # 清算执行器
│   ├── risk/              # 风控引擎
│   ├── notification/      # 通知系统
│   ├── chain/             # 链交互抽象层
│   │   ├── adapter.go     # ChainAdapter 接口
│   │   ├── ethereum/      # ETH 实现
│   │   └── abi/           # 合约 ABI 定义
│   └── common/            # 公共工具
│       ├── math/          # Ray/Wad 定点数运算
│       ├── config/        # 配置管理
│       └── types/         # 共享类型定义
├── pkg/                   # 可导出的库
├── migrations/            # 数据库迁移文件
├── deployments/
│   ├── docker/            # Dockerfile
│   └── k8s/               # K8s manifests / Helm charts
├── configs/               # 配置文件模板
├── scripts/               # 运维脚本
├── docs/                  # 文档
├── go.mod
└── go.sum
```

---

## 服务拆分与部署

系统拆分为 4 个独立进程，可独立部署和伸缩：

| 服务 | 入口 | 职责 | 伸缩策略 |
|------|------|------|---------|
| api | cmd/api | API Gateway + Market + Account | HPA 按 CPU/请求数 |
| indexer | cmd/indexer | 链上数据索引 | 每条链一个实例，不需要多副本 |
| liquidator | cmd/liquidator | 清算引擎（利率计算+仓位扫描+执行） | 单实例运行（Leader Election 保证） |
| worker | cmd/worker | 风控引擎 + 通知系统 + 定时任务 | 按任务量伸缩 |

**关键：liquidator 必须单实例运行**，避免重复清算。通过 K8s Lease 或 Redis 分布式锁做 Leader Election。

---

## 关键数据库表（核心）

```sql
-- 链配置
chains (id, name, chain_id, rpc_urls, ws_url, block_time, confirmations, status)

-- 市场/池子
markets (id, chain_id, asset_address, asset_symbol, asset_decimals,
         pool_address, collateral_factor, liquidation_threshold,
         borrow_cap, supply_cap, status)

-- 池子实时状态
market_states (market_id, total_supply, total_borrow, supply_rate, borrow_rate,
               liquidity_index, borrow_index, last_update_timestamp, utilization_rate)

-- 用户仓位
positions (id, chain_id, user_address, market_id, supply_balance, borrow_balance,
           supply_index, borrow_index, updated_at)

-- 链上事件
events (id, chain_id, block_number, tx_hash, log_index, event_type,
        market_id, user_address, amount, data, created_at)

-- 清算记录
liquidations (id, chain_id, liquidator_address, borrower_address,
              collateral_market_id, debt_market_id, debt_amount, collateral_seized,
              tx_hash, gas_used, gas_price, profit, status, created_at)

-- 价格记录
prices (id, asset_address, chain_id, price_usd, source, timestamp)

-- 风控事件
risk_events (id, rule_id, severity, description, related_tx, created_at)

-- 通知
notifications (id, user_address, type, channel, content, status, created_at)
```

---

## 安全考量

1. **私钥管理：** 清算钱包私钥通过 KMS/Vault 管理，签名操作通过远程 API 完成，私钥不进入应用内存
2. **API 认证：** EIP-4361 签名验证，JWT 会话 Token，无传统密码
3. **输入校验：** 所有 API 参数严格校验（地址格式、数值范围）
4. **SQL 注入防护：** 全部使用参数化查询
5. **限流：** IP + 钱包地址维度双重限流
6. **敏感数据：** 日志中脱敏处理钱包地址和交易数据
7. **链上交易安全：** Gas Price 上限、Nonce 管理、交易超时取消

---

## 可观测性

### 核心指标

| 指标 | 说明 |
|------|------|
| indexer_block_delay | 索引器与链上最新区块的延迟 |
| indexer_reorg_count | 链重组次数 |
| price_update_latency | 价格更新延迟 |
| price_deviation | 价格偏差百分比 |
| liquidation_success_rate | 清算成功率 |
| liquidation_profit | 清算利润 |
| rate_calibration_deviation | 本地利率计算与链上的偏差 |
| rpc_request_latency | RPC 请求延迟 |
| api_request_latency | API 请求延迟 |
| health_factor_distribution | 用户健康因子分布 |

### 告警规则

- indexer_block_delay > 30s → 告警
- price_update_latency > 60s → 告警
- liquidation_success_rate < 90% → 告警
- rate_calibration_deviation > 0.1% → 告警
- 清算钱包 ETH 余额 < 阈值 → 告警
