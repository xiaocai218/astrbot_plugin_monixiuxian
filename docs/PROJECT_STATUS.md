# 项目现状与可修改边界

## 1. 当前定位

本项目是 AstrBot 修仙插件，后端同时服务两类入口：

- 聊天指令入口：`main.py`
- Web 入口：`web/routes.py` + `web/websocket_handler.py`

数据核心由 SQLite 驱动，默认数据文件位于：

- `data/plugin_data/astrbot_plugin_xiuxian/xiuxian.db`

## 2. 模块职责

### 2.1 插件入口

- `main.py`
  - AstrBot 插件注册与初始化
  - 读取 `_conf_schema.json` 配置
  - 启动 `GameEngine` 与可选 Web 服务
  - 提供聊天指令映射

### 2.2 游戏核心

- `game/engine.py`
  - 统一调度玩家、背包、突破、签到、挂机、坊市、宗门、灵田、秘境、PVP 等能力
  - 聊天端与 Web 端共用业务逻辑
- `game/data_manager.py`
  - SQLite 建表、迁移、持久化、后台 CRUD
- `game/models.py`
  - `Player` 数据模型与序列化
- `game/constants.py`
  - 境界、装备、材料、丹药、心法、功法、丹方等注册表与规则常量

### 2.3 玩法模块

- `game/cultivation.py`：修炼、突破
- `game/inventory.py`：背包、使用、装备、回收
- `game/market.py`：坊市上架、购买、手续费、过期清理
- `game/shop.py`：天机阁商店
- `game/sect.py`：宗门、仓库、贡献规则、宗门商店
- `game/spirit_field.py`：灵田与种子系统
- `game/dungeon.py`：秘境推进与结算
- `game/pvp.py`：在线遭遇战/PVP 回合逻辑
- `game/combat.py`：战斗结算
- `game/pills.py`：丹药与 Buff
- `game/adventure.py`：历练奖励/掉落/受伤结算
- `game/auth.py`：Web 登录令牌、聊天绑定
- `game/renderer.py`：聊天端图片渲染

### 2.4 Web 层

- `web/server.py`
  - FastAPI 应用启动与静态资源挂载
- `web/routes.py`
  - HTTP API、管理员 API、静态首页输出
- `web/websocket_handler.py`
  - WebSocket 实时状态、世界频道、坊市/排行推送
- `web/access_guard.py`
  - 页面级凭证、限流、IP 风控

## 3. 当前可直接修改的范围

以下内容可以在当前仓库直接调整：

- 聊天指令行为与文案
- 后端业务规则与数值
- Web API 入参、返回结构、鉴权、限流、后台逻辑
- SQLite 表结构迁移与数据修复逻辑
- 服务器端推送逻辑
- 聊天端图片渲染
- 文档、配置项、元数据

## 4. 当前受限范围

以下部分受前端打包产物限制，无法像普通 Vue 工程一样安全维护：

- `static/js/index-8Va4qv2y.js`
- `static/css/index-DQcCtY4x.css`
- `static/index.html`

原因：

- 当前仓库只有前端 bundle 产物，没有 Vue 源码、组件、构建配置
- 直接改 bundle 可以做应急补丁，但维护成本高、回归风险高、语义不可读
- 涉及页面布局、组件交互、表单流程、状态管理的改动，本质上都属于前端改动

## 5. 目前建议的改动策略

优先做：

- 后端规则修复
- 接口兼容增强
- 文档与版本信息收口
- 数据迁移与容错补丁
- 通过后端补字段、补兜底、补兼容来减少前端改动需求

需要提前告知用户的情况：

- 任何要求修改 Web 页面布局、按钮交互、弹窗流程、筛选器表现、前端状态联动的需求
- 任何只能通过修改 `static/js/index-8Va4qv2y.js` 才能完成的需求

## 6. 已确认的现状问题

- `README.md` 与 `metadata.yaml` 已是 `v0.8.3`
- `main.py` 中插件注册版本原先仍是 `0.5.2`
- 这会导致宿主识别到的插件版本与文档版本不一致，已在当前轮次修正为 `0.8.3`

## 7. 后续维护建议

- 后续若继续保留 Web UI，建议补充 Vue 源码或独立前端仓库说明
- 后端新增接口时，优先保持兼容式扩展，避免破坏现有 bundle 的字段依赖
- 若要做较大 Web 交互调整，应先恢复前端源码链路，再开展功能迭代
