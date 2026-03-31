# Fork 补丁维护说明

## 1. 目的

本仓库是基于上游项目的 fork，本地已加入一批定制补丁。

目标：

- 保留本地可用性修复与文案适配
- 在后续同步上游更新时，尽量减少冲突
- 让补丁具备明确边界，便于逐项复核和回放

上游仓库地址：

- [xiaocai218/astrbot_plugin_monixiuxian](https://github.com/xiaocai218/astrbot_plugin_monixiuxian)

## 2. 当前本地补丁清单

### 2.1 文档与版本收口

#### 内容

- 新增项目现状文档，说明当前可修改边界与前端 bundle 限制
- 将插件注册版本与现有 README / metadata 保持一致

#### 涉及文件

- `docs/PROJECT_STATUS.md`
- `main.py`
- `metadata.yaml`

#### 目的

- 避免版本号漂移
- 让后续维护者快速判断哪些改动能安全做，哪些属于前端源码缺失下的高风险修改

### 2.2 天机阁 `PILL_GRADE_NAMES` 报错修复

#### 内容

- 修复点击天机阁时报错：
  - `UnboundLocalError: cannot access local variable 'PILL_GRADE_NAMES' where it is not associated with a value`

#### 根因

- `game/shop.py` 的 `generate_daily_items()` 内部在 `pill_recipe` 分支重新导入了 `PILL_GRADE_NAMES`
- Python 将其判定为函数局部变量，导致前面的 `pill` 分支提前读取时报错

#### 修复方式

- 保留模块级 `PILL_GRADE_NAMES`
- 将局部导入改为别名导入，只导入真正需要的对象

#### 涉及文件

- `game/shop.py`

#### 同步上游时的判断点

- 若上游已修复该报错，则优先采用上游实现
- 若上游重构了 `generate_daily_items()`，需要重新检查是否仍存在局部变量遮蔽问题

### 2.3 绑定平台文案泛化

#### 内容

- 将“绑定QQ”相关用户可见文案改为“绑定聊天平台”
- 适配 Discord 等非 QQ 平台场景

#### 后端改动

- 返回消息中的“请在QQ中发送”改为“请在聊天平台中发送”
- 聊天帮助和登录提示中的“QQ绑定密钥”改为“聊天平台绑定密钥”

#### 前端改动

- Web 按钮文案从 `绑定QQ` 改为 `绑定聊天平台`

#### 涉及文件

- `main.py`
- `web/routes.py`
- `web/websocket_handler.py`
- `README.md`
- `static/js/index-8Va4qv2y.js`

#### 同步上游时的判断点

- 若上游恢复了前端源码，应优先在源码层保留该文案策略，不再继续维护 bundle 文本替换
- 若上游继续只提供 bundle，则需要检查该按钮文案是否被重新打包为其他字符串

### 2.4 挂机修炼上限文案动态化

#### 内容

- 将聊天端帮助中写死的 `挂机修炼(1~60分钟)` 改为按配置动态显示
- 将 Web 端提示框中的
  - `请输入挂机修炼时长（分钟，1~60）：`
  改为按管理员配置动态显示

#### 后端改动

- `main.py` 增加 `_get_afk_max_minutes()` 与 `_build_cmd_help()`
- `web/routes.py` 首页注入：
  - `window.__XIUXIAN_CONFIG__ = { afkMaxMinutes: ... }`
- `web/server.py` 将挂机上限透传至路由层

#### 前端改动

- `static/js/index-8Va4qv2y.js` 中挂机弹窗提示改为读取：
  - `window.__XIUXIAN_CONFIG__.afkMaxMinutes`

#### 涉及文件

- `main.py`
- `web/routes.py`
- `web/server.py`
- `static/js/index-8Va4qv2y.js`

#### 同步上游时的判断点

- 如果上游已经为前端提供统一配置注入机制，优先对齐上游方式
- 如果上游改动了首页 bootstrap 脚本注入逻辑，需要重新确认 `__XIUXIAN_CONFIG__` 是否仍成功注入
- 如果上游重编译前端 bundle，需要重新确认挂机弹窗是否仍存在写死文案

### 2.5 宗门仓库兑换角色折扣重设

#### 内容

- 将宗门仓库兑换折扣改为：宗主五折、副宗主六折、长老八折、弟子全价
- 移除原先的“宗主免费”设计，保留有限度的身份优惠

#### 根因 / 目的

- 原逻辑中宗主可零贡献点取出公共物品，存在明显经济绕过
- 需要保留一定身份差异，但不能继续允许免费提取宗门公共资产

#### 修复方式

- 将 `ROLE_EXCHANGE_DISCOUNT` 调整为：`leader=0.5`、`vice_leader=0.6`、`elder=0.8`、`disciple=1.0`
- 同步修改函数注释，明确新的角色折扣规则

#### 涉及文件

- `game/sect.py`

#### 同步上游时的判断点

- 若上游继续调整 `ROLE_EXCHANGE_DISCOUNT`，优先保留本地这套折扣：宗主五折、副宗主六折、长老八折、弟子全价
- 若未来希望按来源归属或更细粒度权限控制取回逻辑，需要补充仓库来源字段，不能仅靠折扣表实现

## 3. 哪些补丁最容易与上游冲突

高风险冲突点：

- `main.py`
  - 上游若继续追加聊天命令、帮助文案、初始化逻辑，这里最容易冲突
- `web/routes.py`
  - 首页脚本注入逻辑、认证接口、API 结构调整时容易冲突
- `web/server.py`
  - 仅有参数透传，冲突概率中等
- `static/js/index-8Va4qv2y.js`
  - 风险最高，因为这是打包产物，任何上游前端重新构建都会导致文本片段变化

低风险冲突点：

- `docs/PROJECT_STATUS.md`
- `docs/FORK_PATCH_NOTES.md`

## 4. 建议的同步策略

### 4.1 原则

- 先合并上游，再回放本地补丁
- 尽量让“业务修复”和“文案定制”分开验证
- 把 bundle 改动视为最后一步处理

### 4.2 推荐顺序

1. 同步上游代码
2. 先检查 `game/shop.py` 的天机阁问题是否仍存在
3. 再检查 `main.py` / `web/routes.py` / `web/server.py` 是否仍保留本地注入逻辑
4. 最后检查 `static/js/index-8Va4qv2y.js` 中两类文案：
   - `绑定聊天平台`
   - 挂机时长提示动态化

### 4.3 每次同步后的最小回归检查

- 点击 Web 端“天机阁”是否仍会报错
- Web 端绑定按钮是否显示“绑定聊天平台”
- 获取绑定密钥后提示是否为“请在聊天平台中发送”
- 修改 `afk_cultivate_max_minutes` 配置后：
  - 聊天端帮助是否显示新范围
  - Web 端挂机弹窗是否显示新范围
- 实际发起挂机时，超出上限是否仍会被后端拒绝

## 5. 后续建议

- 若计划长期维护 fork，建议把前端 bundle 对应的源码链路补齐
- 在没有前端源码前，只建议继续做：
  - 文案替换
  - 读取后端注入配置
  - 极小范围、不改变结构的字符串补丁
- 若未来上游开放前端源码，应优先把当前所有 bundle 级改动迁回源码层

## 6. 备注

当前文档只记录本地已确认补丁，不试图替代完整变更历史。

后续新增 fork 定制时，建议继续追加到本文件，保持以下结构：

- 改动内容
- 根因/目的
- 涉及文件
- 同步上游时的判断点
