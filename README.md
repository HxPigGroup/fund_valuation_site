# fund_valuation_site

一个适合个人部署的基金估值跟踪网页，默认运行在 `11452` 端口。

它的目标不是做交易，而是把“我关心的基金今天怎么样”这件事变成一个可公网访问的小站：

- 手动新增/删除跟踪基金
- 自动刷新基金净值与估值
- 同时展示东方财富与新浪两组盘中估值、昨日增长和近一月增长
- 支持按手机号隔离个人跟踪页
- 支持 `AkShare + Tushare` 多数据源回退
- 适合部署在 Linux 服务器并长期运行

仓库地址：
- `https://github.com/HxPigGroup/fund_valuation_site`

## 功能概览

当前版本支持：

- 跟踪基金列表维护
- 基金代码输入与删除
- 手动刷新与定时刷新
- 公共页面与按手机号区分的个人页面
- 昨日增长与近一月增长展示
- 东方财富与新浪盘中估算值、估算涨跌同时展示
- 官方估值缺失时，可展开查看基于前十大重仓和实时股票涨跌的自算估值
- 数据源回退：`AkShare` 失败时自动尝试 `Tushare`
- 本地缓存与限频，尽量减少对上游接口的频繁请求
- `systemd` 长期部署

页面字段包括：

- `东方财富估算值`
- `东方财富估算涨跌`
- `新浪估算值`
- `新浪估算涨跌`
- `昨日增长`
- `近一月增长`

默认隐藏的扩展字段包括：

- `自算估值`
- `自算涨跌`

其中“自算估值”的逻辑是：

1. 读取基金最近披露的股票持仓
2. 提取前十大重仓及权重
3. 获取对应股票的实时涨跌
4. 基于上一日正式净值估算盘中变化

这类估值只适合做参考，不代表基金公司最终公布净值。由于基金持仓披露滞后，自算估值偏差可能较大；当前实现会优先使用东方财富和新浪盘中估值，只有两者都缺失时才尝试自算。

## 目录结构

- `app.py`: 主程序，内置 HTTP 服务与刷新逻辑
- `fund-valuation.service`: `systemd` 服务文件
- `.env`: 环境变量文件，保存 `TUSHARE_TOKEN` 和刷新参数
- `tracked_funds.txt`: 当前跟踪基金列表
- `user_data/`: 个人页面运行时数据目录，按手机号隔离，默认不提交
- `fund_catalog.json`: 基金代码与名称缓存
- `valuation_cache.json`: 页面展示用缓存结果
- `refresh_status.json`: 刷新状态文件
- `service.log`: 服务日志

## 运行环境

建议环境：

- Linux
- Python `3.10+`
- 可访问 `AkShare` / `Tushare` / 东方财富相关上游接口

依赖：

- `akshare`
- `tushare`
- `pandas`

如果你使用 Conda：

```bash
conda create -n fund-site python=3.11 -y
conda activate fund-site
pip install akshare tushare pandas
```

## 配置

项目通过 `/.env` 读取关键环境变量。

示例：

```bash
TUSHARE_TOKEN=你的_tushare_token
FUND_AUTO_REFRESH_SECONDS=900
FUND_HOLDINGS_TIMEOUT_SECONDS=12
FUND_STOCK_SPOT_TIMEOUT_SECONDS=18
FUND_QUOTE_CACHE_TTL_SECONDS=300
FUND_ESTIMATION_CACHE_TTL_SECONDS=300
FUND_CODE_ESTIMATION_TIMEOUT_SECONDS=6
FUND_SINA_ESTIMATION_TIMEOUT_SECONDS=8
```

字段说明：

- `TUSHARE_TOKEN`: `Tushare` token
- `FUND_AUTO_REFRESH_SECONDS`: 自动刷新间隔，默认 `900` 秒
- `FUND_HOLDINGS_TIMEOUT_SECONDS`: 持仓接口超时秒数
- `FUND_STOCK_SPOT_TIMEOUT_SECONDS`: 股票实时行情接口超时秒数
- `FUND_QUOTE_CACHE_TTL_SECONDS`: 股票实时行情缓存时长
- `FUND_ESTIMATION_CACHE_TTL_SECONDS`: 官方估值缓存时长
- `FUND_CODE_ESTIMATION_TIMEOUT_SECONDS`: 按基金代码查询官方估值的超时秒数
- `FUND_SINA_ESTIMATION_TIMEOUT_SECONDS`: 新浪盘中估值接口的读取超时秒数

## 本地启动

```bash
python app.py
```

默认监听：

```text
0.0.0.0:11452
```

浏览器访问：

```text
http://127.0.0.1:11452/
```

## systemd 部署

服务文件示例已经放在仓库里：

- `fund-valuation.service`

可以复制到：

```bash
/etc/systemd/system/fund-valuation.service
```

然后执行：

```bash
systemctl daemon-reload
systemctl enable fund-valuation.service
systemctl restart fund-valuation.service
systemctl status fund-valuation.service
```

如果你想公网访问，还需要放开 `11452` 端口。

## 数据源策略

这个项目不是绑定单一接口，而是按“可用优先”顺序回退：

### 1. 基金净值

优先：
- `AkShare fund_open_fund_info_em`

回退：
- `Tushare fund_nav`

### 2. 盘中估算值

优先：
- `AkShare fund_value_estimation_em`

增强：
- 合并东方财富多个基金分类，避免单一列表截断导致部分基金缺失

回退：
- 天天基金按代码估值接口 `fundgz.1234567.com.cn`

并行补充：
- 新浪财经 `FdFundService.getEstimateNetworthPic`，读取最新分钟点的估算净值与涨跌

### 3. 基金持仓

优先：
- `AkShare fund_portfolio_hold_em`

回退：
- `Tushare fund_portfolio`

### 4. 成分股实时行情

优先：
- `AkShare stock_zh_a_spot_em`

回退：
- `Tushare realtime_quote`

如果某个上游超时或暂时异常，只要另一个源成功，页面就继续更新。

## 个人页面

首页可以输入 11 位手机号进入个人基金跟踪页。每个手机号对应独立的：

- 跟踪基金列表
- 估值缓存
- 刷新状态

手机号只作为本地页面标识，不做短信验证。个人页运行时文件保存在 `user_data/`，该目录已加入 `.gitignore`，避免把个人数据提交到仓库。

## 跟踪基金维护

当前跟踪基金保存在：

- `tracked_funds.txt`

一行一个代码，例如：

```text
161725
005827
110011
```

网页端也支持直接新增和删除，修改后会自动触发刷新。

## 已知限制

这个项目目前更适合：

- 股票型基金
- 偏股混合基金
- 行业/主题基金
- ETF 联接基金

对于下面这些类型，自算估值的参考意义会明显下降：

- 债券基金
- QDII
- FOF
- 持仓披露不完整或滞后明显的基金

另外需要注意：

- 官方估算值依赖第三方上游，不保证每只基金都一定有
- 自算估值依赖最近披露持仓，不是实时仓位
- 上游接口会有限频、超时或临时不可用的情况

## 建议的 GitHub 发布前整理

在上传到 GitHub 前，建议不要提交这些文件：

- `.env`
- `valuation_cache.json`
- `refresh_status.json`
- `service.log`
- `__pycache__/`
- `user_data/`

建议补一个 `.gitignore`，至少包含：

```gitignore
.env
__pycache__/
*.pyc
valuation_cache.json
refresh_status.json
service.log
fund_catalog.json
user_data/
```

## 后续可以继续扩展

如果你准备继续完善，这几个方向最值得做：

- 增加基金搜索提示与代码联想
- 增加估值更新时间与缓存命中状态
- 增加走势图与历史估值归档
- 增加飞书/企业微信通知
- 支持多个基金分组
