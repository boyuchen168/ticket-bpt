# 彩翼云票务自动抢票脚本

仅供个人学习使用，请勿用于商业目的。

## 安装

```bash
cd ticket-bot
pip3 install -r requirements.txt
```

## 使用流程

### 1. 更新认证信息

每次抢票前需要重新获取 token（有效期一般几小时）：

1. 打开 Proxyman，保持抓包
2. 在微信中打开小程序，随便浏览一下
3. 在 Proxyman 中找到 `65373d6e95c3170001074c57.caiyicloud.com` 的请求
4. 复制以下 header 的值更新到 `config.yaml`：
   - `access-token` → `auth.access_token`
   - `cookie` → `auth.cookie`
   - `Angry-Dog`（如果有）→ `auth.angry_dog`

### 2. 测试连通性

```bash
python3 ticket_bot.py test
```

应该输出"API连通正常"和你的观演人信息。如果报错说明 token 过期，需要重新获取。

### 3. 查看演出信息

```bash
python3 ticket_bot.py info
```

会显示场次列表和票档信息。根据输出更新 `config.yaml` 中的目标场次和票档 ID。

### 4. 提交预填单

```bash
python3 ticket_bot.py prefill
```

在开售前提交预填单（选好场次、票档、观演人）。热门票模式下预填单相当于排队。

### 5. 正式抢票

```bash
python3 ticket_bot.py grab
```

会自动：
1. 获取最新演出信息
2. 提交预填单
3. 等到开售时间前 500ms 开始
4. 多线程并发提交
5. 成功后发送通知

## 配置说明

编辑 `config.yaml`：

- `auth` - 认证信息，每次使用前更新
- `show.target_session_id` - 选择哪个场次
- `show.target_seat_plan_id` - 选择哪个票档
- `show.ticket_qty` - 购买几张
- `audience.user_audience_ids` - 观演人 ID 列表
- `strategy.threads` - 并发线程数（建议 3-5）
- `strategy.advance_ms` - 提前多少毫秒开始（建议 300-500）

## 当前演出信息

- 演出：快乐管理猿（独立演员不属于任何俱乐部公司）
- 开售：2026-03-10 18:00
- 场次：
  - 16:30场 `69781a378face90001188b39`
  - 19:30场 `69781a491203fa00012dd378`
- 票档：
  - 前区票 ¥580 `69781a78a009f9000190f701`
  - 中区票 ¥480 `69781a78a009f9000190f707`
  - 普区票 ¥380 `69781a78a009f9000190f70d`

## 重要提醒

1. **Token 有效期短**：每次抢票前 10-30 分钟重新获取
2. **支付需手动**：抢票成功后请立即打开微信小程序完成支付（一般有 15 分钟窗口）
3. **请勿过度请求**：并发数建议不超过 5，避免被风控封号
