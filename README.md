# 超级鹿鼎公持仓自动跟踪（本地版）

已改为自动模式：

- 自动抓取来源：雪球（UID `8790885129`）+ 微博（UID `3962719063`）
- 自动识别最新持仓帖（文本 + 图片 OCR）
- 自动入库：生成“最新持仓快照”，并自动写入交易流水（加仓/减仓）
- 自动定时同步（默认每 180 分钟）

## 发布地址

- Docker Hub: `icekale/stock-lu-tracker`
- GitHub: `https://github.com/icekale/stock-lu-tracker`

## 1. 安装

```bash
npm install
```

## 2. 启动

```bash
npm start
```

访问：

- http://localhost:8787
- 后台配置页：http://localhost:8787/admin.html

## 3. 首次配置（必须）

页面里进入「自动抓取配置」并粘贴：

- 雪球 Cookie（建议包含 `xq_a_token`）
- 微博 Cookie（建议包含 `SUB`）
- 置顶帖子链接（建议填写超级鹿鼎公最新置顶帖，支持多行）
- 标题正则（默认：`游戏仓YYYY年M月PS图`）

> 说明：这两个站点对未登录请求限制较多，不配置登录态通常无法稳定抓取。

## 4. 自动抓取流程

1. 保存自动配置
2. 程序先抓置顶链接，再回退抓时间线
3. 点击「立即抓取一次」做首轮导入
4. 后续按设定间隔自动执行

## 4.1 历史回溯（批量导入历史月度帖子）

- 在设置页点「回溯历史标题帖子」
- 程序会遍历雪球历史时间线（按页）并筛选标题命中正则的帖子
- 识别文本 + 持仓截图 OCR 后自动写入快照和交易流水

## 5. 数据位置

- `data/store.json`

关键字段：

- `masterSnapshots`: 自动抓取到的持仓快照
- `autoTracking`: 自动抓取配置 / 运行状态 / 日志
- `trades`: 自动导入后的交易流水

## 6. Docker 部署（中文说明）

### 6.1 直接拉取镜像运行

```bash
docker run -d \
  --name stock-lu \
  -p 8787:8787 \
  -v $(pwd)/data:/app/data \
  -e NODE_ENV=production \
  -e PORT=8787 \
  -e TESSDATA_PREFIX=/app \
  icekale/stock-lu-tracker:latest
```

访问：

- 主页：`http://localhost:8787`
- 后台配置：`http://localhost:8787/admin.html`

### 6.2 使用 docker compose

```bash
docker compose up -d
```

### 6.3 本地构建并运行

```bash
docker build -t icekale/stock-lu-tracker:latest .
docker run -d --name stock-lu -p 8787:8787 -v $(pwd)/data:/app/data icekale/stock-lu-tracker:latest
```

## 7. 自动发布 Docker（GitHub Actions）

已配置工作流：`.github/workflows/docker-publish.yml`

- 当代码 push 到 `main`：自动构建并推送 `icekale/stock-lu-tracker:latest`
- 当 push 标签（如 `v0.1.1`）：自动推送对应版本标签
- 也支持在 GitHub Actions 页面手动触发
