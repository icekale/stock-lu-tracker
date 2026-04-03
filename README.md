# 超级鹿鼎公持仓自动跟踪（本地版）

已改为自动模式：

- 自动抓取来源：雪球（UID `8790885129`）+ 微博（UID `3962719063`）
- 自动识别最新持仓帖（文本 + 图片 OCR）
- 自动入库：生成“最新持仓快照”，并自动写入交易流水（加仓/减仓）
- 自动定时同步（默认每 180 分钟）
- 首页公开可看，后台配置与抓取接口可选密码保护
- 支持月度指标人工校正，可逐行编辑、批量保存、按状态筛选

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

可选安全项（建议开启）：

- 设置环境变量 `ADMIN_PASSWORD` 后，后台配置页和自动抓取相关接口会启用密码验证
- 登录入口：`http://localhost:8787/admin-login.html`
- 若通过 `http` 访问（如多数 Unraid 局域网场景），请保持 `ADMIN_COOKIE_SECURE=false`

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
- Docker 镜像不会内置本地 `data` 目录内容；运行数据请始终通过 volume 挂载到 `/app/data`

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
  -e ADMIN_PASSWORD='请改成你的后台密码' \
  -e ADMIN_COOKIE_SECURE=false \
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

默认示例已固定到 `v0.1.6`，这样部署更稳定；如果你希望始终跟随最新构建，可手动改回 `latest`。

如需开启后台密码，在 compose 的 `environment` 里增加：

```yaml
ADMIN_PASSWORD: "请改成你的后台密码"
ADMIN_COOKIE_SECURE: "false"
```

可选性能参数（默认可不填）：

```yaml
OCR_CACHE_MAX_ITEMS: "1200"
OCR_CACHE_TTL_MINUTES: "1440"
```

### 6.3 本地构建并运行

```bash
docker build -t icekale/stock-lu-tracker:latest .
docker run -d --name stock-lu -p 8787:8787 -v $(pwd)/data:/app/data -e ADMIN_PASSWORD='请改成你的后台密码' -e ADMIN_COOKIE_SECURE=false icekale/stock-lu-tracker:latest
```

说明：

- 构建镜像时不会打包你本机的 `data/store.json`、Cookie 或历史备份文件
- 首次启动会在挂载目录里自动创建运行所需的数据文件

### 6.4 Unraid（docker compose，bridge + 默认权限）

> 适用于 Unraid 的 Compose Manager，使用 `bridge` 网络模式，数据目录映射到 `appdata`。

```yaml
services:
  stock-lu:
    image: icekale/stock-lu-tracker:v0.1.6
    container_name: stock-lu
    network_mode: bridge
    ports:
      - "8787:8787"
    environment:
      TZ: Asia/Shanghai
      NODE_ENV: production
      PORT: 8787
      TESSDATA_PREFIX: /app
      ADMIN_PASSWORD: "请改成你的后台密码"
      ADMIN_COOKIE_SECURE: "false"
    user: "99:100"
    volumes:
      - /mnt/user/appdata/stock-lu/data:/app/data
    restart: unless-stopped
```

说明：

- `user: "99:100"` 为 Unraid 常见默认用户组（`nobody:users`），可避免写入权限问题
- 若你已在 Unraid 模板里统一处理权限，也可以去掉 `user` 字段
- 首次部署后后台地址：`http://<你的UnraidIP>:8787/admin.html`
- 也可直接使用仓库内示例文件：`docker-compose.unraid.yml`
- 如果你通过反向代理 `https` 访问后台，可将 `ADMIN_COOKIE_SECURE` 改为 `true`
- 如需升级到后续版本，只要把 `image` 标签改成新的版本号，再执行一次拉取和重建即可

## 7. 自动发布 Docker（GitHub Actions）

已配置工作流：`.github/workflows/docker-publish.yml`

- 当代码 push 到 `main`：自动构建并推送 `icekale/stock-lu-tracker:latest`
- 当 push 标签（如 `v0.1.1`）：自动推送对应版本标签
- 也支持在 GitHub Actions 页面手动触发
