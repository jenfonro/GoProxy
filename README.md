# GoProxy

这是一个 Go 语言的流式透传服务：客户端先把“直链 URL + 必要请求头”注册到本服务，随后通过返回的 token 地址进行播放。本服务会使用注册时提供的 headers 去请求直链并将响应透传给客户端。

## API

- 注册直链
  - `POST /register` body: `{ "url": "<url>", "headers": { ... } }`
- 播放透传
  - `GET /<token>`
- TMDB API 代理（固定上游 `api.themoviedb.org`）
  - `GET /tmdb/<path>`（例如：`/tmdb/3/search/multi?...`）
- TMDB 图片代理（固定上游 `image.tmdb.org`）
  - `GET /tmdb-img/<path>`（例如：`/tmdb-img/t/p/w500/xxx.jpg`）
- 测速
  - `GET /speed?bytes=2097152`（返回指定大小的随机字节，用于前端测速）
- 版本信息
  - `GET /version`（返回：`{ "version": "<version>" }`；本地为 `beta-<timestamp>`，云端 beta 为 `beta-<commit>`，release 为 `V1.0.0`）

## 运行

```bash
go run .
```

## 配置（config.json）

- 修改 `config.json` 后，GoProxy 会自动重启以应用新配置（大约 1 秒内生效）。

示例：

```json
{
  "basePath": "/proxy",
  "proxy": {
    "thread": 10,
    "chunk_size_kb": 2048,
    "timeout_ms": 10000
  }
}
```

- `proxy.thread`：上游分片并发数
- `proxy.chunk_size_kb`：单个分片大小，单位 `KB`
- `proxy.timeout_ms`：单个上游请求超时，单位毫秒

请求参数仍可覆盖配置：

- `thread`
- `chunkSize`：单位 `KB`
- `timeout`：单位毫秒

当 `basePath` 设置为 `/proxy` 时：

- 注册：`POST /proxy/register`
- 透传：`GET /proxy/<token>`
- TMDB：`GET /proxy/tmdb/<path>`（用于把 MeowFilm 的 `tmdb_api_base` 指到这里）
- TMDB 图片：`GET /proxy/tmdb-img/<path>`
- 测速：`GET /proxy/speed?bytes=2097152`
- 版本：`GET /proxy/version`

### 与 MeowFilm 的 `tmdb_api_base` 配合

如果 GoProxy 通过 Nginx 挂在二级目录（例如 `/proxy`），并希望把 TMDB 的请求走同域代理：

- GoProxy：`basePath = "/proxy"`
- MeowFilm 后台（全局设置 - TMDB - API Base）：填 `https://<你的域名>/proxy/tmdb/3`

这样 MeowFilm 里对 `search/multi`、`tv/<id>`、`movie/<id>` 等的拼接会变为：
`https://<你的域名>/proxy/tmdb/3/...`，由 GoProxy 透传到 `https://api.themoviedb.org/3/...`。
