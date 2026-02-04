# GoProxy

这是一个 Go 语言的流式透传服务：客户端先把“直链 URL + 必要请求头”注册到本服务，随后通过返回的 token 地址进行播放。本服务会使用注册时提供的 headers 去请求直链并将响应透传给客户端。

## API

- 注册直链
  - `POST /register` body: `{ "url": "<url>", "headers": { ... } }`
- 播放透传
  - `GET /<token>`
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
  "basePath": "/proxy"
}
```

当 `basePath` 设置为 `/proxy` 时：

- 注册：`POST /proxy/register`
- 透传：`GET /proxy/<token>`
- 测速：`GET /proxy/speed?bytes=2097152`
- 版本：`GET /proxy/version`
