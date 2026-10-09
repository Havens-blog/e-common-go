# e-common-go

通用 Go 工具库 —— 从 [e-cam-service](https://github.com/Havens-blog/e-cam-service) 抽取的独立共享库，沉淀跨服务复用的基础能力。

## 安装

```bash
go get github.com/Havens-blog/e-common-go@v0.1.0
```

要求 Go 1.25+。

## 子包

| 包 | 用途 |
|----|------|
| `crypto` | 加解密工具（敏感信息如密钥的对称加解密） |
| `ginx` | Gin 扩展：统一响应结构 `Result`、`WrapBody` 等泛型处理器包装 |
| `mongox` | MongoDB 封装：`Mongo` 句柄、集合访问、通用 CRUD 辅助 |
| `menu` | 菜单/权限树结构工具 |
| `netx` | 网络相关工具 |
| `taskx` | 任务调度/执行工具 |

## 用法示例

```go
import (
    "github.com/Havens-blog/e-common-go/ginx"
    "github.com/Havens-blog/e-common-go/mongox"
)

// 统一响应
func handler(c *gin.Context) {
    c.JSON(200, ginx.Result{Code: 0, Msg: "success", Data: data})
}

// 泛型请求体包装
r.POST("/x", ginx.WrapBody[CreateReq](h.Create))

// MongoDB 句柄
m := mongox.NewMongo(client, "ecam")
```

## 版本

- `v0.1.0` — 从 e-cam-service 首次抽取发布
