<!-- TEMPLATE (ZH) BEGIN: BADGES -->

[![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/yylego/go-mqtt/release.yml?branch=main&label=BUILD)](https://github.com/yylego/go-mqtt/actions/workflows/release.yml?query=branch%3Amain)
[![GoDoc](https://pkg.go.dev/badge/github.com/yylego/go-mqtt)](https://pkg.go.dev/github.com/yylego/go-mqtt)
[![Coverage Status](https://img.shields.io/coveralls/github/yylego/go-mqtt/main.svg)](https://coveralls.io/github/yylego/go-mqtt?branch=main)
[![Supported Go Versions](https://img.shields.io/badge/Go-1.26%2B-lightgrey.svg)](https://go.dev/)
[![GitHub Release](https://img.shields.io/github/release/yylego/go-mqtt.svg)](https://github.com/yylego/go-mqtt/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/yylego/go-mqtt)](https://goreportcard.com/report/github.com/yylego/go-mqtt)

<!-- TEMPLATE (ZH) CLOSE: BADGES -->

# go-mqtt

`github.com/eclipse/paho.mqtt.golang` 的增强型 Golang MQTT 客户端封装，支持重连时自动重订阅。

<!-- TEMPLATE (ZH) BEGIN: LANGUAGE NAVIGATION -->

## 英文文档

[ENGLISH README](README.md)

<!-- TEMPLATE (ZH) CLOSE: LANGUAGE NAVIGATION -->

---

## 核心特性

- 🔄 **自动重订阅**：解决 Paho 客户端重连时缺少自动重订阅功能的问题
- ⚡ **OnConnect 回调**：通过回调模式实现订阅管理，普通主题用 `Subscribe` 一行订阅
- 🔁 **重复逻辑**：内置连接操作的重新尝试机制，支持退避策略
- ⏱️ **令牌辅助工具**：简化令牌状态检查，支持超时设置
- 🛠️ **Mate 生态集成**：集成 `erero`、`must`、`rese` 实现简洁的错误处理
- 📋 **结构化日志**：可自定义的日志接口，集成 zap 日志库

## 安装

```bash
go get github.com/yylego/go-mqtt
```

**环境要求：**

- Go 1.26.0 或更高版本
- MQTT 代理服务器（例如：EMQX、Mosquitto）

## 快速开始

### 基础发布/订阅

此示例演示核心功能：创建支持重连时自动重订阅的发布和订阅客户端。

```go
package main

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/yylego/must"
	"github.com/yylego/neatjson/neatjsons"
	"github.com/yylego/rese"
	"github.com/yylego/zaplog"
)

func main() {
	const mqttTopic = "mqtt-go-demo1-topic"

	config := &mqttgo.Config{
		BrokerServer: "ws://127.0.0.1:8083/mqtt",
		Username:     "username",
		Password:     "password",
		OrderMatters: false,
	}
	client1 := rese.V1(mqttgo.NewClientWithCallback(config, uuid.NewString(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client1.Disconnect(500)

	client2 := rese.V1(mqttgo.NewClientWithCallback(config, uuid.NewString(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			token := c.Subscribe(mqttTopic, 1, func(client mqttgo.Client, message mqttgo.Message) {
				zaplog.SUG.Debugln("subscribe-msg:", neatjsons.SxB(message.Payload()))
			})
			must.Same(rese.C1(mqttgo.CheckToken(token, time.Minute)), mqttgo.TokenStateSuccess)
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client2.Disconnect(500)

	type MessageType struct {
		A string
		B int
		C float64
	}

	for i := range 10 {
		msg := &MessageType{
			A: time.Now().String(),
			B: i,
			C: rand.Float64(),
		}
		contentBytes := rese.A1(json.Marshal(msg))

		zaplog.SUG.Debugln("publish-msg:", neatjsons.SxB(contentBytes))

		token := client1.Publish(mqttTopic, 1, false, contentBytes)
		must.Same(rese.C1(mqttgo.CheckToken(token, time.Second*3)), mqttgo.TokenStateSuccess)
		time.Sleep(time.Second)
	}
}
```

⬆️ **源码：** [demo1x/main.go](internal/demos/demo1x/main.go)

### 带重复逻辑

此示例展示如何使用回调状态控制订阅失败时的重新尝试行为。

```go
package main

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/yylego/erero"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/yylego/must"
	"github.com/yylego/neatjson/neatjsons"
	"github.com/yylego/rese"
	"github.com/yylego/zaplog"
)

func main() {
	const mqttTopic = "mqtt-go-demo2-topic"

	config := &mqttgo.Config{
		BrokerServer: "ws://127.0.0.1:8083/mqtt",
		Username:     "username",
		Password:     "password",
		OrderMatters: false,
	}

	client1 := rese.V1(mqttgo.NewClientWithCallback(config, uuid.NewString(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			if retryTimes > 10 {
				return mqttgo.CallbackTimeout, nil
			}
			if rand.IntN(100) >= 10 {
				return mqttgo.CallbackUnknown, erero.New("random-rate-not-success")
			}
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client1.Disconnect(500)

	client2 := rese.V1(mqttgo.NewClientWithCallback(config, uuid.NewString(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			token := c.Subscribe(mqttTopic, 1, func(client mqttgo.Client, message mqttgo.Message) {
				zaplog.SUG.Debugln("subscribe-msg:", neatjsons.SxB(message.Payload()))
			})
			tokenState, err := mqttgo.WaitToken(token)
			if err != nil {
				return mqttgo.CallbackRetries, errors.WithMessage(err, "subscribe-is-wrong")
			}
			must.Same(tokenState, mqttgo.TokenStateSuccess)
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client2.Disconnect(500)

	type MessageType struct {
		A string
		B int
		C float64
	}

	for i := range 100 {
		msg := &MessageType{
			A: time.Now().String(),
			B: i,
			C: rand.Float64(),
		}
		contentBytes := rese.A1(json.Marshal(msg))

		zaplog.SUG.Debugln("publish-msg:", neatjsons.SxB(contentBytes))

		token := client1.Publish(mqttTopic, 1, false, contentBytes)
		must.Same(rese.C1(mqttgo.CheckToken(token, time.Second*3)), mqttgo.TokenStateSuccess)
		time.Sleep(time.Second)
	}
}
```

⬆️ **源码：** [demo2x/main.go](internal/demos/demo2x/main.go)

## API 参考

### 核心函数

#### `NewClientWithCallback(config *Config, clientID string, callback *Callback) (mqtt.Client, error)`

创建支持通过 OnConnect 回调自动重订阅的 MQTT 客户端。

**参数：**

- `config`：MQTT 代理服务器配置
- `clientID`：唯一客户端标识符
- `callback`：用于订阅管理的 OnConnect 回调处理程序

**返回：** 已连接的 MQTT 客户端或错误

#### `NewClient(opts *mqtt.ClientOptions) (mqtt.Client, error)`

使用给定选项创建基础 MQTT 客户端。阻塞直到连接成功。

#### `NewCallback() *Callback`

创建连接事件的回调处理程序集合。

#### `(*Callback).OnConnect(handler func(client mqtt.Client, retryTimes uint64) (CallbackState, error)) *Callback`

注册在连接/重连时运行的回调。支持链式调用多个处理程序。

#### `(*Callback).Subscribe(topic string, qos byte, onMessage mqtt.MessageHandler) *Callback`

每次连接、重连时订阅这个主题，订阅失败会自动再试。连上之后只需要订阅时用它；还要做别的事时用 `OnConnect`。

### 令牌工具

#### `CheckToken(token mqtt.Token, timeout time.Duration) (TokenState, error)`

检查令牌完成情况并设置超时。

**返回值：**

- `TokenStateSuccess`：操作完成
- `TokenStateTimeout`：超时
- `TokenStateUnknown`：发生错误

#### `WaitToken(token mqtt.Token) (TokenState, error)`

阻塞直到令牌完成，不设超时。

### 配置

```go
type Config struct {
    BrokerServer string // MQTT 代理服务器地址 (tcp:// 或 ws://)
    Username     string // 认证用户名
    Password     string // 认证密码
    OrderMatters bool   // 消息顺序是否重要
}
```

### 回调状态

```go
const (
    CallbackSuccess CallbackState = "success" // 操作成功
    CallbackRetries CallbackState = "retries" // 请求重新尝试
    CallbackTimeout CallbackState = "timeout" // 停止重新尝试
    CallbackUnknown CallbackState = "unknown" // 未知错误，重新尝试
)
```

## 示例

### TCP 连接

```go
config := &mqttgo.Config{
    BrokerServer: "tcp://localhost:1883",
    Username:     "username",
    Password:     "password",
}
```

### WebSocket 连接

```go
config := &mqttgo.Config{
    BrokerServer: "ws://127.0.0.1:8083/mqtt",
    Username:     "username",
    Password:     "password",
}
```

### 订阅多个主题

```go
callback := mqttgo.NewCallback().
    Subscribe("topic/one", 1, onMessage).
    Subscribe("topic/two", 1, onMessage).
    Subscribe("topic/three", 2, onMessage)
```

### 自定义重复逻辑

**重复 10 次后超时：**

```go
OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
    if retryTimes > 10 {
        return mqttgo.CallbackTimeout, nil
    }
    token := c.Subscribe("topic", 1, handler)
    state, err := mqttgo.CheckToken(token, time.Second*5)
    if err != nil {
        return mqttgo.CallbackRetries, err
    }
    return mqttgo.CallbackSuccess, nil
})
```

**失败时立即重新尝试：**

```go
OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
    token := c.Subscribe("topic", 1, handler)
    if state, err := mqttgo.WaitToken(token); err != nil {
        return mqttgo.CallbackRetries, err
    }
    return mqttgo.CallbackSuccess, nil
})
```

### 自定义日志记录器

```go
type CustomLogger struct{}

func (l *CustomLogger) ErrorLog(msg string, fields ...zap.Field) {
    // 你的错误日志逻辑
}

func (l *CustomLogger) DebugLog(msg string, fields ...zap.Field) {
    // 你的调试日志逻辑
}

func main() {
    mqttgo.SetLog(&CustomLogger{})
    // 创建客户端...
}
```

## 设计亮点

Eclipse Paho MQTT 客户端在重连后不会自动重订阅主题（[Issue #22](https://github.com/eclipse/paho.mqtt.golang/issues/22)）。这个封装通过以下方式解决了该问题：

1. **OnConnect 回调**：在每次连接/重连时执行订阅逻辑；常见的订阅用 `Subscribe` 一行搞定
2. **重新尝试机制**：自动重新尝试失败的订阅，间隔从 100ms 起、每次翻倍、最长 5s
3. **状态管理**：清晰的回调状态控制重新尝试行为
4. **Mate 集成**：利用 `rese`、`must`、`erero` 实现简洁代码

## 运行 EMQX 进行测试

参见 [`internal/sketches/sketch1/README.md`](internal/sketches/sketch1/README.md) 了解 Docker 配置说明。

## 运行嵌入式 MQTT

参见 [`internal/sketches/sketch2/README.md`](internal/sketches/sketch2/README.md) 了解使用 `github.com/mochi-mqtt/server/v2` 的嵌入式 MQTT 配置。

<!-- TEMPLATE (ZH) BEGIN: STANDARD PROJECT FOOTER -->
<!-- VERSION 2025-11-25 03:52:28.131064 +0000 UTC -->

## 📄 许可证类型

MIT 许可证 - 详见 [LICENSE](LICENSE)。

---

## 💬 联系与反馈

非常欢迎贡献代码！报告 BUG、建议功能、贡献代码：

- 🐛 **问题报告？** 在 GitHub 上提交问题并附上重现步骤
- 💡 **新颖思路？** 创建 issue 讨论
- 📖 **文档疑惑？** 报告问题，帮助我们完善文档
- 🚀 **需要功能？** 分享使用场景，帮助理解需求
- ⚡ **性能瓶颈？** 报告慢操作，协助解决性能问题
- 🔧 **配置困扰？** 询问复杂设置的相关问题
- 📢 **关注进展？** 关注仓库以获取新版本和功能
- 🌟 **成功案例？** 分享这个包如何改善工作流程
- 💬 **反馈意见？** 欢迎提出建议和意见

---

## 🔧 代码贡献

新代码贡献，请遵循此流程：

1. **Fork**：在 GitHub 上 Fork 仓库（使用网页界面）
2. **克隆**：克隆 Fork 的项目（`git clone https://github.com/yourname/repo-name.git`）
3. **导航**：进入克隆的项目（`cd repo-name`）
4. **分支**：创建功能分支（`git checkout -b feature/xxx`）
5. **编码**：实现您的更改并编写全面的测试
6. **测试**：（Golang 项目）确保测试通过（`go test ./...`）并遵循 Go 代码风格约定
7. **文档**：面向用户的更改需要更新文档
8. **暂存**：暂存更改（`git add .`）
9. **提交**：提交更改（`git commit -m "Add feature xxx"`）确保向后兼容的代码
10. **推送**：推送到分支（`git push origin feature/xxx`）
11. **PR**：在 GitHub 上打开 Merge Request（在 GitHub 网页上）并提供详细描述

请确保测试通过并包含相关的文档更新。

---

## 🌟 项目支持

非常欢迎通过提交 Merge Request 和报告问题来贡献此项目。

**项目支持：**

- ⭐ **给予星标**如果项目对您有帮助
- 🤝 **分享项目**给团队成员和（golang）编程朋友
- 📝 **撰写博客**关于开发工具和工作流程 - 我们提供写作支持
- 🌟 **加入生态** - 致力于支持开源和（golang）开发场景

**祝你用这个包编程愉快！** 🎉🎉🎉

<!-- TEMPLATE (ZH) CLOSE: STANDARD PROJECT FOOTER -->
