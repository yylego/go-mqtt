[![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/yylego/go-mqtt/release.yml?branch=main&label=BUILD)](https://github.com/yylego/go-mqtt/actions/workflows/release.yml?query=branch%3Amain)
[![GoDoc](https://pkg.go.dev/badge/github.com/yylego/go-mqtt)](https://pkg.go.dev/github.com/yylego/go-mqtt)
[![Coverage Status](https://img.shields.io/coveralls/github/yylego/go-mqtt/main.svg)](https://coveralls.io/github/yylego/go-mqtt?branch=main)
[![Supported Go Versions](https://img.shields.io/badge/Go-1.23--1.25-lightgrey.svg)](https://github.com/yylego/go-mqtt)
[![GitHub Release](https://img.shields.io/github/release/yylego/go-mqtt.svg)](https://github.com/yylego/go-mqtt/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/yylego/go-mqtt)](https://goreportcard.com/report/github.com/yylego/go-mqtt)

# go-mqtt

Enhanced Golang MQTT client wrapping `github.com/eclipse/paho.mqtt.golang` with automatic resubscription on reconnect.

<!-- TEMPLATE (EN) BEGIN: LANGUAGE NAVIGATION -->
## CHINESE README

[中文说明](README.zh.md)
<!-- TEMPLATE (EN) END: LANGUAGE NAVIGATION -->

---

## Main Features

🔄 **Automatic Resubscription**: Solves Paho client's missing auto-resubscribe feature on reconnect
⚡ **OnConnect Callbacks**: Subscription management via callback pattern
🔁 **Repeat Logic**: Built-in re-attempt mechanism handling connection operations with backoff
⏱️ **Token Helpers**: Simplified token status checking with timeout support
🛠️ **Mate Ecosystem**: Integrated with `erero`, `must`, `rese` to handle errors
📋 **Structured Logging**: Customizable logging interface with zap integration

## Installation

```bash
go get github.com/yylego/go-mqtt
```

**Requirements:**
- Go 1.23.0 and above
- MQTT broker (e.g., EMQX, Mosquitto)

## Quick Start

### Basic Publish/Subscribe

This example demonstrates the core feature: creating publishing and subscribing clients with automatic resubscription on reconnect.

```go
package main

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/yylego/go-mqtt/internal/utils"
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
	client1 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client1.Disconnect(500)

	client2 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
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

	for i := 0; i < 10; i++ {
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

⬆️ **Source:** [demo1x/main.go](internal/demos/demo1x/main.go)

### With Repeat Logic

This example shows how to use callback states to manage re-attempt patterns on subscription issues.

```go
package main

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/yylego/go-mqtt/internal/utils"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/pkg/errors"
	"github.com/yylego/erero"
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

	client1 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
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

	client2 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
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

	for i := 0; i < 100; i++ {
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

⬆️ **Source:** [demo2x/main.go](internal/demos/demo2x/main.go)

## API Reference

### Core Functions

#### `NewClientWithCallback(config *Config, clientID string, callback *Callback) (mqtt.Client, error)`

Creates MQTT client with automatic resubscription support via OnConnect callbacks.

**Parameters:**
- `config`: MQTT broker configuration
- `clientID`: Unique client ID
- `callback`: OnConnect callback handlers managing subscriptions

**Returns:** Connected MQTT client / error

#### `NewClient(opts *mqtt.ClientOptions) (mqtt.Client, error)`

Creates basic MQTT client with given options. Waits on connection success.

#### `NewCallback() *Callback`

Creates callback collection handling connection events.

#### `(*Callback).OnConnect(handler func(client mqtt.Client, retryTimes uint64) (CallbackState, error)) *Callback`

Registers callback to run on connect/reconnect. Supports chaining multiple handlers.

### Token Utilities

#### `CheckToken(token mqtt.Token, timeout time.Duration) (TokenState, error)`

Checks token completion with timeout.

**Returns:**
- `TokenStateSuccess`: Operation completed
- `TokenStateTimeout`: Exceeded timeout
- `TokenStateUnknown`: Error occurred

#### `WaitToken(token mqtt.Token) (TokenState, error)`

Waits on token completion without timeout.

### Configuration

```go
type Config struct {
    BrokerServer string // MQTT broker URL (tcp:// or ws://)
    Username     string // Authentication username
    Password     string // Authentication password
    OrderMatters bool   // Whether message order matters
}
```

### Callback States

```go
const (
    CallbackSuccess CallbackState = "success" // Operation succeeded
    CallbackRetries CallbackState = "retries" // Retry requested
    CallbackTimeout CallbackState = "timeout" // Stop retrying
    CallbackUnknown CallbackState = "unknown" // Unknown error, retry
)
```

## Examples

### TCP Connection

```go
config := &mqttgo.Config{
    BrokerServer: "tcp://localhost:1883",
    Username:     "username",
    Password:     "password",
}
```

### WebSocket Connection

```go
config := &mqttgo.Config{
    BrokerServer: "ws://127.0.0.1:8083/mqtt",
    Username:     "username",
    Password:     "password",
}
```

### Multiple Subscription Topics

```go
callback := mqttgo.NewCallback().
    OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
        topics := map[string]byte{
            "topic/one":   1,
            "topic/two":   1,
            "topic/three": 2,
        }
        token := c.SubscribeMultiple(topics, messageHandler)
        state, err := mqttgo.WaitToken(token)
        if err != nil || state != mqttgo.TokenStateSuccess {
            return mqttgo.CallbackRetries, err
        }
        return mqttgo.CallbackSuccess, nil
    })
```

### Custom Retry Logic

**Timeout past 10 retries:**

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

**Immediate retry on failures:**

```go
OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
    token := c.Subscribe("topic", 1, handler)
    if state, err := mqttgo.WaitToken(token); err != nil {
        return mqttgo.CallbackRetries, err
    }
    return mqttgo.CallbackSuccess, nil
})
```

### Custom Logger

```go
type CustomLogger struct{}

func (l *CustomLogger) ErrorLog(msg string, fields ...zap.Field) {
    // Your error logging logic
}

func (l *CustomLogger) DebugLog(msg string, fields ...zap.Field) {
    // Your debug logging logic
}

func main() {
    mqttgo.SetLog(&CustomLogger{})
    // Create clients...
}
```

## What makes go-mqtt different?

The Eclipse Paho MQTT client does not auto-resubscribe to topics following reconnection ([Issue #22](https://github.com/eclipse/paho.mqtt.golang/issues/22)). This package solves that problem:

1. **OnConnect Callbacks**: Executes subscription logic on each connect/reconnect
2. **Retry Mechanism**: Auto retries failed subscriptions with 100ms backoff
3. **State Management**: Simple callback states to manage retry patterns
4. **Mate Integration**: Leverages `rese`, `must`, `erero` to write clean code

## Running EMQX to Test

See [`internal/sketches/sketch1/README.md`](internal/sketches/sketch1/README.md) about Docker setup instructions.

## Running Embedded MQTT

See [`internal/sketches/sketch2/README.md`](internal/sketches/sketch2/README.md) about embedded MQTT setup using `github.com/mochi-mqtt/server/v2`.

<!-- TEMPLATE (EN) BEGIN: STANDARD PROJECT FOOTER -->
<!-- VERSION 2025-09-26 07:39:27.188023 +0000 UTC -->

## 📄 License

MIT License. See [LICENSE](LICENSE).

---

## 🤝 Contributing

Contributions are welcome! Report bugs, suggest features, and contribute code:

- 🐛 **Found a mistake?** Open an issue on GitHub with reproduction steps
- 💡 **Have a feature idea?** Create an issue to discuss the suggestion
- 📖 **Documentation confusing?** Report it so we can improve
- 🚀 **Need new features?** Share the use cases to help us understand requirements
- ⚡ **Performance issue?** Help us optimize through reporting slow operations
- 🔧 **Configuration problem?** Ask questions about complex setups
- 📢 **Follow project progress?** Watch the repo to get new releases and features
- 🌟 **Success stories?** Share how this package improved the workflow
- 💬 **Feedback?** We welcome suggestions and comments

---

## 🔧 Development

New code contributions, follow this process:

1. **Fork**: Fork the repo on GitHub (using the webpage UI).
2. **Clone**: Clone the forked project (`git clone https://github.com/yourname/repo-name.git`).
3. **Navigate**: Navigate to the cloned project (`cd repo-name`)
4. **Branch**: Create a feature branch (`git checkout -b feature/xxx`).
5. **Code**: Implement the changes with comprehensive tests
6. **Testing**: (Golang project) Ensure tests pass (`go test ./...`) and follow Go code style conventions
7. **Documentation**: Update documentation to support client-facing changes and use significant commit messages
8. **Stage**: Stage changes (`git add .`)
9. **Commit**: Commit changes (`git commit -m "Add feature xxx"`) ensuring backward compatible code
10. **Push**: Push to the branch (`git push origin feature/xxx`).
11. **PR**: Open a merge request on GitHub (on the GitHub webpage) with detailed description.

Please ensure tests pass and include relevant documentation updates.

---

## 🌟 Support

Welcome to contribute to this project via submitting merge requests and reporting issues.

**Project Support:**

- ⭐ **Give GitHub stars** if this project helps you
- 🤝 **Share with teammates** and (golang) programming friends
- 📝 **Write tech blogs** about development tools and workflows - we provide content writing support
- 🌟 **Join the ecosystem** - committed to supporting open source and the (golang) development scene

**Have Fun Coding with this package!** 🎉🎉🎉

<!-- TEMPLATE (EN) END: STANDARD PROJECT FOOTER -->

---

## GitHub Stars

[![Stargazers](https://starchart.cc/yylego/go-mqtt.svg?variant=adaptive)](https://starchart.cc/yylego/go-mqtt)
