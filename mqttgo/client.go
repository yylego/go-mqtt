// Package mqttgo wraps Eclipse Paho MQTT Golang client with enhanced reconnection handling
// Provides simplified API for MQTT operations with mate ecosystem integration
// Solves automatic resubscription on reconnect using OnConnect callback pattern
//
// mqttgo 包封装 Eclipse Paho MQTT Golang 客户端，增强重连处理能力
// 提供简化的 MQTT 操作接口，集成 mate 生态工具链
// 通过 OnConnect 回调模式解决重连时的自动重订阅问题
package mqttgo

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/yylego/erero"
	"github.com/yylego/must"
	"go.uber.org/zap"
)

// NewClientWithCallback creates an MQTT client with automatic resubscription on reconnect
// Addresses the issue where Paho client does not auto-resubscribe after reconnection
// Solution based on https://github.com/eclipse/paho.mqtt.golang/issues/22
// Implements OnConnect callback pattern for reliable subscription management
//
// NewClientWithCallback 创建支持重连时自动重订阅的 MQTT 客户端
// 解决 Paho 客户端重连后不会自动重订阅的问题
// 解决方案基于 https://github.com/eclipse/paho.mqtt.golang/issues/22
// 实现 OnConnect 回调模式以可靠管理订阅
func NewClientWithCallback(config *Config, clientID string, callback *Callback) (mqtt.Client, error) {
	clientOptions := NewClientOptions(must.Full(config), clientID)
	must.Full(callback)
	if onConnects := callback.onConnects; len(onConnects) > 0 {
		// OnConnect callbacks run in async mode according to https://github.com/eclipse-paho/paho.mqtt.golang/blob/35b84c5b6910d3125376886939d0b36a8284d22a/client.go#L614
		// This wrapping provides simple repeat logic when subscription operations need reattempts
		// OnConnect 回调按照源码异步执行
		// 此封装为订阅操作提供简单的重新尝试逻辑
		clientOptions.OnConnect = func(client mqtt.Client) {
			log.DebugLog("client-connected-reconnected-run", zap.String("clientID", clientID))
			for _, onConnect := range onConnects {
				OnConnectWithRetries(client, onConnect)
			}
		}
	}
	return NewClient(clientOptions)
}

// Callback manages OnConnect callback functions for automatic resubscription
// Stores multiple callback handlers that run when client connects or reconnects
// Supports chaining via fluent API pattern
//
// Callback 管理用于自动重订阅的 OnConnect 回调函数
// 存储在客户端连接或重连时运行的多个回调处理程序
// 通过流式 API 模式支持链式调用
type Callback struct {
	onConnects []func(client mqtt.Client, retryTimes uint64) (CallbackState, error)
}

// NewCallback creates a new callback handler collection for connection events
// Returns empty callback ready for OnConnect handler registration
//
// NewCallback 为连接事件创建新的回调处理程序集合
// 返回准备好注册 OnConnect 处理程序的空回调
func NewCallback() *Callback {
	return &Callback{}
}

// OnConnect adds a callback handler to run on client connect and reconnect events
// Supports multiple handlers that execute in registration order
// Returns self for fluent method chaining
//
// OnConnect 添加在客户端连接和重连事件上运行的回调处理程序
// 支持多个按注册顺序执行的处理程序
// 返回自身以支持流式方法链
func (C *Callback) OnConnect(onConnect func(client mqtt.Client, retryTimes uint64) (CallbackState, error)) *Callback {
	C.onConnects = append(C.onConnects, onConnect)
	return C
}

// NewClient creates and connects an MQTT client with given options
// Enables automatic reconnection when connection drops
// Blocks until initial connection succeeds or fails with error
//
// NewClient 使用给定选项创建并连接 MQTT 客户端
// 启用连接断开时的自动重连功能
// 阻塞直到初始连接成功或失败并返回错误
func NewClient(clientOptions *mqtt.ClientOptions) (mqtt.Client, error) {
	client := mqtt.NewClient(clientOptions)
	token := client.Connect()
	if ok := token.Wait(); !ok {
		return nil, erero.New("failed to connect MQTT broker")
	}
	if err := token.Error(); err != nil {
		return nil, erero.Wro(err)
	}
	return client, nil
}
