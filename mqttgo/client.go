// Package mqttgo wraps Eclipse Paho MQTT Golang client with enhanced reconnection handling
// Provides simplified API for MQTT operations with mate ecosystem integration
// Solves automatic resubscription on reconnect using OnConnect callback pattern
//
// mqttgo 包封装 Eclipse Paho MQTT Golang 客户端，增强重连处理能力
// 提供简化的 MQTT 操作接口，集成 mate 生态工具链
// 通过 OnConnect 回调模式解决重连时的自动重订阅问题
package mqttgo

import (
	"time"

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
			getLog().DebugLog("client-connected-reconnected-run", zap.String("clientID", clientID))
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

// subscribeTimeout caps the wait to get one subscription confirmed
// subscribeTimeout 是等一次订阅被确认的最长时间
const subscribeTimeout = 10 * time.Second

// Subscribe adds a topic subscription that runs on connect and again on each reconnect
// It sends the subscription and waits up to 10 seconds to get it confirmed; a failure goes back to the reattempt loop
// Use OnConnect instead when the client needs more than plain subscriptions once connected
// Callbacks run in sequence, matching the sequence of adding, so a topic the service keeps rejecting holds up the ones added past it;
// an ERROR log then shows up each few seconds, and fixing the topic name and permission clears it
// Returns self to support method chaining
//
// Subscribe 添加一个主题订阅，连接时执行，每次重连后再执行一遍
// 发出订阅后最多等 10 秒确认；失败交给重新尝试的循环再试
// 连上之后除了订阅还要做别的事时，改用 OnConnect
// 回调按添加顺序逐个执行，所以一个一直被服务拒绝的主题会挡住排在它后面的主题；
// 这时每隔几秒会有一条 ERROR 日志，改对主题或权限即可
// 返回自身以支持流式方法链
func (C *Callback) Subscribe(topic string, qos byte, onMessage mqtt.MessageHandler) *Callback {
	return C.OnConnect(func(client mqtt.Client, retryTimes uint64) (CallbackState, error) {
		if err := subscribeTopic(client, topic, qos, onMessage); err != nil {
			return CallbackRetries, err
		}
		return CallbackSuccess, nil
	})
}

// subscribeTopic subscribes one topic and waits up to subscribeTimeout to get it confirmed
// A rejection (such as no permission on the topic) comes back as a return code at 0x80 and up with no error on the token,
// so the code gets checked too; without this check a rejected topic would look subscribed and get no message
//
// subscribeTopic 订阅一个主题，最多等 subscribeTimeout 确认
// 被拒绝（如没有这个主题的权限）时 token 上没有错误，只是返回码为 0x80 及以上，所以返回码也要查；
// 不查的话被拒的主题看起来订阅成功了，却永远收不到消息
func subscribeTopic(client mqtt.Client, topic string, qos byte, onMessage mqtt.MessageHandler) error {
	token := client.Subscribe(topic, qos, onMessage)
	if _, err := CheckToken(token, subscribeTimeout); err != nil {
		return erero.WithMessagef(err, "subscribe topic %s", topic)
	}
	if subToken, ok := token.(*mqtt.SubscribeToken); ok {
		if code := subToken.Result()[topic]; code >= 0x80 {
			return erero.Errorf("subscribe topic %s rejected (return code %#x)", topic, code)
		}
	}
	return nil
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
