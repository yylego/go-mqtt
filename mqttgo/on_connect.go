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
	"github.com/yylego/tern/zerotern"
	"go.uber.org/zap"
)

// CallbackState represents the status of an OnConnect callback execution
// Controls repeat actions following connection setup and reconnection events
// Decides on continuing reattempts, timeout conditions and completion upon success
//
// CallbackState 表示 OnConnect 回调执行的状态
// 控制连接建立或重连后的重复操作
// 决定是否继续重新尝试、超时还是成功完成
type CallbackState string

const (
	// CallbackUnknown indicates an unknown issue needing reattempt with backoff
	// Callback continues with 100ms gap between attempts
	//
	// CallbackUnknown 表示需要退避重新尝试的未知错误
	// 回调将以每次尝试之间 100ms 的延迟继续
	CallbackUnknown CallbackState = "unknown"

	// CallbackRetries indicates an explicit request to reattempt the callback
	// Analogous to Unknown yet gives explicit purpose when reattempting
	//
	// CallbackRetries 表示明确请求重新尝试回调
	// 类似于 Unknown 但为重新尝试逻辑提供明确意图
	CallbackRetries CallbackState = "retries"

	// CallbackTimeout indicates the callback should stop reattempting when timeout occurs
	// Ends the loop even when operation has not succeeded
	//
	// CallbackTimeout 表示回调因超时应停止重新尝试
	// 即使操作未成功也终止循环
	CallbackTimeout CallbackState = "timeout"

	// CallbackSuccess indicates the callback completed without issues
	// Stops the reattempt loop and confirms operation finished as expected
	//
	// CallbackSuccess 表示回调无错误完成
	// 停止重新尝试循环并确认操作按预期完成
	CallbackSuccess CallbackState = "success"
)

// OnConnectWithRetries runs a callback function with reattempt mechanism on connection events
// Continues reattempting until callback succeeds when client connection remains active
// Uses 100ms gap between attempts when operations need reattempts
//
// OnConnectWithRetries 在连接事件上运行带重新尝试逻辑的回调函数
// 持续重新尝试直到回调成功或客户端断开连接
// 失败操作的重新尝试之间使用 100ms 延迟
func OnConnectWithRetries(client mqtt.Client, onConnect func(client mqtt.Client, retryTimes uint64) (CallbackState, error)) {
	for retryTimes := uint64(0); client.IsConnected(); retryTimes++ {
		action, err := onConnect(client, retryTimes)
		if err != nil {
			action = zerotern.VV(action, CallbackUnknown)
			switch action {
			case CallbackUnknown, CallbackRetries:
				log.ErrorLog("run-on-connect-with-retries", zap.String("action", string(action)), zap.Uint64("retry_times", retryTimes), zap.Error(err))
				time.Sleep(time.Millisecond * 100)
				continue
			case CallbackTimeout, CallbackSuccess:
				log.DebugLog("run-on-connect-with-retries", zap.String("action", string(action)), zap.Uint64("retry_times", retryTimes))
				return
			}
		}
		log.DebugLog("run-on-connect-success-done", zap.String("action", string(action)), zap.Uint64("retry_times", retryTimes))
		return
	}
}
