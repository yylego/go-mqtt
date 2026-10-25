// Package mqttgo wraps Eclipse Paho MQTT Golang client with enhanced reconnection handling
// Provides simplified API for MQTT operations with mate ecosystem integration
// Solves automatic resubscription on reconnect using OnConnect callback pattern
//
// mqttgo 包封装 Eclipse Paho MQTT Golang 客户端，增强重连处理能力
// 提供简化的 MQTT 操作接口，集成 mate 生态工具链
// 通过 OnConnect 回调模式解决重连时的自动重订阅问题
package mqttgo

import (
	"sync/atomic"

	"github.com/yylego/erero"
	"github.com/yylego/zaplog"
	"go.uber.org/zap"
)

// Log defines structured logging interface for MQTT operations and events
// Supports custom implementations via SetLog for integration with different frameworks
// Default implementation uses zaplog from mate ecosystem
//
// Log 定义 MQTT 操作和事件的结构化日志接口
// 通过 SetLog 支持自定义实现，以集成不同的日志框架
// 默认实现使用 mate 生态的 zaplog
type Log interface {
	ErrorLog(msg string, fields ...zap.Field) // Logs error-level messages with structured fields // 记录错误级别消息及结构化字段
	DebugLog(msg string, fields ...zap.Field) // Logs debug-level messages with structured fields // 记录调试级别消息及结构化字段
}

// currentLog holds the Log in use; it is atomic since clients log from background goroutines while SetLog can run
// currentLog 保存当前使用的 Log；客户端在后台协程里打日志时也可能有人调用 SetLog，所以用原子指针
var currentLog atomic.Pointer[Log]

func init() {
	SetLog(newLog())
}

// SetLog replaces the Log in use with a custom implementation
// Allows integration with existing logging infrastructure
// Safe at each point in time, even once clients run; messages logged past this point go to the new Log
//
// SetLog 用自定义实现替换当前使用的日志
// 允许与现有日志基础设施集成
// 随时可以调用，客户端已经在跑也安全；调用之后的日志写到新的 Log
func SetLog(mqttLog Log) {
	if mqttLog == nil {
		panic(erero.New("mqttgo.SetLog: mqttLog must not be nil"))
	}
	currentLog.Store(&mqttLog)
}

// getLog returns the Log in use
// getLog 返回当前使用的 Log
func getLog() Log {
	return *currentLog.Load()
}

type zapLog struct{}

func newLog() *zapLog {
	return &zapLog{}
}

func (z *zapLog) ErrorLog(msg string, fields ...zap.Field) {
	zaplog.LOGS.Skip(1).Error(msg, fields...)
}

func (z *zapLog) DebugLog(msg string, fields ...zap.Field) {
	zaplog.LOGS.Skip(1).Debug(msg, fields...)
}
