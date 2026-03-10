// Package mqttgo wraps Eclipse Paho MQTT Golang client with enhanced reconnection handling
// Provides simplified API for MQTT operations with mate ecosystem integration
// Solves automatic resubscription on reconnect using OnConnect callback pattern
//
// mqttgo 包封装 Eclipse Paho MQTT Golang 客户端，增强重连处理能力
// 提供简化的 MQTT 操作接口，集成 mate 生态工具链
// 通过 OnConnect 回调模式解决重连时的自动重订阅问题
package mqttgo

import (
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

var log Log = newLog()

// SetLog replaces the default logger with a custom implementation
// Allows integration with existing logging infrastructure
// Must be called before creating any MQTT clients to take effect
//
// SetLog 用自定义实现替换默认日志记录器
// 允许与现有日志基础设施集成
// 必须在创建任何 MQTT 客户端之前调用才能生效
func SetLog(mqttLog Log) {
	log = mqttLog
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
