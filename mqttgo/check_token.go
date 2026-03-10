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
)

// TokenState represents the completion status of an MQTT operation token
// Used to identify whether operations succeeded, timed out, or encountered errors
//
// TokenState 表示 MQTT 操作令牌的完成状态
// 用于识别操作是成功、超时还是遇到错误
type TokenState string

const (
	// TokenStateUnknown indicates an unknown error occurred during token operation
	// Usually means the operation failed with an unexpected condition
	//
	// TokenStateUnknown 表示令牌操作期间发生未知错误
	// 通常意味着操作因意外情况失败
	TokenStateUnknown TokenState = "unknown"

	// TokenStateTimeout indicates the token operation exceeded the allowed time limit
	// Returned when WaitTimeout expires before operation completes
	//
	// TokenStateTimeout 表示令牌操作超过允许的时间限制
	// 在 WaitTimeout 过期且操作未完成时返回
	TokenStateTimeout TokenState = "timeout"

	// TokenStateSuccess indicates the token operation completed without errors
	// Confirms the MQTT action finished as expected
	//
	// TokenStateSuccess 表示令牌操作成功完成且无错误
	// 确认 MQTT 操作按预期完成
	TokenStateSuccess TokenState = "success"
)

// CheckToken checks token completion with timeout and returns operation status
// Waits up to timeout duration for token to complete before returning
// Returns TokenStateSuccess if operation finishes without error
//
// CheckToken 检查令牌完成情况并设置超时，返回操作状态
// 等待最多 timeout 时长以等令牌完成
// 若操作无错误完成则返回 TokenStateSuccess
func CheckToken(token mqtt.Token, timeout time.Duration) (TokenState, error) {
	if success := token.WaitTimeout(timeout); !success {
		return TokenStateTimeout, erero.New("MQTT token operation timeout")
	}
	if err := token.Error(); err != nil {
		return TokenStateUnknown, erero.Wro(err)
	}
	return TokenStateSuccess, nil
}

// WaitToken blocks until token operation completes and returns final status
// Does not timeout but waits for completion or client disconnect
// Returns TokenStateSuccess only when operation finishes without error
//
// WaitToken 阻塞直到令牌操作完成并返回最终状态
// 不设超时但会等待完成或客户端断开
// 仅在操作无错误完成时返回 TokenStateSuccess
func WaitToken(token mqtt.Token) (TokenState, error) {
	if !token.Wait() {
		return TokenStateUnknown, erero.New("MQTT token wait not complete")
	}
	if err := token.Error(); err != nil {
		return TokenStateUnknown, erero.Wro(err)
	}
	return TokenStateSuccess, nil
}
