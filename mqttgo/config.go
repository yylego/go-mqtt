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
	"github.com/yylego/must"
	"go.uber.org/zap"
)

// Config represents MQTT client settings for single or batch connections
// Contains authentication credentials and behavior parameters for broker communication
// Supports WebSocket connections via ws:// broker URLs
//
// Config 表示单个或批量客户端的 MQTT 连接配置
// 包含代理服务器的认证凭据和行为参数
// 通过 ws:// 代理服务器地址支持 WebSocket 连接
type Config struct {
	BrokerServer string // MQTT broker URL (e.g., tcp://host:port or ws://host:port) // 代理服务器地址（如 tcp://host:port 或 ws://host:port）
	Username     string // Authentication username for broker connection // 代理服务器连接的认证用户名
	Password     string // Authentication password for broker connection // 代理服务器连接的认证密码
	OrderMatters bool   // Whether message order matters in subscription handlers // 订阅处理程序中消息顺序是否重要
}

// NewClientOptions creates MQTT client options from configuration and client identifier
// Configures broker connection, authentication, keep-alive intervals and callbacks
// Sets default handlers for publish, connect and disconnect events
//
// NewClientOptions 从配置和客户端标识符创建 MQTT 客户端选项
// 配置代理服务器连接、认证、保活间隔和回调函数
// 设置发布、连接和断开事件的默认处理程序
func NewClientOptions(cfg *Config, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		AddBroker(must.Nice(cfg.BrokerServer)).
		SetClientID(must.Nice(clientID)).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetOrderMatters(cfg.OrderMatters) // When true, subscription handlers process messages in order without blocking other topics // 为 true 时，订阅处理程序按顺序处理消息且不阻塞其他主题

	opts.SetKeepAlive(60 * time.Second)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		log.DebugLog("default-publish-handle-function", zap.String("clientID", clientID), zap.String("topic", msg.Topic()))
	})
	opts.SetPingTimeout(1 * time.Second)
	opts.OnConnect = func(client mqtt.Client) {
		log.DebugLog("client-on-connected-on-reconnected-callback", zap.String("clientID", clientID))
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		log.ErrorLog("client-on-lost-connection-callback", zap.String("clientID", clientID), zap.Error(err))
	}
	return opts
}
