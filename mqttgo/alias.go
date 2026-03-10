// Package mqttgo wraps Eclipse Paho MQTT Golang client with enhanced reconnection handling
// Provides simplified API for MQTT operations with mate ecosystem integration
// Solves automatic resubscription on reconnect using OnConnect callback pattern
//
// mqttgo 包封装 Eclipse Paho MQTT Golang 客户端，增强重连处理能力
// 提供简化的 MQTT 操作接口，集成 mate 生态工具链
// 通过 OnConnect 回调模式解决重连时的自动重订阅问题
package mqttgo

import mqtt "github.com/eclipse/paho.mqtt.golang"

// Client represents an MQTT client connection for publish and subscribe operations
// Alias to mqtt.Client from paho.mqtt.golang
//
// Client 表示用于发布和订阅操作的 MQTT 客户端连接
// 对应 paho.mqtt.golang 的 mqtt.Client 类型别名
type Client = mqtt.Client

// Message represents an MQTT message received from broker or to be published
// Contains topic, payload, QoS level and other metadata
//
// Message 表示从代理服务器接收或待发布的 MQTT 消息
// 包含主题、负载、QoS 级别及其他元数据
type Message = mqtt.Message

// Token represents an asynchronous operation handle for MQTT actions
// Used to track status of publish, subscribe and other operations
//
// Token 表示 MQTT 操作的异步操作句柄
// 用于跟踪发布、订阅等操作的状态
type Token = mqtt.Token
