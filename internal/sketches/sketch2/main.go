package main

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/google/uuid"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/yylego/must"
	"github.com/yylego/rese"
	"github.com/yylego/tern"
	"github.com/yylego/zaplog"
	"go.uber.org/zap"
)

func main() {
	// Create embedded MQTT broker instance
	// Creates broker for embedded testing with TCP listener
	//
	// Create embedded MQTT broker instance
	// 创建嵌入式 MQTT broker 实例用于测试
	server := mqttserver.New(&mqttserver.Options{
		InlineClient: true, // Enable inline client for direct injection // 启用内联客户端用于直接注入
	})

	// Add authentication hook for basic security
	// Adds username/password auth with allow-all rule for testing
	//
	// Add authentication hook for basic security
	// 添加基础认证钩子提供安全性
	must.Done(server.AddHook(new(auth.AllowHook), nil))

	// Create TCP listener on port 11883 for local testing
	// Creates TCP listener avoiding conflicts with standard port 1883
	//
	// Create TCP listener on port 11883 for local testing
	// 在端口 11883 创建 TCP 监听器用于本地测试
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "tcp-listener",
		Address: ":11883",
	})
	must.Done(server.AddListener(tcp))

	// Start broker server in background routine
	// Starts broker serving connections in non-blocking mode
	//
	// Start broker server in background routine
	// 在后台协程中启动 broker 服务
	go func() {
		must.Done(server.Serve())
	}()
	defer func() {
		must.Done(server.Close())
	}()

	// Wait for broker to initialize and start listening
	// Ensures broker is ready before client connections attempt
	//
	// Wait for broker to initialize and start listening
	// 等待 broker 初始化并开始监听
	time.Sleep(time.Millisecond * 100)

	zaplog.LOG.Info("embedded-mqtt-broker-started", zap.String("address", "tcp://127.0.0.1:11883"))

	const mqttTopic = "mqtt-go-sketch2-topic"

	// Create multiple subscriber clients for testing concurrency
	// Creates 10 subscriber clients listening to same topic
	//
	// Create multiple subscriber clients for testing concurrency
	// 创建多个订阅客户端用于测试并发
	for i := 0; i < 10; i++ {
		client := rese.V1(mqttgo.NewClient(NewClientOptions(func(client mqtt.Client) {
			token := client.Subscribe(mqttTopic, 1, func(client mqtt.Client, message mqtt.Message) {
				zaplog.SUG.Debugln("subscribe-msg:", string(message.Payload()))
			})
			must.Same(rese.C1(mqttgo.CheckToken(token, time.Minute)), mqttgo.TokenStateSuccess)
		})))
		must.True(client.IsConnected())
	}

	// Create publisher client for sending messages
	// Creates single publisher to send test messages to subscribers
	//
	// Create publisher client for sending messages
	// 创建发布客户端用于发送消息
	client := rese.V1(mqttgo.NewClient(NewClientOptions(nil)))
	must.True(client.IsConnected())
	defer client.Disconnect(250)

	// Publish test messages to subscribed clients
	// Sends 100 timestamped messages with status checking
	//
	// Publish test messages to subscribed clients
	// 向订阅客户端发布测试消息
	for i := 0; i < 100; i++ {
		content := time.Now().String()
		zaplog.SUG.Debugln("publish-msg:", content)
		token := client.Publish(mqttTopic, 1, false, content)
		if ok := token.WaitTimeout(time.Second * 3); !ok {
			zaplog.LOG.Debug("publish-msg-timeout")
		}
		if err := token.Error(); err != nil {
			zaplog.LOG.Error("publish-msg", zap.Error(err))
		}
		time.Sleep(time.Second)
	}
}

// NewClientOptions creates MQTT client options for embedded broker connection
// Configures client with UUID, keep-alive, and callback handlers for local testing
//
// NewClientOptions creates MQTT client options for embedded broker connection
// 创建连接嵌入式 broker 的 MQTT 客户端配置选项
func NewClientOptions(onConnect func(client mqtt.Client)) *mqtt.ClientOptions {
	const brokerServer = "tcp://127.0.0.1:11883" // Embedded broker address // 嵌入式 broker 地址

	opts := mqtt.NewClientOptions().
		AddBroker(brokerServer).
		SetClientID(rese.C1(uuid.NewUUID()).String()).
		SetUsername("username").
		SetPassword("password")

	opts.SetKeepAlive(60 * time.Second)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		zaplog.LOG.Debug("default-publish-handle", zap.String("topic", msg.Topic()), zap.ByteString("payload", msg.Payload()))
	})
	opts.SetPingTimeout(1 * time.Second)
	opts.OnConnect = tern.BVV(onConnect != nil, onConnect, func(client mqtt.Client) {
		zaplog.LOG.Debug("on_connect", zap.Bool("is_connected", client.IsConnected()), zap.Time("time", time.Now()))
	})
	return opts
}
