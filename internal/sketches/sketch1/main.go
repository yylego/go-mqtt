package main

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/google/uuid"
	"github.com/yylego/must"
	"github.com/yylego/rese"
	"github.com/yylego/tern"
	"github.com/yylego/zaplog"
	"go.uber.org/zap"
)

func main() {
	const mqttTopic = "mqtt-go-sketch1-topic"

	for i := 0; i < 10; i++ {
		client := rese.V1(mqttgo.NewClient(NewClientOptions(func(client mqtt.Client) {
			token := client.Subscribe(mqttTopic, 1, func(client mqtt.Client, message mqtt.Message) {
				zaplog.SUG.Debugln("subscribe-msg:", string(message.Payload()))
			})
			must.Same(rese.C1(mqttgo.CheckToken(token, time.Minute)), mqttgo.TokenStateSuccess)
		})))
		must.True(client.IsConnected())
	}

	client := rese.V1(mqttgo.NewClient(NewClientOptions(nil)))
	must.True(client.IsConnected())
	defer client.Disconnect(250)
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

func NewClientOptions(onConnect func(client mqtt.Client)) *mqtt.ClientOptions {
	const brokerServer = "ws://127.0.0.1:8083/mqtt"

	opts := mqtt.NewClientOptions().
		AddBroker(brokerServer).
		SetClientID(rese.C1(uuid.NewUUID()).String()).
		SetUsername("username").
		SetPassword("password")

	opts.SetKeepAlive(60 * time.Second)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		zaplog.LOG.Debug("default-publish-handle", zap.String("topic", msg.Topic()), zap.ByteString("payload", msg.Payload()))
	}) // 设置消息回调处理函数
	opts.SetPingTimeout(1 * time.Second)
	opts.OnConnect = tern.BVV(onConnect != nil, onConnect, func(client mqtt.Client) {
		zaplog.LOG.Debug("on_connect", zap.Bool("is_connected", client.IsConnected()), zap.Time("time", time.Now()))
	})
	return opts
}
