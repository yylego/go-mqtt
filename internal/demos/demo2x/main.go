package main

import (
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/yylego/go-mqtt/internal/utils"
	"github.com/yylego/go-mqtt/mqttgo"
	"github.com/pkg/errors"
	"github.com/yylego/erero"
	"github.com/yylego/must"
	"github.com/yylego/neatjson/neatjsons"
	"github.com/yylego/rese"
	"github.com/yylego/zaplog"
)

func main() {
	const mqttTopic = "mqtt-go-demo2-topic"

	config := &mqttgo.Config{
		BrokerServer: "ws://127.0.0.1:8083/mqtt",
		Username:     "username",
		Password:     "password",
		OrderMatters: false,
	}

	client1 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			if retryTimes > 10 {
				return mqttgo.CallbackTimeout, nil
			}
			if rand.IntN(100) >= 10 {
				return mqttgo.CallbackUnknown, erero.New("random-rate-not-success")
			}
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client1.Disconnect(500)

	client2 := rese.V1(mqttgo.NewClientWithCallback(config, utils.NewUUID(), mqttgo.NewCallback().
		OnConnect(func(c mqttgo.Client, retryTimes uint64) (mqttgo.CallbackState, error) {
			token := c.Subscribe(mqttTopic, 1, func(client mqttgo.Client, message mqttgo.Message) {
				zaplog.SUG.Debugln("subscribe-msg:", neatjsons.SxB(message.Payload()))
			})
			tokenState, err := mqttgo.WaitToken(token)
			if err != nil {
				return mqttgo.CallbackRetries, errors.WithMessage(err, "subscribe-is-wrong")
			}
			must.Same(tokenState, mqttgo.TokenStateSuccess)
			return mqttgo.CallbackSuccess, nil
		}),
	))
	defer client2.Disconnect(500)

	type MessageType struct {
		A string
		B int
		C float64
	}

	for i := 0; i < 100; i++ {
		msg := &MessageType{
			A: time.Now().String(),
			B: i,
			C: rand.Float64(),
		}
		contentBytes := rese.A1(json.Marshal(msg))

		zaplog.SUG.Debugln("publish-msg:", neatjsons.SxB(contentBytes))

		token := client1.Publish(mqttTopic, 1, false, contentBytes)
		must.Same(rese.C1(mqttgo.CheckToken(token, time.Second*3)), mqttgo.TokenStateSuccess)
		time.Sleep(time.Second)
	}
}
