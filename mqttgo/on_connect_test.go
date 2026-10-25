package mqttgo

import (
	"errors"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yylego/rese"
)

// TestOnConnectWithRetries runs a callback that fails twice and then works, and checks that it gets three attempts
// TestOnConnectWithRetries 跑一个先失败两次再成功的回调，验证它被尝试了三次
func TestOnConnectWithRetries(t *testing.T) {
	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-username",
		Password:     "test-pass",
	}
	client := rese.V1(NewClient(NewClientOptions(cfg, uuid.NewString())))
	defer client.Disconnect(250)

	t.Run("reattempts-to-success", func(t *testing.T) {
		var attempts []uint64
		OnConnectWithRetries(client, func(client mqtt.Client, retryTimes uint64) (CallbackState, error) {
			attempts = append(attempts, retryTimes)
			if retryTimes < 2 {
				return CallbackRetries, errors.New("not up yet")
			}
			return CallbackSuccess, nil
		})
		require.Equal(t, []uint64{0, 1, 2}, attempts)
	})

	t.Run("timeout-stops-the-loop", func(t *testing.T) {
		var attempts []uint64
		OnConnectWithRetries(client, func(client mqtt.Client, retryTimes uint64) (CallbackState, error) {
			attempts = append(attempts, retryTimes)
			return CallbackTimeout, errors.New("give up")
		})
		require.Equal(t, []uint64{0}, attempts)
	})
}
