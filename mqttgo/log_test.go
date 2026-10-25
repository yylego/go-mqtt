package mqttgo

import (
	"errors"
	"sync"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yylego/rese"
	"go.uber.org/zap"
)

// logCapture keeps the ERROR messages it receives, so the test can check what the package logged
// logCapture 记下收到的 ERROR 消息，测试据此检查包打了哪些日志
type logCapture struct {
	mutex    sync.Mutex
	messages []string
}

func (c *logCapture) ErrorLog(msg string, fields ...zap.Field) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.messages = append(c.messages, msg)
}

func (c *logCapture) DebugLog(msg string, fields ...zap.Field) {}

func (c *logCapture) getMessages() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return append([]string(nil), c.messages...)
}

// TestSetLog swaps in a custom Log and checks that a failed callback attempt reaches it
// TestSetLog 换上自定义的 Log，验证回调失败的那一次会记到它上面
func TestSetLog(t *testing.T) {
	capture := &logCapture{}
	SetLog(capture)
	defer SetLog(newLog())

	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-username",
		Password:     "test-pass",
	}
	client := rese.V1(NewClient(NewClientOptions(cfg, uuid.NewString())))
	defer client.Disconnect(250)

	OnConnectWithRetries(client, func(client mqtt.Client, retryTimes uint64) (CallbackState, error) {
		if retryTimes == 0 {
			return CallbackRetries, errors.New("first attempt fails")
		}
		return CallbackSuccess, nil
	})
	require.Contains(t, capture.getMessages(), "run-on-connect-with-retries")
}
