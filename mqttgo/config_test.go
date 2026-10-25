package mqttgo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNewClientOptions checks the options built from Config, including the timing settings that keep a client online
// TestNewClientOptions 验证从 Config 生成的选项，包括让客户端保持在线的几个时间设置
func TestNewClientOptions(t *testing.T) {
	opts := NewClientOptions(&Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-username",
		Password:     "test-pass",
	}, "client-a")

	require.Equal(t, "client-a", opts.ClientID)
	require.Equal(t, "test-username", opts.Username)
	require.Len(t, opts.Servers, 1)
	require.Equal(t, testBrokerAddress, opts.Servers[0].String())

	require.Equal(t, int64(60), opts.KeepAlive)
	require.Equal(t, time.Second*10, opts.PingTimeout)
	require.Equal(t, time.Second*10, opts.MaxReconnectInterval)
	require.True(t, opts.AutoReconnect)
}
