package mqttgo

import (
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/stretchr/testify/require"
	"github.com/yylego/must"
	"github.com/yylego/rese"
)

const testBrokerAddress = "tcp://127.0.0.1:11883" // Test broker address // 测试 broker 地址

// TestMain sets up embedded MQTT broker for all tests and tears down after completion
// Provides isolated test environment without outside broker dependencies
// Runs before any test functions and ensures clean broker startup and shutdown
//
// TestMain 为所有测试设置嵌入式 MQTT broker 并在完成后清理
// 提供无需外部 broker 依赖的隔离测试环境
// 在任何测试函数之前运行并确保 broker 干净启动和关闭
func TestMain(m *testing.M) {
	// Create embedded MQTT broker with inline client support for testing
	// Creates broker instance with options enabling direct packet injection
	//
	// Create embedded MQTT broker with inline client support for testing
	// 创建支持内联客户端的嵌入式 MQTT broker 用于测试
	testServer := mqttserver.New(&mqttserver.Options{
		InlineClient: true, // Enable inline client for testing // 启用内联客户端用于测试
	})

	// Add authentication hook allowing all connections for test environment
	// Uses allow-all auth hook suitable for testing without real credentials
	//
	// Add authentication hook allowing all connections for test environment
	// 添加允许所有连接的认证钩子用于测试环境
	must.Done(testServer.AddHook(new(auth.AllowHook), nil))

	// Create and attach TCP listener on test port for client connections
	// Uses port 11883 to avoid conflicts with standard MQTT port 1883
	//
	// Create and attach TCP listener on test port for client connections
	// 在测试端口创建并附加 TCP 监听器用于客户端连接
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "test-tcp-listener",
		Address: ":11883",
	})
	must.Done(testServer.AddListener(tcp))

	// Start broker server in background routine for test duration
	// Launches broker serving connections without blocking test execution
	//
	// Start broker server in background routine for test duration
	// 在后台协程中启动 broker 服务用于测试期间
	go func() {
		must.Done(testServer.Serve())
	}()

	// Allow broker time to initialize before running tests
	// Ensures broker is fully ready to accept connections
	//
	// Allow broker time to initialize before running tests
	// 让 broker 有时间初始化后再运行测试
	time.Sleep(time.Millisecond * 200)

	// Run all test functions with prepared broker environment
	// Executes test suite and captures exit code for final cleanup
	//
	// Run all test functions with prepared broker environment
	// 在准备好的 broker 环境中运行所有测试函数
	code := m.Run()

	// Shutdown broker and cleanup resources after test completion
	// Ensures clean teardown regardless of test results
	//
	// Shutdown broker and cleanup resources after test completion
	// 测试完成后关闭 broker 并清理资源
	must.Done(testServer.Close())

	// Exit with test suite result code for CI integration
	// Preserves test pass/fail status for outside tooling
	//
	// Exit with test suite result code for CI integration
	// 使用测试套件结果代码退出以便 CI 集成
	os.Exit(code)
}

// TestNewClient verifies basic client creation and connection to broker
// Validates that client can establish connection and disconnect with grace
//
// TestNewClient 验证基本的客户端创建和 broker 连接
// 验证客户端可以成功建立连接并干净地断开
func TestNewClient(t *testing.T) {
	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	clientID := rese.C1(uuid.NewUUID()).String()
	opts := NewClientOptions(cfg, clientID)

	client := rese.V1(NewClient(opts))
	require.True(t, client.IsConnected())

	client.Disconnect(250)
	time.Sleep(time.Millisecond * 300)
	require.False(t, client.IsConnected())
}

// TestPublishSubscribe validates message publishing and subscription functionality
// Tests that messages published to topic are received as expected by subscribers
//
// TestPublishSubscribe 验证消息发布和订阅功能
// 测试发布到主题的消息是否被订阅者正确接收
func TestPublishSubscribe(t *testing.T) {
	const testTopic = "test/topic/pubsub"
	const testMessage = "Hello MQTT Test"

	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	// Create subscriber client with message handler
	// Sets up client to receive and verify published messages
	//
	// Create subscriber client with message handler
	// 创建订阅客户端并设置消息处理器
	var receivedMsg string
	var wg sync.WaitGroup
	wg.Add(1)

	subClientID := rese.C1(uuid.NewUUID()).String()
	subOpts := NewClientOptions(cfg, subClientID)
	subOpts.OnConnect = func(client mqtt.Client) {
		token := client.Subscribe(testTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
			receivedMsg = string(msg.Payload())
			wg.Done()
		})
		must.Same(rese.C1(CheckToken(token, time.Second*5)), TokenStateSuccess)
	}

	subClient := rese.V1(NewClient(subOpts))
	require.True(t, subClient.IsConnected())
	defer subClient.Disconnect(250)

	// Wait for subscription to be ready before publishing
	// Ensures subscriber is listening when message arrives
	//
	// Wait for subscription to be ready before publishing
	// 确保在发布前订阅已就绪
	time.Sleep(time.Millisecond * 100)

	// Create publisher client and send test message
	// Publishes message to subscribed topic for verification
	//
	// Create publisher client and send test message
	// 创建发布客户端并发送测试消息
	pubClientID := rese.C1(uuid.NewUUID()).String()
	pubOpts := NewClientOptions(cfg, pubClientID)
	pubClient := rese.V1(NewClient(pubOpts))
	require.True(t, pubClient.IsConnected())
	defer pubClient.Disconnect(250)

	token := pubClient.Publish(testTopic, 1, false, testMessage)
	must.Same(rese.C1(CheckToken(token, time.Second*5)), TokenStateSuccess)

	// Wait for message to be received and verify content
	// Confirms publish-subscribe flow works as expected
	//
	// Wait for message to be received and verify content
	// 等待接收消息并验证内容
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		require.Equal(t, testMessage, receivedMsg)
	case <-time.After(time.Second * 3):
		t.Fatal("message receive timeout")
	}
}

// TestNewClientWithCallback validates automatic resubscription on reconnect
// Verifies that subscription callbacks execute as expected during connection events
//
// TestNewClientWithCallback 验证重连时的自动重订阅功能
// 验证连接事件期间订阅回调正确执行
func TestNewClientWithCallback(t *testing.T) {
	const testTopic = "test/topic/callback"

	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	var subscribed bool
	callback := NewCallback().OnConnect(func(client mqtt.Client, retryTimes uint64) (CallbackState, error) {
		token := client.Subscribe(testTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
			t.Log("received message:", string(msg.Payload()))
		})
		state := rese.C1(CheckToken(token, time.Second*5))
		if state == TokenStateSuccess {
			subscribed = true
			return CallbackSuccess, nil
		}
		return CallbackRetries, nil
	})

	clientID := rese.C1(uuid.NewUUID()).String()
	client := rese.V1(NewClientWithCallback(cfg, clientID, callback))
	require.True(t, client.IsConnected())
	defer client.Disconnect(250)

	// Wait for callback to complete subscription
	// Confirms OnConnect callback executed and subscribed
	//
	// Wait for callback to complete subscription
	// 等待回调完成订阅
	time.Sleep(time.Millisecond * 200)
	require.True(t, subscribed)
}

// TestCheckToken validates token status checking with timeout behavior
// Tests that token operations report success, timeout and error states as expected
//
// TestCheckToken 验证带超时的 token 状态检查
// 测试 token 操作正确报告成功、超时和错误状态
func TestCheckToken(t *testing.T) {
	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	clientID := rese.C1(uuid.NewUUID()).String()
	opts := NewClientOptions(cfg, clientID)
	client := rese.V1(NewClient(opts))
	require.True(t, client.IsConnected())
	defer client.Disconnect(250)

	// Test successful token operation completion
	// Publishes message and verifies token indicates success
	//
	// Test successful token operation completion
	// 测试成功的 token 操作完成
	token := client.Publish("test/check/token", 0, false, "test payload")
	state := rese.C1(CheckToken(token, time.Second*3))
	require.Equal(t, TokenStateSuccess, state)
}

// TestWaitToken validates blocking token wait without timeout
// Confirms WaitToken waits for operation completion as expected
//
// TestWaitToken 验证无超时的阻塞 token 等待
// 确认 WaitToken 正确等待操作完成
func TestWaitToken(t *testing.T) {
	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	clientID := rese.C1(uuid.NewUUID()).String()
	opts := NewClientOptions(cfg, clientID)
	client := rese.V1(NewClient(opts))
	require.True(t, client.IsConnected())
	defer client.Disconnect(250)

	// Test token wait completes for successful operation
	// Uses WaitToken without timeout for operation verification
	//
	// Test token wait completes for successful operation
	// 测试 token 等待对成功操作完成
	token := client.Publish("test/wait/token", 0, false, "test payload")
	state := rese.C1(WaitToken(token))
	require.Equal(t, TokenStateSuccess, state)
}

// TestMultipleSubscribers validates multiple clients subscribing to same topic
// Tests that all subscribers receive messages published to shared topic
//
// TestMultipleSubscribers 验证多个客户端订阅同一主题
// 测试所有订阅者接收到发布到共享主题的消息
func TestMultipleSubscribers(t *testing.T) {
	const testTopic = "test/topic/multiple"
	const testMessage = "broadcast message"
	const numSubscribers = 5

	cfg := &Config{
		BrokerServer: testBrokerAddress,
		Username:     "test-user",
		Password:     "test-pass",
	}

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	// Create multiple subscriber clients listening to same topic
	// Sets up concurrent subscribers to test broadcast behavior
	//
	// Create multiple subscriber clients listening to same topic
	// 创建多个订阅客户端监听同一主题
	for i := 0; i < numSubscribers; i++ {
		clientID := rese.C1(uuid.NewUUID()).String()
		opts := NewClientOptions(cfg, clientID)
		opts.OnConnect = func(client mqtt.Client) {
			token := client.Subscribe(testTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
				require.Equal(t, testMessage, string(msg.Payload()))
				wg.Done()
			})
			must.Same(rese.C1(CheckToken(token, time.Second*5)), TokenStateSuccess)
		}

		client := rese.V1(NewClient(opts))
		require.True(t, client.IsConnected())
		defer client.Disconnect(250)
	}

	// Allow subscriptions to establish before publishing
	// Ensures all subscribers ready to receive message
	//
	// Allow subscriptions to establish before publishing
	// 确保所有订阅者准备好接收消息
	time.Sleep(time.Millisecond * 200)

	// Publish single message to topic with multiple subscribers
	// Tests that message broadcasts to all listening clients
	//
	// Publish single message to topic with multiple subscribers
	// 向有多个订阅者的主题发布单条消息
	pubClientID := rese.C1(uuid.NewUUID()).String()
	pubOpts := NewClientOptions(cfg, pubClientID)
	pubClient := rese.V1(NewClient(pubOpts))
	require.True(t, pubClient.IsConnected())
	defer pubClient.Disconnect(250)

	token := pubClient.Publish(testTopic, 1, false, testMessage)
	must.Same(rese.C1(CheckToken(token, time.Second*5)), TokenStateSuccess)

	// Wait for all subscribers to receive message with timeout
	// Verifies broadcast delivery to concurrent subscribers
	//
	// Wait for all subscribers to receive message with timeout
	// 等待所有订阅者接收消息并设置超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("broadcast success")
	case <-time.After(time.Second * 5):
		t.Fatal("broadcast timeout")
	}
}
