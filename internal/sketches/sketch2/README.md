# Sketch2 - Embedded MQTT Setup

Uses [mochi-mqtt/server](https://github.com/mochi-mqtt/server) as the embedded MQTT engine.

This sketch demonstrates:
- Building an embedded MQTT service with `github.com/mochi-mqtt/server/v2`
- Launching the service on TCP port 11883
- Multiple clients that receive messages from the same topic
- Single client that sends messages to the topic

No need to set up outside MQTT infrastructure - the entire setup runs within the application.

## Run

```bash
go run main.go
```

The embedded service starts on `tcp://127.0.0.1:11883` and creates 10 receiving clients plus 1 sending client.
