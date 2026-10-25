# Sketch1 - EMQX Client Demo

Uses [EMQX](https://github.com/emqx/emqx) as the external MQTT message service.

This sketch shows:

- Connecting to EMQX via WebSocket (ws://127.0.0.1:8083/mqtt)
- Creating 10 clients that receive messages from the same topic
- Creating 1 client that sends 100 timestamped messages

## Setup EMQX with Docker

Start EMQX using Docker (note: old port 8081 has merged into port 18083 in new versions):

```bash
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx
```

Access the dashboard at `http://localhost:18083` (default: admin/public).

## Run

```bash
go run main.go
```
