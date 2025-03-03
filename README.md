# Rescoot Redis IPC Library

Redis-based IPC library implementing pub/sub messaging, queue processing, and atomic transactions.
This is a clean-room reimplementation of the IPC functionality from unu's usk library.

## Core Architecture

Single-client Redis multiplexer handling four IPC patterns:

1. **Pub/Sub Messaging**: Topic-based message distribution via Redis PUBLISH/SUBSCRIBE
2. **Request Processing**: Blocking queue consumers using BRPOP/LPUSH
3. **Atomic Transactions**: Pipelined command groups with MULTI/EXEC semantics
4. **Direct Command Execution**: Immediate execution of Redis commands with results

## Usage Example

```go
// Initialize with connection params
client := redis_ipc.New(redis_ipc.Config{
    Address: "localhost",
    Port:    6379,
})

// Subscribe to messages
sg := client.Subscribe("group")
sg.Handle("channel", func(msg []byte) error {
    log.Printf("Received: %s", msg)
    return nil
})

// Process queue items
client.HandleRequests("queue", func(data []byte) error {
    log.Printf("Processing: %s", data)
    return nil
})

// Direct command execution
value, err := client.Get("mykey")
if err != nil {
    log.Printf("Error: %v", err)
}
log.Printf("Value: %s", value)

// Using convenience methods
err = client.Set("mykey", "newvalue", 0)
count, err := client.Incr("counter")
hashValue, err := client.HGet("myhash", "field")

// Execute atomic transactions with results
txg := client.NewTxGroup("tx")
txg.Add("SET", "key", "value")
txg.Add("GET", "key")
txg.Add("INCR", "counter")

results, err := txg.Exec()
if err != nil {
    log.Printf("Transaction failed: %v", err)
}
log.Printf("SET result: %v", results[0])
log.Printf("GET result: %v", results[1])
log.Printf("INCR result: %v", results[2])
```

## Concurrency Model

- Multiplexed Redis connections (single pool)
- Message routing via concurrent maps
- Lock-free handler dispatch
- Thread-safe transaction pipelining

## Dependencies

- Redis server
- go-redis

## Installation

```bash
go get github.com/rescoot/redis-ipc
```

## License

AGPL-3.0, see LICENSE for details
