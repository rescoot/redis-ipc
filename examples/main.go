package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis_ipc "rescoot-redis-ipc"
)

const (
	testChannel = "test-channel"
	testQueue   = "test-queue"
	testKey     = "test-key"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Initializing IPC client...")
	client, err := redis_ipc.New(redis_ipc.Config{
		Address:       "localhost",
		Port:          6379,
		RetryInterval: time.Second,
		MaxRetries:    3,
	})
	if err != nil {
		log.Fatalf("Failed to create IPC client: %v", err)
	}
	defer client.Close()

	// Create message handler
	log.Printf("Setting up subscription handler...")
	sg := client.Subscribe("example")
	err = sg.Handle(testChannel, func(msg []byte) error {
		log.Printf("SUB: Received message on %s: %s", testChannel, string(msg))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to set up subscription handler: %v", err)
	}

	// Set up request processor
	log.Printf("Setting up request handler...")
	client.HandleRequests(testQueue, func(data []byte) error {
		log.Printf("QUEUE: Processing request from %s: %s", testQueue, string(data))
		return nil
	})

	// Start test message publisher
	go func() {
		count := 0
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count++

				// Test direct command execution
				value := fmt.Sprintf("test-value-%d", count)
				log.Printf("DIRECT: Setting %s = %s", testKey, value)

				err := client.Set(testKey, value, 0)
				if err != nil {
					log.Printf("ERROR: Direct SET failed: %v", err)
					continue
				}

				// Read back the value using direct command
				readValue, err := client.Get(testKey)
				if err != nil {
					log.Printf("ERROR: Direct GET failed: %v", err)
					continue
				}
				log.Printf("DIRECT: Read %s = %s", testKey, readValue)

				// Test transaction with results
				txg := client.NewTxGroup("example-tx")
				txg.Add("SET", testKey+"-tx", value)
				txg.Add("GET", testKey+"-tx")
				txg.Add("INCR", "counter")

				results, err := txg.Exec()
				if err != nil {
					log.Printf("ERROR: Transaction failed: %v", err)
					continue
				}

				log.Printf("TX: Results: SET=%v, GET=%v, INCR=%v",
					results[0], results[1], results[2])

				// Publish test message
				msg := fmt.Sprintf("test-message-%d", count)
				log.Printf("PUB: Publishing to %s: %s", testChannel, msg)
				txg = client.NewTxGroup("pub-tx")
				txg.Add("PUBLISH", testChannel, msg)
				pubResults, err := txg.Exec()
				if err != nil {
					log.Printf("ERROR: Publish failed: %v", err)
					continue
				}
				log.Printf("PUB: Result: %v", pubResults[0])

				// Push test queue item
				item := fmt.Sprintf("test-item-%d", count)
				log.Printf("QUEUE: Pushing to %s: %s", testQueue, item)
				txg = client.NewTxGroup("queue-tx")
				txg.Add("LPUSH", testQueue, item)
				queueResults, err := txg.Exec()
				if err != nil {
					log.Printf("ERROR: Queue push failed: %v", err)
					continue
				}
				log.Printf("QUEUE: Result: %v", queueResults[0])
			}
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)
}
