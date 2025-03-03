package redis_ipc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Address       string
	Port          int
	RetryInterval time.Duration
	MaxRetries    int
}

type Client struct {
	cfg         Config
	redis       *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	subGroups   sync.Map
	reqHandlers sync.Map
}

func New(cfg Config) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	opts := &redis.Options{
		Addr:        fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
		DialTimeout: 2 * time.Second,
		PoolSize:    3,
		MaxRetries:  cfg.MaxRetries,
	}

	redis := redis.NewClient(opts)

	c := &Client{
		cfg:    cfg,
		redis:  redis,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := c.redis.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	go c.monitorConnection()
	return c, nil
}

func (c *Client) monitorConnection() {
	ticker := time.NewTicker(c.cfg.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.redis.Ping(context.Background()).Err(); err != nil {
				log.Printf("Connection check failed: %v", err)
			}
		}
	}
}

type SubscriberGroup struct {
	name     string
	client   *Client
	handlers map[string]func([]byte) error // init-only, no mutex
}

func (c *Client) Subscribe(name string) *SubscriberGroup {
	if sg, ok := c.subGroups.Load(name); ok {
		return sg.(*SubscriberGroup)
	}

	sg := &SubscriberGroup{
		name:     name,
		client:   c,
		handlers: make(map[string]func([]byte) error),
	}
	c.subGroups.Store(name, sg)
	return sg
}

func (sg *SubscriberGroup) Handle(channel string, handler func([]byte) error) error {
	sg.handlers[channel] = handler

	pubsub := sg.client.redis.Subscribe(sg.client.ctx, channel)

	subscribed := make(chan struct{})
	go func() {
		ch := pubsub.Channel()
		close(subscribed)
		for msg := range ch {
			if err := handler([]byte(msg.Payload)); err != nil {
				log.Printf("Handler error: %v", err)
			}
		}
	}()

	select {
	case <-subscribed:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("subscription timeout")
	}
}

type RequestHandler struct {
	name    string
	client  *Client
	handler func([]byte) error
}

func (c *Client) HandleRequests(name string, handler func([]byte) error) *RequestHandler {
	if rh, ok := c.reqHandlers.Load(name); ok {
		return rh.(*RequestHandler)
	}

	rh := &RequestHandler{
		name:    name,
		client:  c,
		handler: handler,
	}
	c.reqHandlers.Store(name, rh)
	go rh.processLoop()
	return rh
}

func (rh *RequestHandler) processLoop() {
	for {
		result, err := rh.client.redis.BRPop(rh.client.ctx, 1*time.Second, rh.name).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if rh.client.ctx.Err() != nil {
				return
			}
			log.Printf("BRPOP error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if err := rh.handler([]byte(result[1])); err != nil {
			log.Printf("Handler error: %v", err)
		}
	}
}

type TxGroup struct {
	name   string
	client *Client
	pipe   redis.Pipeliner // thread-safe internally
}

func (c *Client) NewTxGroup(name string) *TxGroup {
	return &TxGroup{
		name:   name,
		client: c,
		pipe:   c.redis.Pipeline(),
	}
}

func (g *TxGroup) Add(cmd string, args ...interface{}) error {
	cmdArgs := append([]interface{}{cmd}, args...)
	return g.pipe.Do(g.client.ctx, cmdArgs...).Err()
}

func (g *TxGroup) Exec() error {
	// Execute pipeline, ignore return value, we don't need it
	_, err := g.pipe.Exec(g.client.ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	// Reset pipeline after execution
	g.pipe = g.client.redis.Pipeline()
	return nil
}

func (c *Client) Close() error {
	c.cancel()
	return c.redis.Close()
}
