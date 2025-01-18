package redis

import (
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	*redis.Client
}

// NewClient creates a new Redis client.
func NewClient(config config.Config) *Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr: config.RedisAddress,
	})
	return &Client{Client: redisClient}
}
