package redis

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRedisIntegration(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Test taskQueue
	redisClient.LPush(context.Background(), "taskQueue", "task-1")
	queue, _ := redisClient.LRange(context.Background(), "taskQueue", 0, -1).Result()
	assert.Equal(t, []string{"task-1"}, queue)

	// Test taskState
	redisClient.HSet(context.Background(), "taskState", "task-1", "Pending")
	state, _ := redisClient.HGet(context.Background(), "taskState", "task-1").Result()
	assert.Equal(t, "Pending", state)
}
