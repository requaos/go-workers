package workers

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
)

const (
	DEFAULT_MAX_RETRY = 25
	LAYOUT            = "2006-01-02 15:04:05 MST"
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) processError(queue string, message *Msg, err error) error {
	if retry(message) {
		message.Set("queue", queue)
		message.Set("error_message", fmt.Sprintf("%v", err))
		retryCount := incrementRetry(message)

		waitDuration := durationToSecondsWithNanoPrecision(
			time.Duration(
				secondsToDelay(retryCount),
			) * time.Second,
		)

		rc := Config.Client
		_, rcErr := rc.ZAdd(Config.Namespace+RETRY_KEY, redis.Z{
			Score:  nowToSecondsWithNanoPrecision() + waitDuration,
			Member: message.ToJson(),
		}).Result()

		// If we can't add the job to the retry queue,
		// then we shouldn't acknowledge the job, otherwise
		// it'll disappear into the void.
		if rcErr != nil {
			return rcErr
		} else {
			return nil
		}
	} else {
		return err
	}
}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func() error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			lerr, ok := e.(error)
			if !ok {
				err = errors.New(fmt.Sprintf("Unable to get error from recover(): %v", e))
			} else {
				err = lerr
			}
		}

		if err != nil {
			err = r.processError(queue, message, err)
		}
	}()

	err = next()
	if err != nil {
		err = r.processError(queue, message, err)
	} else {
		val, err := message.Get("unique").Bool()
		if err != nil && val {
			rc := Config.Client
			sum := sha1.Sum([]byte(message.Args().ToJson()))
			rc.Del(hex.EncodeToString(sum[:])).Result()
		}
	}

	return
}

func retry(message *Msg) bool {
	retry := false
	max := DEFAULT_MAX_RETRY

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	} else if param, err := message.Get("retry").Int(); err == nil {
		max = param
		retry = true
	}

	count, _ := message.Get("retry_count").Int()

	return retry && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 0

	if count, err := message.Get("retry_count").Int(); err != nil {
		message.Set("failed_at", time.Now().UTC().Format(LAYOUT))
	} else {
		message.Set("retried_at", time.Now().UTC().Format(LAYOUT))
		retryCount = count + 1
	}

	message.Set("retry_count", retryCount)

	return
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
