package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

func processKeys(client *redis.ClusterClient, keys []string, regExp *regexp.Regexp, wg *sync.WaitGroup) {

	for _, key := range keys {
		newKey := strings.Replace(key, "uniquekycuser", "uniquekycuser_rummy", -1)
		if !regExp.MatchString(key) {
			fmt.Println("Already done for:", key)
			continue
		}
		value, err := client.HGetAll(key).Result()
		if err == redis.Nil {
			fmt.Println("Key", key, "does not exist")
		} else if err != nil {
			fmt.Println("Failed to retrieve value for key", key, ":", err)
		} else {
			fmt.Println("Key:", key, "Value:", value)
			newValue := map[string]interface{}{}
			for vkey, velement := range value {
				valueK := strings.Replace(vkey, "uniquekycuser", "uniquekycuser_rummy", -1)
				valueV := strings.Replace(velement, "uniquekycuser", "uniquekycuser_rummy", -1)
				newValue[valueK] = valueV
			}

			tx := client.TxPipeline()
			tx.HMSet(newKey, newValue)
			tx.Del(key)
			_, err := tx.Exec()
			if err == nil {
				fmt.Println("Hash map added successfully!")
			} else {
				fmt.Println("Failed to add hash map:", err)
			}

		}
	}
}

func main() {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"jr-kube-nonprod-redis.2oaslo.clustercfg.aps1.cache.amazonaws.com:6379"},
	})

	pong, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Failed to connect to Redis:", err)
		return
	}
	fmt.Println("Connected to Redis:", pong)

	cursor := uint64(0)
	pattern := "uniquekycuser_*"
	count := int64(1000)
	regExp := regexp.MustCompile(`^uniquekycuser_\d+$`)

	var wg sync.WaitGroup

	for {
		keys, scanCursor, err := client.Scan(cursor, pattern, count).Result()
		if err != nil {
			fmt.Println("Failed to scan keys:", err)
			break
		}
		wg.Add(1)
		go func(keys []string, wg *sync.WaitGroup) {
			defer wg.Done()
			processKeys(client, keys, regExp, wg)
		}(keys, &wg)

		cursor = scanCursor

		if cursor == 0 {
			break
		}
	}

	wg.Wait()

	err = client.Close()
	if err != nil {
		fmt.Println("Failed to close Redis connection:", err)
	}
}
