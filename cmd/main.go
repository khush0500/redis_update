package main

import (
	"log"
	"regexp"
	"strings"

	"github.com/go-redis/redis"
)

func processKeys(client *redis.ClusterClient, regExp *regexp.Regexp, iter *redis.ScanIterator) {

	for iter.Next() {
		key := iter.Val()
		newKey := strings.Replace(key, "uniquekycuser", "uniquekycuser_rummy", -1)
		if !regExp.MatchString(key) {
			log.Println("Already done for:", key)
			continue
		}
		if client.Exists(newKey).Val() == 1 {
			log.Println("already converted")
			continue
		}
		value, err := client.HGetAll(key).Result()
		if err == redis.Nil {
			log.Println("Key", key, "does not exist")
		} else if err != nil {
			log.Println("Failed to retrieve value for key", key, ":", err)
		} else {
			log.Println("Key:", key, "Value:", value)
			newValue := map[string]interface{}{}
			for vkey, velement := range value {
				valueK := strings.Replace(vkey, "uniquekycuser", "uniquekycuser_rummy", -1)
				valueV := strings.Replace(velement, "uniquekycuser", "uniquekycuser_rummy", -1)
				newValue[valueK] = valueV
			}

			tx := client.TxPipeline()
			tx.HMSet(newKey, newValue)
			_, err := tx.Exec()
			if err == nil {
				log.Println("Hash map added successfully!")
			} else {
				log.Println("Failed to add hash map:", err)
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
		log.Println("Failed to connect to Redis:", err)
		return
	}
	log.Println("Connected to Redis:", pong)

	cursor := uint64(0)
	pattern := "uniquekycuser*"
	count := int64(1000)
	regExp := regexp.MustCompile(`^uniquekycuser_\d+$`)

	iter := client.Scan(cursor, pattern, count).Iterator()

	processKeys(client, regExp, iter)

	err = client.Close()
	if err != nil {
		log.Println("Failed to close Redis connection:", err)
	}
}
