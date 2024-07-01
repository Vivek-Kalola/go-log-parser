package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"log-processor/constants"
	"log-processor/elastic"
	"log-processor/fortinet"
	"os"
	"sync"
	"time"
)

func main() {

	file, err := os.Open(constants.File(constants.CWD, "config.json"))
	if err != nil {
		log.Fatalf("Failed to open config file: %s", err)
	}
	defer file.Close()

	// Read the file content
	byteValue, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	// Define a map to hold the configuration
	config := make(map[string]string)

	// Unmarshal the JSON content into the map
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}

	kafkaBroker := config["kafkaBroker"]
	kafkaTopic := config["kafkaTopic"]
	kafkaGroupId := config["kafkaGroupId"]
	elasticHost := config["elasticHost"]
	elasticIndex := config["elasticIndex"]
	elasticCreateIndex := config["elasticCreateIndex"] // yes/no
	elasticUsername := config["elasticUsername"]
	elasticPassword := config["elasticPassword"]

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBroker},
		Topic:     kafkaTopic,
		GroupID:   kafkaGroupId,
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})

	// Create channels for logs and results.
	inputChan := make(chan string)
	var wg sync.WaitGroup

	// Number of workers to process logs concurrently.
	numWorkers := 8

	connection, err := elastic.NewConnection([]string{elasticHost}, elasticIndex, elasticCreateIndex, elasticUsername, elasticPassword)

	if err != nil {
		log.Fatal(err)
	}

	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go fortinet.Worker(inputChan, &wg, connection)
	}

	eps := uint64(0)

	totalLogs := uint64(0)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Println(time.Now(), "Kafka consumer Logs/Sec = ", eps/10, "Total Logs:", totalLogs)
			eps = 0

			if totalLogs+1000 >= constants.MaxUint64 {
				totalLogs = 0
			}
		}
	}()

	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				close(inputChan)
				fmt.Println("ERROR", "Error reading message: ", err)
				if err := r.Close(); err != nil {
					log.Fatal("failed to close reader:", err)
				}
				break
			}
			eps++
			totalLogs++
			inputChan <- string(m.Value)
			//fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}
	}()

	wg.Wait()
}
