package fortinet

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

func TestProcess(t *testing.T) {

	file, err := os.Open("/home/vivek/Downloads/logscount1.txt")

	if err != nil {
		panic(err)
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	var logs []string

	logCount := 0
	errCount := 0

	for {
		if log, err := reader.ReadString('\n'); err == nil {
			logCount++
			logs = append(logs, log)
		} else {
			if err == io.EOF {
				break
			}
			errCount++
		}
	}

	fmt.Println("logs: ", logCount, "errors", errCount)

	// Create channels for logs and results.
	inputChan := make(chan string)
	outputChan := make(chan map[string]interface{})

	var wg sync.WaitGroup

	// Number of workers to process logs concurrently.
	numWorkers := 8

	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(inputChan, &wg, nil)
	}

	t1 := time.Now().UnixMilli()
	fmt.Println("processing started: ", t1)

	// Feeding log messages to the channel.
	go func() {
		fmt.Println("feeding", len(logs), "logs")
		for _, log := range logs {
			inputChan <- log
		}
		close(inputChan)
	}()

	// Create a goroutine to collect the processed results.
	var processedCount int64
	go func() {
		for result := range outputChan {
			processedCount++
			// You can process the result further here or just count them
			_ = result
		}
	}()

	// Wait for all workers to finish processing.
	wg.Wait()
	close(outputChan)

	t2 := time.Now().UnixMilli()

	fmt.Println("processing finished: ", t2, "time taken:", t2-t1, "processed logs:", processedCount)
	eps := processedCount * 1000 / (t2 - t1)
	fmt.Println("EPS:", eps)

}
