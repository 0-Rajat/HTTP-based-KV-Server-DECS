package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type Result struct {
	responseTime time.Duration
	isError      bool
}

var popularKeys = []string{"key-1", "key-2", "key-3", "key-4", "key-5"}

func primePopularKeys() {
	client := &http.Client{Timeout: 5 * time.Second}
	for _, key := range popularKeys {
		val := "data-" + key
		req, err := http.NewRequest("PUT", "http://localhost:8080/kv/"+key, bytes.NewBufferString(val))
		if err != nil {
			log.Printf("Failed to create prime request: %v", err)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to prime key %s: %v", key, err)
			continue
		}
		resp.Body.Close()
	}
}

func main() {
	numClients := flag.Int("clients", 10, "Number of concurrent clients")
	durationSec := flag.Int("duration", 30, "Test duration in seconds")
	workloadType := flag.String("workload", "get-popular", "Type: get-popular, put-all, get-all, or mixed")
	flag.Parse()

	if *workloadType == "get-popular" || *workloadType == "mixed" {
		primePopularKeys()
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))
	resultsChan := make(chan Result)
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go runClient(i, *workloadType, resultsChan, &wg, stopChan)
	}

	go func() {
		time.Sleep(time.Duration(*durationSec) * time.Second)
		close(stopChan)
	}()

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var totalRequests int64
	var totalErrors int64
	var totalResponseTime time.Duration

	for res := range resultsChan {
		totalRequests++
		totalResponseTime += res.responseTime
		if res.isError {
			totalErrors++
		}
	}

	testDuration := time.Duration(*durationSec) * time.Second
	successfulRequests := totalRequests - totalErrors
	throughput := float64(successfulRequests) / testDuration.Seconds()

	var avgResponseTimeMs int64
	if totalRequests > 0 {
		avgResponseTimeMs = (totalResponseTime / time.Duration(totalRequests)).Milliseconds()
	}

	fmt.Println("\n===================================")
	fmt.Println("       LOAD TEST RESULTS")
	fmt.Println("===================================")
	fmt.Printf("Workload:            %s\n", *workloadType)
	fmt.Printf("Active Clients:      %d\n", *numClients)
	fmt.Printf("Duration:            %s\n", testDuration)
	fmt.Println("-----------------------------------")
	fmt.Printf("Total Requests:      %d\n", totalRequests)
	fmt.Printf("Success:             %d\n", successfulRequests)
	fmt.Printf("Failed:              %d\n", totalErrors)
	fmt.Println("-----------------------------------")
	fmt.Printf("THROUGHPUT:          %.2f reqs/sec\n", throughput)
	fmt.Printf("AVG RESPONSE TIME:   %d ms\n", avgResponseTimeMs)
	fmt.Println("===================================")
}

func runClient(id int, workload string, results chan<- Result, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	client := &http.Client{Timeout: 10 * time.Second}

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		startTime := time.Now()
		var req *http.Request
		var err error

		switch workload {
		case "get-popular":
			key := popularKeys[rand.Intn(len(popularKeys))]
			req, err = http.NewRequest("GET", "http://localhost:8080/kv/"+key, nil)

		case "put-all":
			key := fmt.Sprintf("key-%d-%d", id, time.Now().UnixNano())
			val := "some-data-payload"
			req, err = http.NewRequest("PUT", "http://localhost:8080/kv/"+key, bytes.NewBufferString(val))

		case "get-all":
			key := fmt.Sprintf("key-%d-%d", id, time.Now().UnixNano())
			req, err = http.NewRequest("GET", "http://localhost:8080/kv/"+key, nil)

		case "mixed":
			if rand.Float32() < 0.5 {
				key := popularKeys[rand.Intn(len(popularKeys))]
				req, err = http.NewRequest("GET", "http://localhost:8080/kv/"+key, nil)
			} else {
				key := fmt.Sprintf("key-%d-%d", id, time.Now().UnixNano())
				val := "data-mixed-" + key
				req, err = http.NewRequest("PUT", "http://localhost:8080/kv/"+key, bytes.NewBufferString(val))
			}

		default:
			log.Fatalf("Unknown workload type: %s", workload)
		}

		if err != nil {
			results <- Result{0, true}
			continue
		}

		resp, err := client.Do(req)
		responseTime := time.Since(startTime)

		isError := err != nil || resp.StatusCode >= 400
		if resp != nil {
			resp.Body.Close()
		}

		results <- Result{responseTime, isError}
	}
}
