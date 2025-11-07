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
	fmt.Println("Priming database with popular keys...")

	for _, key := range popularKeys {
		val := "data-" + key
		req, err := http.NewRequest("PUT", "http://localhost:8080/kv/"+key, bytes.NewBufferString(val))
		if err != nil {
			log.Printf("Failed to create prime request for %s: %v", key, err)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to prime key %s: %v", key, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			log.Printf("Warning: Priming %s returned status %d", key, resp.StatusCode)
		}
	}
	fmt.Println("Priming complete.")
}

func main() {
	numClients := flag.Int("clients", 10, "Number of concurrent clients (users)")
	durationSec := flag.Int("duration", 30, "Test duration in seconds")
	workloadType := flag.String("workload", "get-popular", "Workload type: get-popular, put-all, or get-all")
	flag.Parse()

	if *workloadType == "get-popular" {
		primePopularKeys()
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))
	resultsChan := make(chan Result)
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	fmt.Printf("Starting load test with %d clients for %d seconds...\n", *numClients, *durationSec)

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

	fmt.Println("\n--- Load Test Finished ---")
	fmt.Printf("Workload Type:     %s\n", *workloadType)
	fmt.Printf("Test Duration:     %s\n", testDuration)
	fmt.Printf("Total Requests:    %d\n", totalRequests)
	fmt.Printf("Successful Requests: %d\n", successfulRequests)
	fmt.Printf("Failed Requests:   %d\n", totalErrors)
	fmt.Printf("Average Throughput:  %.2f reqs/sec\n", throughput)
	fmt.Printf("Average Response Time: %d ms\n", avgResponseTimeMs)
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
		default:
			log.Fatalf("Unknown workload type: %s", workload)
		}

		if err != nil {
			results <- Result{responseTime: 0, isError: true}
			continue
		}

		resp, err := client.Do(req)
		responseTime := time.Since(startTime)
		isError := false
		if err != nil || resp.StatusCode >= 400 {
			isError = true
		}
		if resp != nil {
			resp.Body.Close()
		}
		results <- Result{responseTime: responseTime, isError: isError}
	}
}
