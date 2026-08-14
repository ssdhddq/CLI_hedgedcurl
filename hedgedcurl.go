package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var timeout int
	flag.IntVar(&timeout, "t", 15, "timeout seconds")
	flag.IntVar(&timeout, "timeout", 15, "timeout seconds")
	flag.Parse()

	timeoutDuration := time.Duration(timeout) * time.Second

	args := flag.Args()

	var ctx context.Context
	var cancel context.CancelFunc

	ctx, cancel = context.WithCancel(context.Background())

	defer cancel()

	resultChan := make(chan string, 1)
	var wg sync.WaitGroup
	done := make(chan struct{})

	var timeoutValid int32
	if len(args) == 0 {
		fmt.Printf("Введите url")
		os.Exit(0)
	}
	for _, url := range args {
		wg.Add(1)
		go getRequestWithTimeout(url, ctx, resultChan, &wg, &timeoutValid, cancel, timeoutDuration)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case res := <-resultChan:
		fmt.Printf("Первый ответ: %s", res)
	case <-done:
		if atomic.LoadInt32(&timeoutValid) > 0 {
			fmt.Println("Таймауты по url^ам")
			os.Exit(228)
		} else {
			fmt.Println("Все запросы завершились с ошибками")
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func getRequestWithTimeout(url string, ctx context.Context, resCh chan<- string, wg *sync.WaitGroup,
	timeoutCounter *int32, ctxCancel context.CancelFunc, timeout time.Duration) {
	defer wg.Done()

	contextLocal, cancelLocal := context.WithTimeout(ctx, timeout)

	defer cancelLocal()

	req, err := http.NewRequestWithContext(contextLocal, "GET", url, nil)
	if err != nil {
		fmt.Printf("Неверный url: %s, err: %v \n", url, err)
		return
	}

	resp, err := http.DefaultClient.Do(req)

	if errors.Is(err, context.DeadlineExceeded) {
		atomic.AddInt32(timeoutCounter, 1)
		fmt.Printf("Таймаут при запросе url: %s, err: %v \n", url, err)
		return
	} else if err != nil {
		fmt.Printf("Ошибка при запросе url: %s, err: %v \n", url, err)
		return
	}
	defer resp.Body.Close()

	var headersBuilder strings.Builder
	for key, values := range resp.Header {
		for _, value := range values {
			headersBuilder.WriteString(fmt.Sprintf("%s: %s\n", key, value))
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Не смогли прочитать тело %s \n", url)
		resCh <- fmt.Sprintf("url: %s \n status code: %s %s \n header: %s \n", url, resp.Proto, resp.Status,
			headersBuilder)

		ctxCancel()
		return
	}

	resCh <- fmt.Sprintf("url: %s \n status code: %s %s \n header: %s \n body: %s ", url, resp.Proto, resp.Status,
		headersBuilder, string(body))

	ctxCancel()
	return
}
