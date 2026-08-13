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
	timeoutSF := flag.Int("t", 0, "timeout seconds")
	timeoutLF := flag.Int("timeout", 0, "timeout seconds")

	flag.Parse()

	timeout := time.Duration(*timeoutSF)
	if timeout == 0 {
		timeout = time.Duration(*timeoutLF)
	}

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
		go getRequestWithTimeout(url, ctx, resultChan, &wg, &timeoutValid, cancel, timeout)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case res := <-resultChan:
		fmt.Printf("Первый ответ: %s", res)
	case <-done:
		if timeoutValid > 0 {
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

	var contextLocal context.Context
	if timeout == 0 {
		contextLocal = ctx
	} else {
		contextLocal, _ = context.WithTimeout(ctx, timeout)
	}

	req, err := http.NewRequestWithContext(contextLocal, "GET", url, nil)
	if err != nil {
		ctxCancel()
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

	if http.StatusOK != resp.StatusCode {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			fmt.Printf("Не смогли прочитать тело %s \n", url)
			return
		}
		fmt.Printf("URL: %s, статус: %d \nТело ответа:\n%s \n", url, resp.StatusCode, string(body))
		return
	}

	var headersBuilder strings.Builder
	for key, values := range resp.Header {
		for _, value := range values {
			headersBuilder.WriteString(fmt.Sprintf("%s: %s\n", key, value))
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Не смогли прочитать тело %s \n", url)
	}

	resCh <- fmt.Sprintf("url: %s \n status code: %s %s \n header: %s \n body: %s ", url, resp.Proto, resp.Status,
		headersBuilder, string(body))
	ctxCancel()

	return
}
