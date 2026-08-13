package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	timeoutSF := flag.Duration("t", 0, "timeout seconds")
	timeoutLF := flag.Duration("timeout", 0, "timeout seconds")

	flag.Parse()

	timeout := *timeoutSF
	if timeout.Seconds() == 0 {
		timeout = *timeoutLF
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

	header := resp.Header.Get("Content-Type")

	resCh <- fmt.Sprintf("url: %s \n status code: %d \n header: %s \n ", url, http.StatusOK, header)
	ctxCancel()

	return
}
