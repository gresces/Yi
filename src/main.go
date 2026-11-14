package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"yi/src/bus"
)

var version = "0.1.0"

func main() {
	showVersion := flag.Bool("version", false, "打印版本并退出")
	host := flag.String("host", "127.0.0.1", "监听地址")
	servicePort := flag.Int("service-port", 8081, "服务端口")
	frontendPort := flag.Int("frontend-port", 8080, "前端端口")
	withFrontend := flag.Bool("frontend", true, "是否启动前端")
	dataDir := flag.String("data", "data", "数据目录")
	archiveEvery := flag.Int64("archive-batch", 256, "归档批量;0 表示禁用归档")
	flag.Parse()

	if *showVersion {
		fmt.Println("yi", version)
		return
	}

	bus.SetArchiveBatch(*archiveEvery)

	b, err := bus.Open(*dataDir)
	if err != nil {
		log.Fatalf("open bus: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serviceAddr := net.JoinHostPort(*host, strconv.Itoa(*servicePort))
	servers := []*http.Server{{
		Addr:              serviceAddr,
		Handler:           serviceHandler(b),
		ReadHeaderTimeout: 5 * time.Second,
	}}
	log.Printf("yi %s  service http://%s  data %s", version, serviceAddr, *dataDir)

	if *withFrontend {
		frontendAddr := net.JoinHostPort(*host, strconv.Itoa(*frontendPort))
		servers = append(servers, &http.Server{
			Addr:              frontendAddr,
			Handler:           frontendHandler(b),
			ReadHeaderTimeout: 5 * time.Second,
		})
		log.Printf("frontend http://%s", frontendAddr)
	}

	errs := make(chan error, len(servers))
	for _, srv := range servers {
		go func(srv *http.Server) {
			errs <- srv.ListenAndServe()
		}(srv)
	}

	select {
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	if err := b.Close(); err != nil {
		log.Printf("bus close: %v", err)
	}
	log.Println("bye")
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
