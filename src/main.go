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
	"strings"
	"syscall"
	"time"

	"yi/src/bus"
)

var version = "0.1.1"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "install":
			return runInstall(args[1:])
		case "uninstall":
			return runUninstall(args[1:])
		case "version":
			fmt.Println("yi", version)
			return nil
		case "help":
			printUsage()
			return nil
		default:
			return fmt.Errorf("unknown command %q (try \"yi help\")", args[0])
		}
	}
	return runServer(args)
}

func printUsage() {
	fmt.Printf(`usage:
  yi [flags]             启动服务
  yi install [flags]     安装 systemd 服务和配置
  yi uninstall [flags]   停止并移除 systemd 服务
  yi version             打印版本
  yi help                显示本帮助

服务参数: yi -h
安装参数: yi install -h
默认配置文件: %s
`, defaultConfigPath)
}

func runServer(args []string) error {
	cfg, err := parseServerArgs(args, defaultConfigPath)
	if err != nil {
		return err
	}
	if cfg.ShowVersion {
		fmt.Println("yi", version)
		return nil
	}

	s := cfg.Settings
	bus.SetArchiveBatch(s.ArchiveBatch)

	b, err := bus.Open(s.Data)
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serviceAddr := net.JoinHostPort(s.Host, strconv.Itoa(s.ServicePort))
	servers := []*http.Server{{
		Addr:              serviceAddr,
		Handler:           serviceHandler(b),
		ReadHeaderTimeout: 5 * time.Second,
	}}
	log.Printf("yi %s  service http://%s  data %s", version, serviceAddr, s.Data)
	if cfg.ConfigLoaded {
		log.Printf("config %s", cfg.ConfigPath)
	} else {
		log.Printf("config %s (missing, using defaults)", cfg.ConfigPath)
	}

	if s.Frontend {
		frontendAddr := net.JoinHostPort(s.Host, strconv.Itoa(s.FrontendPort))
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
	return nil
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
