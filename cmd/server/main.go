package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kavirajk/kv/internal/config"
	"github.com/kavirajk/kv/internal/gossip"
)

func main() {
	var (
		cfgFile string
	)

	flag.StringVar(&cfgFile, "config", "./config.yaml", "addr for UDP server to listen to")
	flag.Parse()

	if cfgFile == "" {
		panic("please provide -config")
	}

	cfg, err := config.Load(cfgFile)
	if err != nil {
		panic("config load failed")
	}

	fmt.Printf("%+v\n", cfg)

	server := gossip.New(cfg.ListenAddr, 200*time.Millisecond, cfg.Peers)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go server.Loop(ctx)
	go server.ListenLoop(ctx)

	fmt.Println("UDP server listening on: ", cfg.ListenAddr)

	log.Fatal(http.ListenAndServe(cfg.HTTPListen, server))

	// go server.Loop(ctx)

	// for _, peer := range cfg.Peers {
	// 	if err := server.SendPeer(peer.Addr, "PING"); err != nil {
	// 		panic(err)
	// 	}
	// }

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)

	select {
	case sg := <-sig:
		fmt.Printf("Signal received: %s. stopping server\n", sg)
		cancel()
	case err := <-ctx.Done():
		fmt.Printf("context canceled: %q\n", err)
	}
}
