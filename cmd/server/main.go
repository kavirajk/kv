package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kavirajk/kv/internal/cluster"
	"github.com/kavirajk/kv/internal/config"
	kvhttp "github.com/kavirajk/kv/internal/http"
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

	logger := log.NewJSONLogger(os.Stdout)

	peer, err := cluster.NewPeer(log.With(logger, "component", "peer"), cfg.ListenAddr, cfg.Peers)
	if err != nil {
		panic("failed to create cluster")
	}

	go func() {
		for {
			fmt.Println(peer.Members())
			time.Sleep(3 * time.Second)
		}
	}()

	server := kvhttp.NewServer(peer, log.With(logger, "component", "server"))

	// ctx, cancel := context.WithCancel(context.Background())

	// go server.Loop(ctx)
	// go server.ListenLoop(ctx)

	fmt.Println("HTTP server listening on: ", cfg.HTTPListen)

	http.ListenAndServe(cfg.HTTPListen, server)

	// go server.Loop(ctx)

	// for _, peer := range cfg.Peers {
	// 	if err := server.SendPeer(peer.Addr, "PING"); err != nil {
	// 		panic(err)
	// 	}
	// }

	// sig := make(chan os.Signal)
	// signal.Notify(sig, syscall.SIGINT)

	// select {
	// case sg := <-sig:
	// 	fmt.Printf("Signal received: %s. stopping server\n", sg)
	// 	cancel()
	// case err := <-ctx.Done():
	// 	fmt.Printf("context canceled: %q\n", err)
	// }
}
