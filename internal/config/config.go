package config

import (
	"io/ioutil"
	"os"
	
	"github.com/go-yaml/yaml"
	"github.com/pkg/errors"
)

type Config struct {
	Peers []Peer `yaml:"peers"`
	ListenAddr string `yaml:"listenAddr"`
}

type Peer struct {
	Addr string `yaml:"addr"`
}

func Load(filename string) (*Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Errorf("failed to open file: %s", filename)
	}
	var cfg Config
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(buf, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
