package main

import (
	"fmt"
	"os"
	"redis/config"
	"redis/lib/logger"
	"redis/resp/handler"
	"redis/tcp"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "127.0.0.1",
	Port: 3000,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	//a := []byte("OK")
	//a := []byte{43, 79, 75, 13, 10}
	//fmt.Println(string(a))

	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, handler.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
