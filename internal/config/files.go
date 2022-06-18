package config

import (
	"log"
	"os"
	"path/filepath"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	ClientCertFile       = configFile("client.pem")
	ClientKeyFile        = configFile("client-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

func configFile(filename string) string {
	if configDir := os.Getenv("CONFIG_DIR"); configDir != "" {
		return filepath.Join(configDir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to home dir %s", err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}
