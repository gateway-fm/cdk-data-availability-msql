package config

import (
	"bytes"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// DefaultValues is the default configuration
const DefaultValues = `
PrivateKey = {Path = "/pk/test-member.keystore", Password = "testonly"}

[L1]
WsURL = "ws://localhost:8546"
Contract = "0x0DCd1Bf9A1b36cE34237eEaFef220932846BCD82"
Timeout = "1m"
RetryPeriod = "5s"

[Log]
Environment = "development" # "production" or "development"
Level = "info"
Outputs = ["stderr"]

[DB]
User = "committee_user"
Password = "committee_password"
Name = "committee_db"
Host = "supernets2.0-data-availability-db"
Port = "5432"
EnableLog = false
MaxConns = 200

[RPC]
Host = "0.0.0.0"
Port = 8444
ReadTimeout = "60s"
WriteTimeout = "60s"
MaxRequestsPerIPAndSecond = 500
SequencerNodeURI = ""
EnableL2SuggestedGasPricePolling = false
	[RPC.WebSockets]
		Enabled = false
`

// Default parses the default configuration values.
func Default() (*Config, error) {
	var cfg Config
	viper.SetConfigType("toml")

	err := viper.ReadConfig(bytes.NewBuffer([]byte(DefaultValues)))
	if err != nil {
		return nil, err
	}
	err = viper.Unmarshal(&cfg, viper.DecodeHook(mapstructure.TextUnmarshallerHookFunc()))
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
