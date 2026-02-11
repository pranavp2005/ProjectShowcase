package config

type Config struct {
	App     AppConfig     `mapstructure:"app"`
	Client  ClientConfig  `mapstructure:"client"`
	Cluster ClusterConfig `mapstructure:"cluster"`
	Nodes   []NodeConfig  `mapstructure:"nodes"`
}

type AppConfig struct {
	Name     string `mapstructure:"name"`
	Env      string `mapstructure:"env"`
	LogLevel string `mapstructure:"log_level"`
}

type ClientConfig struct {
	RPCTargets       []string    `mapstructure:"rpc_targets"`
	RequestTimeoutMS int         `mapstructure:"request_timeout_ms"`
	Retry            RetryPolicy `mapstructure:"retry"`
}

type RetryPolicy struct {
	MaxRetries        int     `mapstructure:"max_retries"`
	InitialBackoffMS  int     `mapstructure:"initial_backoff_ms"`
	MaxBackoffMS      int     `mapstructure:"max_backoff_ms"`
	BackoffMultiplier float64 `mapstructure:"backoff_multiplier"`
	JitterPct         int     `mapstructure:"jitter_pct"`
}

type ClusterConfig struct {
	InterNode InterNodeConfig `mapstructure:"inter_node"`
}

type InterNodeConfig struct {
	DialTimeoutMS       int `mapstructure:"dial_timeout_ms"`
	RPCTimeoutMS        int `mapstructure:"rpc_timeout_ms"`
	HeartbeatIntervalMS int `mapstructure:"heartbeat_interval_ms"`
	MaxInflight         int `mapstructure:"max_inflight"`
}

type NodeConfig struct {
	ID            string       `mapstructure:"id"`
	RPCListenAddr string       `mapstructure:"rpc_listen_addr"`
	DB            NodeDBConfig `mapstructure:"db"`
}

type NodeDBConfig struct {
	InitialStatus      string `mapstructure:"intial_status"`
	Driver             string `mapstructure:"driver"`
	Host               string `mapstructure:"host"`
	Port               int    `mapstructure:"port"`
	User               string `mapstructure:"user"`
	Password           string `mapstructure:"password"`
	Name               string `mapstructure:"name"`
	SSLMode            string `mapstructure:"sslmode"`
	MaxOpenConns       int    `mapstructure:"max_open_conns"`
	MaxIdleConns       int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeMin int    `mapstructure:"conn_max_lifetime_min"`
	ConnTimeoutMS      int    `mapstructure:"conn_timeout_ms"`
}
