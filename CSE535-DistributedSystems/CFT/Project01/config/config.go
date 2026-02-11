package config

import (
	"log/slog"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var current atomic.Value // stores *Config

// get the package directory of config.go irrespective of where we run the program from
func packageDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Dir(file)
}

// Load reads config from a file path, applies defaults, and (optionally) hot-reloads.
// If `path` is empty, it searches for "config.(yaml|yml|json|toml)" in:
//
//	packageDir(config.go), . , ./config , /etc/myapp
func Load(enableHotReload bool) (*Config, error) {
	v := viper.New()

	// Defaults
	// v.SetDefault("app.name", "myapp")
	// v.SetDefault("app.env", "dev")
	// v.SetDefault("app.port", 8080)
	// v.SetDefault("app.log_level", "info")
	// v.SetDefault("http.read_timeout_sec", 10)
	// v.SetDefault("http.write_timeout_sec", 15)
	// v.SetDefault("http.idle_timeout_sec", 60)

	// File location
	v.SetConfigName("config")
	if pkgDir := packageDir(); pkgDir != "" {
		v.AddConfigPath(pkgDir)
	}

	// Read file if present; keep going with defaults if not found
	if err := v.ReadInConfig(); err != nil {
		// Otherwise: we rely on defaults only.
		slog.Warn("config file not found; using defaults", slog.String("path", v.ConfigFileUsed()))
	}

	cfg, err := unmarshal(v)
	if err != nil {
		return nil, err
	}
	current.Store(cfg)

	// Hot reload when the file changes (only if a real file is loaded)
	if enableHotReload && v.ConfigFileUsed() != "" {
		v.WatchConfig()
		v.OnConfigChange(func(_ fsnotify.Event) {
			newCfg, err := unmarshal(v)
			if err != nil {
				slog.Error("config reload failed", slog.Any("error", err))
				return
			}
			current.Store(newCfg)
			slog.Info("config reloaded", slog.String("path", v.ConfigFileUsed()))
		})
	}

	return cfg, nil
}

func unmarshal(v *viper.Viper) (*Config, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Get returns the latest snapshot. Do not mutate it.
func Get() *Config {
	if c := current.Load(); c != nil {
		return c.(*Config)
	}
	return &Config{} // or panic if you prefer enforcing Load() first
}
