package logging

// import (
// 	"log/slog"
// 	"os"
// )

// type Format int

// const (
// 	FormatText Format = iota
// 	FormatJSON
// )

// type Options struct {
// 	Level     slog.Leveler
// 	Format    Format
// 	AddSource bool
// }

// func Init(opts Options) {
// 	level := opts.Level
// 	if level == nil {
// 		level = slog.LevelInfo
// 	}
// 	handlerOptions := &slog.HandlerOptions{
// 		Level:     level,
// 		AddSource: opts.AddSource,
// 	}

// 	var handler slog.Handler
// 	switch opts.Format {
// 	case FormatJSON:
// 		handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
// 	default:
// 		handler = slog.NewTextHandler(os.Stdout, handlerOptions)
// 	}

// 	slog.SetDefault(slog.New(handler))
// }

// func InitDefault() {
// 	Init(Options{
// 		Level:     slog.LevelInfo,
// 		Format:    FormatText,
// 		AddSource: false,
// 	})
// }
