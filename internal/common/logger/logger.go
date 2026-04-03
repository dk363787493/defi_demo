package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New(level string, format string) zerolog.Logger {
	var w io.Writer = os.Stdout
	if format == "console" {
		w = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	return zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Caller().
		Logger()
}
