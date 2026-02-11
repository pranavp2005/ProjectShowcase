package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var PATH = filepath.Join(".", "server", "persistence", "logs")

const timestampLen = len("2006-01-02T15:04:05.999999999Z07:00")

func ensureLogFile(filePath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	return f, nil
}

// AppendString appends the provided string to the file at path.
func AppendString(filePath, s string) error {
	f, err := ensureLogFile(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	needsNewline, err := needsTrailingNewline(f)
	if err != nil {
		return err
	}

	normalized := strings.ReplaceAll(s, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "")
	if normalized == "" {
		return nil
	}

	lines := strings.Split(normalized, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return nil
	}

	var builder strings.Builder
	estimated := len(normalized) + len(lines)*(timestampLen+2)
	if needsNewline {
		estimated++
	}
	builder.Grow(estimated)

	if needsNewline {
		builder.WriteByte('\n')
	}
	for _, line := range lines {
		timestamp := time.Now().UTC().Format(time.RFC3339Nano)
		builder.WriteString(timestamp)
		builder.WriteByte(' ')
		builder.WriteString(line)
		builder.WriteByte('\n')
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	if _, err := f.WriteString(builder.String()); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func needsTrailingNewline(f *os.File) (bool, error) {
	info, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("stat file: %w", err)
	}
	size := info.Size()
	if size == 0 {
		return false, nil
	}

	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, size-1); err != nil {
		return false, fmt.Errorf("inspect file: %w", err)
	}
	return buf[0] != '\n', nil
}
