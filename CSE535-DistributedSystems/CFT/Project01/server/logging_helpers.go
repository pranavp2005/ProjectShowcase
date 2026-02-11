package main

import (
	"fmt"
	"log/slog"

	"pranavpateriya.com/multipaxos/logging"
)

func LogProtocol(nodeID int, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	filename := fmt.Sprintf("/Users/pranavpateriya/Downloads/Fall 2025/Acads/CSE535-DistributedSystems/Projects/cft-pranavp2005/Project01/server/persistence/logs/log%d.log", nodeID)
	if err := logging.AppendString(filename, message); err != nil {
		slog.Error("protocol logging failed", slog.Int("node", nodeID), slog.Any("error", err))
	}
}
