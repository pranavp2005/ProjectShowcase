//filename const.go
package common

type NodeRole string
const (
 RoleLeader NodeRole = "leader"
 RoleBackup NodeRole = "backup"
)


type NodeStatus string

const (
 StatusActive NodeStatus = "active"
 StatusStopped NodeStatus = "stopped"
)

type SequenceStatus string 

const (
	StatusAccepted SequenceStatus = "A"
	StatusCommitted SequenceStatus = "C"
	StatusExecuted SequenceStatus = "E"
	StatusNoStatus SequenceStatus = "X" //Used to represent a gap in log
)