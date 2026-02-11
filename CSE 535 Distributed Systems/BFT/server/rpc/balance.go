package server

func (node *Node) State() map[string]int {
	snapshot := node.Bank.Snapshot()
	state := make(map[string]int, len(snapshot))
	for account, balance := range snapshot {
		state[string(account)] = int(balance)
	}
	return state
}
