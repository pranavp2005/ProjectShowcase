package main

type Transaction struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

type Set struct {
	Number       int           `json:"setNumber"`
	Transactions []Transaction `json:"transactions"`
	LiveNodes    []string      `json:"liveNodes"`
}