package main

type Message struct {
	Client *Client `json:"client"`
	Hub *Hub `json:"hub"`
	Type string `json:"type"`
	Content string `json:"content"`
	Created int64 `json:"created"`
}