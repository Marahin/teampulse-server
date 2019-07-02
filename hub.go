package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const pingCalculationTime = 500 * time.Millisecond

type Hub struct {
	Name string `json:"name"`

	// Registered clients.
	clients map[*Client]bool

	// Inbound messages from the clients.
	broadcast chan []byte

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	state string
}

func newHub(name string) *Hub {
	return &Hub{
		Name: name,
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) run() {
	defer func(h *Hub) { h.state = "stopped" }(h)
	h.state = "running"

	go func(h *Hub) { // PING calculation loop
		for {
			pingMessage := Message{Content: strconv.Itoa(int(time.Now().UnixNano()/1e6)), Type: "ping", Hub: h}
			for client := range h.clients {
				data, err := json.Marshal(pingMessage)
				if err != nil {
					log.WithFields(log.Fields{"err": err}).Error("Ping message marshalling failed")
				}
				client.send <- data
			}

			time.Sleep(pingCalculationTime)
		}

	}(h)

	for {
		select {
		case client := <-h.register:
			log.WithFields(log.Fields{"hub": h, "client": client}).Info("Registered new client")

			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				log.WithFields(log.Fields{"hub": h, "client": client}).Info("Unregistered client")

				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}