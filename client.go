package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 2 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 2 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = 500 * time.Millisecond

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	Name string `json:"name"`

	Hub *Hub `json:"hub"`

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
		disconnectMessage := Message{
			Type: "unregister", Content: fmt.Sprintf("%s has disconnected", c.Name),
			Hub: c.Hub, Created: time.Now().UnixNano() /1e6, Client: c,
		}
		data, err := json.Marshal(&disconnectMessage)
		if err != nil {
			log.WithFields(log.Fields{"msg": disconnectMessage, "err": err}).Error("Disconnection Message marshalling failed")
		}
		c.Hub.broadcast <- data
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Warn("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))
		msg := &Message{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			log.WithFields(log.Fields{"message": string(message), "err": err}).Error("Unmarshalling message onto Message struct failed")
		}
		msg.Client = c
		msg.Created = time.Now().UnixNano() /1e6
		msg.Hub = c.Hub

		switch msg.Type {
		case "pong":
			msg.Type = "ping-diff"
			receivedTimestamp, _ := strconv.Atoi(msg.Content)
			msg.Content = strconv.Itoa(int(time.Now().UnixNano()/1e6 - int64(receivedTimestamp)))
		default: //Do nothing
		}

		data, err := json.Marshal(msg)
		if err != nil {
			log.WithFields(log.Fields{"msg": msg, "err": err}).Error("Marshalling message failed")
		}

		c.Hub.broadcast <- data
	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// serveWs handles websocket requests from the peer.
func serveWs(hub *Hub, nickname string, w http.ResponseWriter, r *http.Request) {
	log.WithFields(log.Fields{"host": r.Host}).Info("Requesting WS connection")

	upgrader.CheckOrigin = func(r *http.Request) bool {
		origin := r.Header.Get("Origin")

		if os.Getenv("DEVELOPMENT") == "true" { return true }
		if origin == "https://teampulse-frontend.herokuapp.com" { return true }

		return false
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &Client{Name: nickname, Hub: hub, conn: conn, send: make(chan []byte, 256)}
	client.Hub.register <- client

	connectionMessage := Message{
		Type: "register", Content: fmt.Sprintf("%s has connected", client.Name),
		Hub: hub, Created: time.Now().UnixNano() /1e6, Client: client,
	}
	data, err := json.Marshal(&connectionMessage)
	if err != nil {
		log.WithFields(log.Fields{"msg": connectionMessage, "err": err}).Error("Connection Message marshalling failed")
	}
	client.Hub.broadcast <- data

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}