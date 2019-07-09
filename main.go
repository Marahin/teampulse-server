package main

import (
	"flag"
	"net/http"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

var hubs []*Hub
var hubLock = &sync.Mutex{}

func main() {
	flag.Parse()
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		var hub *Hub
		roomNames, _ := r.URL.Query()["room"]
		nickNames, _ := r.URL.Query()["name"]
		room := roomNames[0]
		name := nickNames[0]

		hubLock.Lock()
		served := false
		for _, foundHub := range hubs {
			if foundHub.Name == room {
				served = true

				hub = foundHub
			}
		}

		if !served {
			served = true
			newHub := newHub(room)
			hubs = append(hubs, newHub)

			hub = newHub
		}
		hubLock.Unlock()
		if hub != nil && hub.state != "running" {
			go hub.run()
		}

		log.WithFields(log.Fields{"room": room, "name": name}).Info("Serving Websocket")
		serveWs(hub, name, w, r)
	})
	log.Info("Started server")
	err := http.ListenAndServe(":" + os.Getenv("PORT"), nil)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("ListenAndServe")
	}

}