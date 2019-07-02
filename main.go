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

func serveHome(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL)
	if r.URL.Path != "/" {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.ServeFile(w, r, "main.html")
}

func main() {
	flag.Parse()
	http.HandleFunc("/", serveHome)
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