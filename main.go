package main

import (
	"log"
	"net/http"

	"github.com/Robinaditya1045/go-realtime-mastery/client"
	"github.com/Robinaditya1045/go-realtime-mastery/hub"
)

func main() {
	log.Println("🚀 Starting Go Real-Time Signaling Server...")

	// Create and start the central Hub goroutine
	h := hub.NewHub()
	go h.Run()

	// WebSocket endpoint
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		client.ServeWs(h, w, r)
	})

	// Simple embedded HTML test client
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		html := `<!DOCTYPE html>
<html>
<head><title>Go Real-Time WebSocket</title></head>
<body style="font-family: Arial; padding: 20px;">
  <h2>Go Real-Time Signaling & Chat Hub</h2>
  <div id="messages" style="border:1px solid #ccc; height:300px; overflow-y:scroll; padding:10px; margin-bottom:10px;"></div>
  <input id="input" type="text" placeholder="Type a message or SDP signal..." style="width:70%; padding:8px;" />
  <button onclick="send()" style="padding:8px 16px;">Send</button>
  <script>
    const ws = new WebSocket("ws://" + window.location.host + "/ws");
    const messages = document.getElementById("messages");
    ws.onmessage = function(event) {
      const p = document.createElement("p");
      p.innerText = "Received: " + event.data;
      messages.appendChild(p);
      messages.scrollTop = messages.scrollHeight;
    };
    function send() {
      const input = document.getElementById("input");
      if (input.value) { ws.send(input.value); input.value = ""; }
    }
  </script>
</body>
</html>`
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html))
	})

	log.Println("🌐 Real-Time Server listening on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
