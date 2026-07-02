package client

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/Robinaditya1045/go-realtime-mastery/hub"
	"github.com/Robinaditya1045/go-realtime-mastery/sfu"
	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 65536 // Increased size to allow large WebRTC SDP strings
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// SignalMessage defines the JSON structure sent between browser and Go server
type SignalMessage struct {
	Type string `json:"type"` // "chat" or "webrtc_offer" or "webrtc_answer"
	Data string `json:"data"` // Chat message text or raw SDP string
}

type Client struct {
	hub  *hub.Hub
	sfu  *sfu.SFUEngine
	conn *websocket.Conn
	send chan []byte
}

func (c *Client) SendChannel() chan []byte {
	return c.send
}

func (c *Client) readPump() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Try to parse as a JSON SignalMessage
		var sig SignalMessage
		if err := json.Unmarshal(messageBytes, &sig); err == nil && sig.Type == "webrtc_offer" {
			log.Println("📡 Intercepted WebRTC Offer from browser over WebSocket!")
			
			// Process offer using our Pion SFU Engine
			answerSDP, err := c.sfu.ProcessOffer(sig.Data)
			if err != nil {
				log.Printf("SFU ProcessOffer error: %v", err)
				continue
			}

			// Send back WebRTC Answer over this client's send channel
			respSig := SignalMessage{Type: "webrtc_answer", Data: answerSDP}
			respBytes, _ := json.Marshal(respSig)
			c.send <- respBytes
			continue
		}

		// Otherwise treat as normal chat message and broadcast to all tabs
		c.hub.Broadcast(messageBytes)
	}
}

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
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
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

// ServeWs handles websocket requests and injects the SFU engine
func ServeWs(h *hub.Hub, sfuEngine *sfu.SFUEngine, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	client := &Client{hub: h, sfu: sfuEngine, conn: conn, send: make(chan []byte, 256)}
	client.hub.Register(client)

	go client.writePump()
	go client.readPump()
}
