package hub

import "sync/atomic"

// ClientInterface allows us to decouple the Hub from gorilla/websocket for easy testing
type ClientInterface interface {
	SendChannel() chan []byte
}

type Hub struct {
	// Registered clients map
	clients map[ClientInterface]bool

	// Channels for Goroutine synchronization
	broadcast  chan []byte
	register   chan ClientInterface
	unregister chan ClientInterface

	// Atomic counter to track connected clients safely across threads
	count int32
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[ClientInterface]bool),
		broadcast:  make(chan []byte),
		register:   make(chan ClientInterface),
		unregister: make(chan ClientInterface),
	}
}

// Register sends a client to the registration channel
func (h *Hub) Register(c ClientInterface) {
	h.register <- c
}

// Unregister sends a client to the unregistration channel
func (h *Hub) Unregister(c ClientInterface) {
	h.unregister <- c
}

// Broadcast pushes a raw message to all connected clients
func (h *Hub) Broadcast(msg []byte) {
	h.broadcast <- msg
}

// ClientCount safely returns number of active connections
func (h *Hub) ClientCount() int {
	return int(atomic.LoadInt32(&h.count))
}

// Run executes inside a dedicated Goroutine to process channel events sequentially
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			atomic.AddInt32(&h.count, 1)

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.SendChannel())
				atomic.AddInt32(&h.count, -1)
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.SendChannel() <- message:
					// Message dispatched successfully
				default:
					// Client buffer is full or slow; disconnect them to prevent lag
					close(client.SendChannel())
					delete(h.clients, client)
					atomic.AddInt32(&h.count, -1)
				}
			}
		}
	}
}
