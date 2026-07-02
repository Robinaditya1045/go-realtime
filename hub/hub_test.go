package hub_test

import (
	"testing"
	"time"

	"github.com/Robinaditya1045/go-realtime-mastery/hub"
)

// DummyClient simulates a websocket connection using a Go channel
type DummyClient struct {
	send chan []byte
}

func (d *DummyClient) SendChannel() chan []byte {
	return d.send
}

func TestHub_RegisterAndBroadcast(t *testing.T) {
	h := hub.NewHub()

	// Start the Hub loop in a background Goroutine
	go h.Run()

	// 1. Create two mock connected clients
	client1 := &DummyClient{send: make(chan []byte, 10)}
	client2 := &DummyClient{send: make(chan []byte, 10)}

	// 2. Register both clients
	h.Register(client1)
	h.Register(client2)

	// Allow goroutines a tiny moment to process channel messages
	time.Sleep(10 * time.Millisecond)

	if h.ClientCount() != 2 {
		t.Fatalf("Expected 2 registered clients, got %d", h.ClientCount())
	}

	// 3. Broadcast a message through the Hub
	testMessage := []byte("Hello WebRTC Peers!")
	h.Broadcast(testMessage)

	time.Sleep(10 * time.Millisecond)

	// 4. Verify both clients received the message on their send channels
	select {
	case msg := <-client1.send:
		if string(msg) != string(testMessage) {
			t.Errorf("Client 1 got %s, expected %s", msg, testMessage)
		}
	default:
		t.Errorf("Client 1 did not receive the broadcast message")
	}

	select {
	case msg := <-client2.send:
		if string(msg) != string(testMessage) {
			t.Errorf("Client 2 got %s, expected %s", msg, testMessage)
		}
	default:
		t.Errorf("Client 2 did not receive the broadcast message")
	}

	// 5. Unregister Client 1
	h.Unregister(client1)
	time.Sleep(10 * time.Millisecond)

	if h.ClientCount() != 1 {
		t.Errorf("Expected 1 client remaining after unregister, got %d", h.ClientCount())
	}
}
