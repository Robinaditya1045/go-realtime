package sfu_test

import (
	"context"
	"testing"
	"time"

	"github.com/Robinaditya1045/go-realtime-mastery/sfu"
	"github.com/pion/webrtc/v3"
)

func TestPionDataChannelEcho(t *testing.T) {
	// 1. Create our SFU Engine
	engine := sfu.NewSFUEngine()

	// 2. Create a Mock Client PeerConnection
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("Failed to create client peer connection: %v", err)
	}
	defer clientPC.Close()

	// 3. Create a WebRTC Data Channel on the client side
	dc, err := clientPC.CreateDataChannel("chat-channel", nil)
	if err != nil {
		t.Fatalf("Failed to create data channel: %v", err)
	}

	// Channel to verify we receive an echo back from Go SFU
	echoReceived := make(chan string, 1)
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		echoReceived <- string(msg.Data)
	})

	// 4. Create an SDP Offer from the client
	offer, err := clientPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer failed: %v", err)
	}
	if err = clientPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription failed: %v", err)
	}

	// 5. Pass the Offer to our Go SFU Engine to get an Answer!
	answer, err := engine.ProcessOffer(offer.SDP)
	if err != nil {
		t.Fatalf("SFU failed to process offer: %v", err)
	}

	// 6. Set remote description on client side to complete handshake
	answerDesc := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	}
	if err = clientPC.SetRemoteDescription(answerDesc); err != nil {
		t.Fatalf("SetRemoteDescription failed: %v", err)
	}

	// 7. Wait for Data Channel to open and send a test message
	openCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dc.OnOpen(func() {
		dc.SendText("Hello Pion WebRTC!")
	})

	select {
	case reply := <-echoReceived:
		if reply != "ECHO: Hello Pion WebRTC!" {
			t.Errorf("Expected echo message, got: %s", reply)
		}
	case <-openCtx.Done():
		t.Fatalf("Timed out waiting for WebRTC Data Channel echo response")
	}
}
