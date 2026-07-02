package sfu

import (
	"fmt"
	"log"

	"github.com/pion/webrtc/v3"
)

type SFUEngine struct {
	api *webrtc.API
}

func NewSFUEngine() *SFUEngine {
	// We configure default MediaEngine and InterceptorRegistry
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		log.Fatalf("Failed to register codecs: %v", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
	return &SFUEngine{api: api}
}

// ProcessOffer takes an SDP Offer string from a browser/peer, sets up event handlers, and returns an SDP Answer string
func (s *SFUEngine) ProcessOffer(offerSDP string) (string, error) {
	// 1. Create a new PeerConnection using public STUN servers for ICE discovery
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	peerConnection, err := s.api.NewPeerConnection(config)
	if err != nil {
		return "", fmt.Errorf("failed to create PeerConnection: %w", err)
	}

	// 2. Handle Data Channels created by the browser
	peerConnection.OnDataChannel(func(d *webrtc.DataChannel) {
		log.Printf("📡 New WebRTC Data Channel opened: %s (%d)", d.Label(), d.ID())

		// Register message handler on data channel
		d.OnMessage(func(msg webrtc.DataChannelMessage) {
			log.Printf("📩 Received on DataChannel '%s': %s", d.Label(), string(msg.Data))

			// Echo back to client
			echoReply := fmt.Sprintf("ECHO: %s", string(msg.Data))
			if err := d.SendText(echoReply); err != nil {
				log.Printf("Error echoing data: %v", err)
			}
		})
	})

	// 3. Handle Video/Audio Media Tracks (SFU Fan-Out Foundation)
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("🎥 New Incoming Media Track: Codec %s | PayloadType %d", track.Codec().MimeType, track.PayloadType())
		
		// In a full multi-room SFU, you read incoming RTP packets here and broadcast them to other peers:
		go func() {
			buf := make([]byte, 1500) // MTU size buffer
			for {
				n, _, err := track.Read(buf)
				if err != nil {
					return
				}
				// Packets inside buf[:n] are raw RTP packets ready to be forwarded!
				_ = n
			}
		}()
	})

	// 4. Set Remote Description (The browser's SDP Offer)
	offerDesc := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}
	if err := peerConnection.SetRemoteDescription(offerDesc); err != nil {
		return "", fmt.Errorf("SetRemoteDescription error: %w", err)
	}

	// 5. Create our Answer
	answerDesc, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("CreateAnswer error: %w", err)
	}

	// 6. Gather local ICE candidates & set Local Description
	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(answerDesc); err != nil {
		return "", fmt.Errorf("SetLocalDescription error: %w", err)
	}

	// Wait for ICE candidate gathering to finish so the SDP string contains network IP candidates
	<-gatherComplete

	return peerConnection.LocalDescription().SDP, nil
}
