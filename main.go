package main

import (
	"log"
	"net/http"

	"github.com/Robinaditya1045/go-realtime-mastery/client"
	"github.com/Robinaditya1045/go-realtime-mastery/hub"
	"github.com/Robinaditya1045/go-realtime-mastery/sfu"
)

func main() {
	log.Println("🚀 Booting Go Real-Time WebSocket + Pion WebRTC SFU...")

	// Initialize Central Hub & SFU Engine
	h := hub.NewHub()
	go h.Run()

	sfuEngine := sfu.NewSFUEngine()

	// WebSocket Endpoint
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		client.ServeWs(h, sfuEngine, w, r)
	})

	// Embedded Real-Time Test Suite HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		html := `<!DOCTYPE html>
<html>
<head><title>Go Pion WebRTC Mastery</title></head>
<body style="font-family: Arial, sans-serif; padding: 20px; max-width: 800px; margin: auto;">
  <h2>Go Real-Time Engine (WebSocket + Pion WebRTC)</h2>
  
  <div style="display:flex; gap: 20px;">
    <!-- WebSocket Panel -->
    <div style="flex:1; border:1px solid #ddd; padding: 15px; border-radius: 8px;">
      <h3>1. WebSocket Chat Hub</h3>
      <div id="ws-logs" style="background:#f9f9f9; height:200px; overflow-y:scroll; border:1px solid #eee; padding:8px; font-size:13px; margin-bottom:10px;"></div>
      <input id="ws-input" type="text" placeholder="Type chat..." style="width:65%; padding:6px;" />
      <button onclick="sendWS()" style="padding:6px 12px;">Broadcast</button>
    </div>

    <!-- WebRTC Panel -->
    <div style="flex:1; border:1px solid #ddd; padding: 15px; border-radius: 8px; background:#f0f8ff;">
      <h3>2. WebRTC UDP Data Channel</h3>
      <button id="rtc-btn" onclick="connectWebRTC()" style="padding:10px 18px; background:#0066cc; color:white; border:none; border-radius:4px; cursor:pointer; font-weight:bold;">Connect WebRTC to Go SFU</button>
      <div id="rtc-logs" style="background:#fff; height:155px; overflow-y:scroll; border:1px solid #ccc; padding:8px; font-size:13px; margin-top:10px; margin-bottom:10px;"></div>
      <input id="rtc-input" type="text" placeholder="Send UDP message to Go..." style="width:65%; padding:6px;" disabled />
      <button id="rtc-send-btn" onclick="sendRTC()" style="padding:6px 12px;" disabled>Send UDP</button>
    </div>
  </div>

  <script>
    const ws = new WebSocket("ws://" + window.location.host + "/ws");
    const wsLogs = document.getElementById("ws-logs");
    const rtcLogs = document.getElementById("rtc-logs");
    let pc = null;
    let dc = null;

    function logWS(msg) { wsLogs.innerHTML += "<div>" + msg + "</div>"; wsLogs.scrollTop = wsLogs.scrollHeight; }
    function logRTC(msg) { rtcLogs.innerHTML += "<div><b>" + msg + "</b></div>"; rtcLogs.scrollTop = rtcLogs.scrollHeight; }

    ws.onopen = () => logWS("✅ WebSocket connected");
    ws.onmessage = (event) => {
      try {
        const sig = JSON.parse(event.data);
        if (sig.type === "webrtc_answer") {
          logRTC("📥 Received WebRTC SDP Answer from Go SFU over WebSocket!");
          pc.setRemoteDescription({ type: "answer", sdp: sig.data });
          return;
        }
      } catch(e) {}
      logWS("💬 Broadcast: " + event.data);
    };

    function sendWS() {
      const inp = document.getElementById("ws-input");
      if (inp.value) { ws.send(inp.value); inp.value = ""; }
    }

    async function connectWebRTC() {
      document.getElementById("rtc-btn").disabled = true;
      logRTC("⚙️ Creating RTCPeerConnection in browser...");
      pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });

      // Create WebRTC Data Channel
      dc = pc.createDataChannel("chat-channel");
      dc.onopen = () => {
        logRTC("🚀 WebRTC Data Channel OPEN! Direct UDP connection active!");
        document.getElementById("rtc-input").disabled = false;
        document.getElementById("rtc-send-btn").disabled = false;
      };
      dc.onmessage = (e) => logRTC("⚡ UDP Echo from Go: " + e.data);

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      // Wait 1 sec for ICE gathering, then send SDP Offer over WebSocket signaling
      setTimeout(() => {
        logRTC("📤 Sending SDP Offer to Go server via WebSocket...");
        ws.send(JSON.stringify({ type: "webrtc_offer", data: pc.localDescription.sdp }));
      }, 1000);
    }

    function sendRTC() {
      const inp = document.getElementById("rtc-input");
      if (inp.value && dc) {
        logRTC("Out: " + inp.value);
        dc.send(inp.value);
        inp.value = "";
      }
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
