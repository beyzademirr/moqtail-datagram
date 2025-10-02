# MOQT V9 End-to-End Demo

A complete demonstration of the MOQT V9 DatagramHandler with Draft-11 compliance, featuring real browser validation via WebTransport.


## **Quick Start**

### 1. Start Web Server
```bash
cd demo2/web
python -m http.server 8080
```

### 2. Start MOQT Server  
```bash
cd demo2/server
cargo run --bin moqt-demo-server
```

### 3. Open Browser
Navigate to: http://localhost:8080

Click "Connect to WebTransport" to see the live demo


## **Expected Demo Output**

### Server Console (Working Example):
```
🚀 MOQT V9 End-to-End Demo Server
🔐 Loading mkcert certificates...
✅ Certificates loaded successfully
📡 Server listening on https://localhost:4433 (WebTransport)
🔌 New WebTransport request from: [::1]:62364
✅ WebTransport connection established
📡 Testing datagrams...
✅ Test datagram sent successfully
🔧 Setting up MOQT V9 handler...
🔧 V9 Handler initialized
📺 Track 42 activated for slideshow
🎬 Starting slideshow with 5 frames
📤 Sent frame 1 (size: 113 bytes, subgroup: Some(1))
📤 Sent frame 2 (size: 113 bytes, subgroup: Some(2))
📤 Sent frame 3 (size: 113 bytes, subgroup: Some(2))
📤 Sent frame 4 (size: 113 bytes, subgroup: Some(2))
📤 Sent frame 5 (size: 113 bytes, subgroup: Some(2))
✅ Slideshow complete, sent EndOfGroup status
🏁 Session finished
```

### Browser Display:
- 🎨 **Live Canvas**: Colored rectangles (Red → Green → Blue → Yellow → Purple)
- 📊 **Real-time Stats**: Objects received, bytes processed, frame rate
- 📋 **Message Logs**: Draft-11 parsing and validation results
- 🔍 **Wire Format Details**: Varint parsing, extension extraction

## 🔧 **Key Technical Solutions**

### WebTransport Configuration
```rust
// Working configuration
let config = ServerConfig::builder()
    .with_bind_default(4433)  // Key change: use bind_default
    .with_identity(identity)
    .build();
```

### Staged Connection Setup
```rust
// 1. Establish WebTransport connection first
let conn = session_request.accept().await?;
tokio::time::sleep(Duration::from_millis(200)).await;

// 2. Test basic connectivity
conn.send_datagram("test".into())?;

// 3. THEN create MOQT handler
let handler = Arc::new(DatagramHandlerV9::with_config(conn, cfg));
```

### mkcert Certificate Setup
```bash
cd demo2/cert
mkcert localhost 127.0.0.1 ::1
# Creates localhost+2.pem and localhost+2-key.pem
```

## 🎯 **Available Demo Servers**

1. **`simple`**: Basic WebTransport test (no MOQT)
   ```bash
   cargo run --bin simple
   ```

2. **`moqt-demo-server`**: Full MOQT V9 demo with slideshow
   ```bash
   cargo run --bin moqt-demo-server
   ```

3. **`hybrid`**: Alternative implementation approach
   ```bash
   cargo run --bin hybrid
   ```

## **Validation Success Criteria**

✅ **Connection Established**: WebTransport handshake completes successfully  
✅ **Objects Received**: Browser receives MOQT datagram objects (5 frames)  
✅ **Extensions Parsed**: Draft-11 subgroup extensions correctly decoded  
✅ **Performance Metrics**: Consistent 1.5s frame intervals, ~113 bytes per object  
✅ **Wire Format Compliance**: All varint and encoding validation passes  
✅ **Clean Lifecycle**: Track activation → streaming → deactivation → EndOfGroup

##  **Browser Requirements**

- **Chrome 97+** or **Edge 97+**
- **Enable WebTransport**: Go to `chrome://flags/` and enable `#enable-experimental-web-platform-features`
- **mkcert**: Locally trusted certificates (already included)

## 📁 **File Structure**

```
demo2/
├── README.md           # This file
├── STATUS.md           # Previous troubleshooting notes
├── cert/               # mkcert certificates
│   ├── localhost+2.pem
│   └── localhost+2-key.pem
├── server/             # Rust WebTransport + MOQT server
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs     # Full MOQT demo
│   │   ├── simple.rs   # Basic WebTransport test
│   │   └── hybrid.rs   # Alternative implementation
└── web/                # Browser client
    ├── index.html      # Full demo interface
    ├── simple.html     # Basic WebTransport test
    ├── test.html       # Certificate setup helper
    └── player.js       # Independent Draft-11 parser
```

