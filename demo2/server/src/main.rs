use anyhow::Result;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use wtransport::{Endpoint, ServerConfig, tls::Identity};
use wtransport::endpoint::{IncomingSession, SessionRequest};

// Import your V9 handler
use moqtail::transport::datagram_handler_v9::{
    DatagramHandlerV9, DatagramConfigV9, EnhancedObjectStatus
};
use moqtail::model::data::object::Object;
use moqtail::model::common::location::Location;
use moqtail::model::data::constant::{ObjectForwardingPreference, ObjectStatus};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    println!("🚀 MOQT V9 End-to-End Demo Server");
    
    // 1) Load mkcert certificates for WebTransport
    println!("🔐 Loading mkcert certificates...");
    let cert_path = r"C:\Users\bd024583\Desktop\moqtail-rs\demo2\cert\localhost+2.pem";
    let key_path = r"C:\Users\bd024583\Desktop\moqtail-rs\demo2\cert\localhost+2-key.pem";
    let identity = Identity::load_pemfiles(cert_path, key_path).await?;
    println!("✅ Certificates loaded successfully");

    // 2) Start WebTransport server
    let config = ServerConfig::builder()
        .with_bind_default(4433)
        .with_identity(identity)
        .build();

    let server = Endpoint::server(config)?;
    println!("📡 Server listening on https://localhost:4433 (WebTransport)");
    println!("🌐 Open browser to http://localhost:8080 and click Connect");

    // 3) Accept connections
    loop {
        let incoming_session = server.accept().await;
        tokio::spawn(async move {
            match handle_incoming_session(incoming_session).await {
                Ok(_) => println!("✅ Session completed successfully"),
                Err(e) => eprintln!("❌ Session error: {e:?}"),
            }
        });
    }
}

async fn handle_incoming_session(incoming_session: IncomingSession) -> Result<()> {
    // Wait for the session request
    let session_request = incoming_session.await?;
    println!("🔌 New WebTransport request from: {:?}", session_request.remote_address());
    
    // Accept the request to get the connection
    let conn = session_request.accept().await?;
    println!("✅ WebTransport connection established");

    // Wait a bit for connection to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Test basic connection first
    println!("📡 Testing datagrams...");
    let test_data: bytes::Bytes = b"Hello WebTransport!".as_ref().into();
    if let Err(e) = conn.send_datagram(test_data) {
        println!("⚠️ Failed to send test datagram: {}", e);
    } else {
        println!("✅ Test datagram sent successfully");
    }

    // Now create V9 handler
    println!("🔧 Setting up MOQT V9 handler...");
    let cfg = DatagramConfigV9::default()
        .with_client_mode(false)              // Server mode
        .with_publisher_role(true)            // We send objects
        .with_max_datagram_size(1200)         // Conservative for WebTransport
        .with_effective_max_size(Some(1000))  // Leave room for headers
        .with_recv_queue_cap(256)
        .with_metrics_report_interval(Duration::from_secs(5));

    let handler = Arc::new(DatagramHandlerV9::with_config(conn, cfg));
    
    println!("🔧 V9 Handler initialized");

    // 5) Activate track 42 for image slideshow
    handler.activate_track(
        1,                      // request_id
        42,                     // track_alias  
        None,                   // group_order (keep simple)
        None,                   // largest_location
        Some(60),               // expires in 60s
    ).await?;
    
    println!("📺 Track 42 activated for slideshow");

    // 6) Create demo image frames (small embedded PNGs)
    let frames = create_demo_frames();
    
    println!("🎬 Starting slideshow with {} frames", frames.len());

    // 7) Send frames as MOQT objects
    for (i, frame_data) in frames.iter().enumerate() {
        let object = Object {
            track_alias: 42,
            location: Location::new(1, (i + 1) as u64), // group=1, object=1..N
            publisher_priority: 128,
            forwarding_preference: ObjectForwardingPreference::Datagram,
            subgroup_id: if i == 0 { Some(1) } else { Some(2) }, // Keyframe vs regular frame
            status: ObjectStatus::Normal,
            extensions: None,
            payload: Some(Bytes::from(frame_data.clone())),
        };

        // Send with V9 extensions for subgroup
        let extensions = if object.subgroup_id.is_some() {
            Some(vec![moqtail::transport::datagram_handler_v9::ExtensionHeader {
                extension_type: 0x04, // SUBGROUP_ID extension type
                data: Bytes::from(vec![(i % 2) as u8]), // Simple subgroup data
            }])
        } else {
            None
        };

        handler.send_object(&object, extensions).await?;
        
        println!("📤 Sent frame {} (size: {} bytes, subgroup: {:?})", 
                 i + 1, frame_data.len(), object.subgroup_id);
        
        sleep(Duration::from_millis(800)).await; // ~1.25 fps for demo
    }

    // 8) Send end-of-group status
    handler.send_object_status(
        42, 
        1, 
        frames.len() as u64, 
        EnhancedObjectStatus::EndOfGroup, 
        None
    ).await?;

    println!("✅ Slideshow complete, sent EndOfGroup status");

    // 9) Keep connection alive for a bit
    println!("⏳ Keeping connection open for 10 seconds...");
    sleep(Duration::from_secs(10)).await;

    // 10) Cleanup
    handler.close().await;
    println!("🏁 Session finished");

    Ok(())
}

fn create_demo_frames() -> Vec<Vec<u8>> {
    // Create simple colored PNG-like frames for the demo
    // In a real scenario, you'd load actual image files
    vec![
        create_colored_frame(255, 0, 0),     // Red frame
        create_colored_frame(0, 255, 0),     // Green frame  
        create_colored_frame(0, 0, 255),     // Blue frame
        create_colored_frame(255, 255, 0),   // Yellow frame
        create_colored_frame(255, 0, 255),   // Magenta frame
    ]
}

fn create_colored_frame(r: u8, g: u8, b: u8) -> Vec<u8> {
    // Create a minimal "image" payload for demo
    // This is just colored bytes, not actual PNG format
    // The browser demo will interpret this as raw data
    let mut frame = Vec::new();
    
    // Simple header identifying color
    frame.extend_from_slice(b"DEMO_FRAME");
    frame.push(r);
    frame.push(g); 
    frame.push(b);
    
    // Add some payload data (simulate compressed image)
    for i in 0..100 {
        frame.push(((i + r as usize + g as usize + b as usize) % 256) as u8);
    }
    
    frame
}