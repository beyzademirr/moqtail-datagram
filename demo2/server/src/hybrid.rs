use anyhow::Result;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use wtransport::{Endpoint, ServerConfig, tls::Identity};
use wtransport::endpoint::IncomingSession;

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
    
    println!("🚀 MOQT V9 Hybrid Demo Server");
    
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
            match handle_hybrid_session(incoming_session).await {
                Ok(_) => println!("✅ Session completed successfully"),
                Err(e) => eprintln!("❌ Session error: {e:?}"),
            }
        });
    }
}

async fn handle_hybrid_session(incoming_session: IncomingSession) -> Result<()> {
    // Wait for the session request
    let session_request = incoming_session.await?;
    println!("🔌 New WebTransport request from: {:?}", session_request.remote_address());
    
    // Accept the request to get the connection
    let conn = session_request.accept().await?;
    println!("✅ WebTransport connection established");

    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Send a test message to verify connection works
    let test_data: Bytes = "🎬 Starting MOQT slideshow...".into();
    if let Err(e) = conn.send_datagram(test_data) {
        println!("⚠️ Failed to send test message: {}", e);
        return Ok(());
    } else {
        println!("✅ Test message sent successfully");
    }

    // Now create V9 handler AFTER connection is established
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
        let location = Location::absolute(i as u64);
        
        // Create object with V9 subgroup extensions
        let mut object = Object::new(42, location, frame_data.clone());
        object.forwarding_preference = ObjectForwardingPreference::Group;
        object.subgroup_id = Some(1);
        
        // Send via V9 handler 
        match handler.send_object(object).await {
            Ok(_) => println!("📤 Sent frame {} ({}KB)", i + 1, frame_data.len() / 1024),
            Err(e) => println!("⚠️ Failed to send frame {}: {}", i + 1, e),
        }
        
        // Frame rate control
        sleep(Duration::from_millis(1500)).await;
    }

    // 8) Send EndOfGroup status
    let end_status = EnhancedObjectStatus::new(
        42,                                    // track_alias
        Location::absolute(frames.len() as u64), // next expected location  
        ObjectStatus::EndOfGroup,
        None,                                  // no error
    );
    
    match handler.send_status(end_status).await {
        Ok(_) => println!("🏁 Sent EndOfGroup status"),
        Err(e) => println!("⚠️ Failed to send status: {}", e),
    }

    println!("🎉 Slideshow completed! Keeping connection alive...");
    
    // Keep connection alive
    sleep(Duration::from_secs(30)).await;
    
    Ok(())
}

fn create_demo_frames() -> Vec<Bytes> {
    vec![
        create_colored_frame("Red", [255, 100, 100]),
        create_colored_frame("Green", [100, 255, 100]), 
        create_colored_frame("Blue", [100, 100, 255]),
        create_colored_frame("Yellow", [255, 255, 100]),
        create_colored_frame("Purple", [255, 100, 255]),
        create_colored_frame("Cyan", [100, 255, 255]),
    ]
}

fn create_colored_frame(name: &str, color: [u8; 3]) -> Bytes {
    // Create simple "frame" metadata as JSON
    let frame_json = format!(
        r#"{{"type":"frame","name":"{}","color":[{},{},{}],"timestamp":{}}}"#,
        name, color[0], color[1], color[2],
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    
    frame_json.into()
}