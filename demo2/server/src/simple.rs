use anyhow::Result;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::sleep;
use wtransport::{Endpoint, ServerConfig, tls::Identity};
use wtransport::endpoint::IncomingSession;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    println!("🚀 Simple WebTransport Test Server");
    
    // Load mkcert certificates
    println!("🔐 Loading mkcert certificates...");
    let cert_path = r"C:\Users\bd024583\Desktop\moqtail-rs\demo2\cert\localhost+2.pem";
    let key_path = r"C:\Users\bd024583\Desktop\moqtail-rs\demo2\cert\localhost+2-key.pem";
    let identity = Identity::load_pemfiles(cert_path, key_path).await?;
    println!("✅ Certificates loaded successfully");

    // Start WebTransport server  
    let config = ServerConfig::builder()
        .with_bind_default(4433)
        .with_identity(identity)
        .build();

    let server = Endpoint::server(config)?;
    println!("📡 Server listening on https://localhost:4433 (WebTransport)");
    println!("🌐 Open browser to http://localhost:8080/simple.html");

    // Accept connections
    loop {
        let incoming_session = server.accept().await;
        tokio::spawn(async move {
            match handle_simple_session(incoming_session).await {
                Ok(_) => println!("✅ Session completed successfully"),
                Err(e) => eprintln!("❌ Session error: {e:?}"),
            }
        });
    }
}

async fn handle_simple_session(incoming_session: IncomingSession) -> Result<()> {
    // Wait for the session request
    let session_request = incoming_session.await?;
    println!("🔌 New WebTransport request from: {:?}", session_request.remote_address());
    
    // Accept the request to get the connection
    let conn = session_request.accept().await?;
    println!("✅ WebTransport connection established");

    // Send some simple test data
    for i in 0..10 {
        let test_data: Bytes = format!("Hello from server! Message {}", i + 1).into();
        if let Err(e) = conn.send_datagram(test_data) {
            println!("⚠️ Failed to send datagram {}: {}", i + 1, e);
        } else {
            println!("📤 Sent datagram {}", i + 1);
        }
        sleep(Duration::from_millis(1000)).await;
    }

    // Keep connection alive
    println!("🔄 Keeping connection alive...");
    sleep(Duration::from_secs(30)).await;
    
    Ok(())
}