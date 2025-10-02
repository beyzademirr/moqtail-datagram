use warp::Filter;

#[tokio::main]
async fn main() {
    let routes = warp::fs::dir(".");
    
    println!("🌐 Web server running on http://localhost:8080");
    println!("📱 Open http://localhost:8080 in your browser");
    
    warp::serve(routes)
        .run(([127, 0, 0, 1], 8080))
        .await;
}