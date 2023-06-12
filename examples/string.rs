use std::{io::Write, time::SystemTime};

fn build_string_message(data: &str) -> anyhow::Result<Vec<u8>> {
    let mut msg = vec![0; std::mem::size_of::<u32>() + data.len()];
    // ROS 1 message strings are encoded as 4-bytes length and then the byte data.
    let mut w = std::io::Cursor::new(&mut msg);
    w.write(&(data.len() as u32).to_le_bytes())?;
    w.write(data.as_bytes())?;
    Ok(msg)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let server = foxglove_ws::FoxgloveWebSocket::new();
    tokio::spawn({
        let server = server.clone();
        async move { server.serve().await }
    });
    let channel = server
        .publish(
            "/data".to_string(),
            "ros1".to_string(),
            "std_msgs/String".to_string(),
            "string data".to_string(),
            "ros1msg".to_string(),
            false,
        )
        .await?;
    let channel_latching = server
        .publish(
            "/data_latching".to_string(),
            "ros1".to_string(),
            "std_msgs/String".to_string(),
            "string data".to_string(),
            "ros1msg".to_string(),
            true,
        )
        .await?;

    channel_latching
        .send(
            SystemTime::now().elapsed().unwrap().as_nanos() as u64,
            &build_string_message("latching!")?,
        )
        .await?;

    let mut counter = 0;
    loop {
        channel
            .send(
                SystemTime::now().elapsed().unwrap().as_nanos() as u64,
                &build_string_message(&format!("Hello {}!", counter))?,
            )
            .await?;
        counter += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}
