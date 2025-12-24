use crate::error::RtmpError;
use crate::protocol::RTMP_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn perform_server_handshake(socket: &mut TcpStream) -> Result<(), RtmpError> {
    // Read C0+C1 (1+1536 bytes)
    let mut c0c1 = [0u8; 1537];
    socket.read_exact(&mut c0c1).await?;

    if c0c1[0] != RTMP_VERSION {
        return Err(RtmpError::Protocol(format!(
            "Unsupported RTMP version: {}",
            c0c1[0]
        )));
    }

    tracing::debug!("Received C0+C1");

    // Prepare S0+S1+S2 response
    let mut response = Vec::with_capacity(3073);
    response.push(RTMP_VERSION);

    // S1 - Timestamp + Zero + Random
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    response.extend_from_slice(&time.to_be_bytes());
    response.extend_from_slice(&[0u8; 4]);

    // Random bytes for S1
    let random_bytes: Vec<u8> = (0..1528).map(|_| rand::random::<u8>()).collect();
    response.extend_from_slice(&random_bytes);

    // S2 - Echo client's C1
    response.extend_from_slice(&c0c1[1..1537]);

    socket.write_all(&response).await?;
    tracing::debug!("Sent S0+S1+S2");

    let mut c2 = [0u8; 1536];
    socket.read_exact(&mut c2).await?;
    tracing::debug!("Received C2");
    tracing::info!("Handshake completed successfully");

    Ok(())
}

pub async fn perform_client_handshake(socket: &mut TcpStream) -> Result<(), RtmpError> {
    // Send C0+C1
    let mut c0c1 = Vec::with_capacity(1537);
    c0c1.push(RTMP_VERSION);

    // C1 - Timestamp + Zero + Random
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    c0c1.extend_from_slice(&time.to_be_bytes());
    c0c1.extend_from_slice(&[0u8; 4]);

    let random: Vec<u8> = (0..1528).map(|_| rand::random::<u8>()).collect();
    c0c1.extend_from_slice(&random);
    socket.write_all(&c0c1).await?;

    // Read S0+S1+S2
    let mut s0s1s2 = [0u8; 3073];
    socket.read_exact(&mut s0s1s2).await?;

    // Verify version
    if s0s1s2[0] != RTMP_VERSION {
        return Err(RtmpError::Protocol(format!(
            "Unsupported RTMP version: {}",
            s0s1s2[0]
        )));
    }

    // Send C2 (echo S1)
    let c2 = &s0s1s2[1..1537];
    socket.write_all(c2).await?;

    tracing::info!("Client handshake completed successfully");
    Ok(())
}
