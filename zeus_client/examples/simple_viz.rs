use std::net::SocketAddr;
use std::time::Duration;
use zeus_client::ZeusClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Simple Viz Client...");

    let mut client = ZeusClient::new(9999)?;
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;

    println!("Connecting to {}...", addr);
    client.connect(addr).await?;
    println!("Connected!");

    loop {
        if let Ok(Some(data)) =
            tokio::time::timeout(Duration::from_millis(100), client.read_datagram()).await
        {
            if data.len() > 0 {
                println!(
                    "[Client] Packet: Type 0x{:02X}, Len {}",
                    data[0],
                    data.len()
                );

                if data[0] == 0xCC {
                    let bytes = &data[1..];
                    match zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(bytes) {
                        Ok(update) => {
                            if let Some(ghosts) = update.ghosts() {
                                println!("    -> {} ghosts in StateUpdate", ghosts.len());
                            } else {
                                println!("    -> StateUpdate with NO ghosts field?");
                            }
                        }
                        Err(e) => {
                            println!("    -> Failed to parse Flatbuffer: {:?}", e);
                        }
                    }
                } else if data[0] == 0xAA {
                    if data.len() >= 4 {
                        let entities = ((data[1] as u16) << 8) | (data[2] as u16);
                        println!("    -> Server Status: {} entities", entities);
                    }
                }
            }
        }
    }
}
