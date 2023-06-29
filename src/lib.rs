//! This library provides means to publish messages to the amazing Foxglove UI in Rust. It
//! implements part of the Foxglove WebSocket protocol described in
//! <https://github.com/foxglove/ws-protocol>.
//!
//! On its own the protocol does not fix a specific data scheme for the messages. But for Foxglove
//! to understand the messages it makes sense to follow the well-known serialization schemes
//! <https://mcap.dev/spec/registry>.
//!
//! # Example
//!
//! This is an example with single ROS1 channel/topic with the `std_msgs/String` message type.
//!
//! ```
//! use std::{io::Write, time::SystemTime};
//!
//! fn build_string_message(data: &str) -> anyhow::Result<Vec<u8>> {
//!     let mut msg = vec![0; std::mem::size_of::<u32>() + data.len()];
//!     // ROS 1 message strings are encoded as 4-bytes length and then the byte data.
//!     let mut w = std::io::Cursor::new(&mut msg);
//!     w.write(&(data.len() as u32).to_le_bytes())?;
//!     w.write(data.as_bytes())?;
//!     Ok(msg)
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let server = foxglove_ws::FoxgloveWebSocket::new();
//!     tokio::spawn({
//!         let server = server.clone();
//!         async move { server.serve(([127, 0, 0, 1], 8765)).await }
//!     });
//!     let channel = server
//!         .publish(
//!             "/data".to_string(),
//!             "ros1".to_string(),
//!             "std_msgs/String".to_string(),
//!             "string data".to_string(),
//!             "ros1msg".to_string(),
//!             false,
//!         )
//!         .await?;
//!     channel
//!         .send(
//!             SystemTime::now().elapsed().unwrap().as_nanos() as u64,
//!             &build_string_message("Hello!")?,
//!         )
//!         .await?;
//! }
//! ```
use std::{
    collections::HashMap,
    io::{Cursor, Write},
    mem::size_of,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::anyhow;
use futures_util::{stream::SplitSink, SinkExt, StreamExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;
use warp::{
    ws::{Message, WebSocket},
    Filter,
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerChannelMessage {
    id: usize,
    topic: String,
    encoding: String,
    schema_name: String,
    schema: String,
    schema_encoding: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum ServerMessage {
    #[serde(rename_all = "camelCase")]
    ServerInfo {
        name: String,
        capabilities: Vec<String>,
        supported_encodings: Vec<String>,
        metadata: HashMap<String, String>,
        session_id: String,
    },
    #[serde(rename_all = "camelCase")]
    Advertise { channels: Vec<ServerChannelMessage> },
}

type ClientChannelId = u32;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientSubscriptionMessage {
    id: ClientChannelId,
    channel_id: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum ClientMessage {
    #[serde(rename_all = "camelCase")]
    Subscribe {
        subscriptions: Vec<ClientSubscriptionMessage>,
    },
    #[serde(rename_all = "camelCase")]
    Unsubscribe {
        subscription_ids: Vec<ClientChannelId>,
    },
    #[serde(rename_all = "camelCase")]
    Close {},
}

#[derive(Debug)]
struct Client {
    id: Uuid,
    tx: mpsc::Sender<Message>,
    subscriptions: HashMap<usize, ClientChannelId>,
}

type Clients = RwLock<HashMap<Uuid, Client>>;

#[derive(Debug, Default)]
struct ClientState {
    clients: Clients,
}

#[derive(Debug)]
struct MessageData {
    timestamp_ns: u64,
    data: Vec<u8>,
}

impl MessageData {
    fn build_message(&self, subscription_id: u32) -> anyhow::Result<Message> {
        let mut buffer =
            vec![0; size_of::<u8>() + size_of::<u32>() + size_of::<u64>() + self.data.len()];
        {
            let mut w = Cursor::new(&mut buffer);
            // Write op code for the "Message Data" type.
            w.write(&(1 as u8).to_le_bytes())?;
            // Write subscription ID for this client.
            w.write(&subscription_id.to_le_bytes())?;
            w.write(&self.timestamp_ns.to_le_bytes())?;
            w.write(&self.data)?;
        }
        Ok(Message::binary(buffer))
    }
}

/// Represents a channel to send data with.
#[derive(Debug)]
pub struct Channel {
    id: usize,
    topic: String,
    is_latching: bool,

    clients: Arc<ClientState>,
    pinned_message: Arc<RwLock<Option<MessageData>>>,
}

impl Channel {
    /// Sends a message to all subscribed clients for this channel.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Point in time this message was published/created/logged.
    /// * `data` - Data buffer to publish.
    pub async fn send(&self, timestamp_ns: u64, data: &[u8]) -> anyhow::Result<()> {
        let message_data = MessageData {
            timestamp_ns,
            data: data.to_vec(),
        };
        for client in self.clients.clients.read().await.values() {
            if let Some(subscription_id) = client.subscriptions.get(&self.id) {
                log::debug!(
                    "Send message on {} to client {} ({}).",
                    self.topic,
                    client.id,
                    client.tx.capacity()
                );
                client
                    .tx
                    .try_send(message_data.build_message(*subscription_id)?)?;
            }
        }

        if self.is_latching {
            *self.pinned_message.write().await = Some(message_data);
        }

        Ok(())
    }
}

#[derive(Debug)]
struct ChannelMetadata {
    channel_message: ServerChannelMessage,
    pinned_message: Arc<RwLock<Option<MessageData>>>,
}

type Channels = RwLock<HashMap<usize, ChannelMetadata>>;

#[derive(Debug, Default)]
struct ChannelState {
    next_channel_id: AtomicUsize,
    channels: Channels,
}

/// The service WebSocket. It tracks the connected clients and takes care of subscriptions.
#[derive(Clone, Debug, Default)]
pub struct FoxgloveWebSocket {
    clients: Arc<ClientState>,
    channels: Arc<ChannelState>,
}

async fn initialize_client(
    user_ws_tx: &mut SplitSink<WebSocket, Message>,
    channels: &Channels,
    client_id: &Uuid,
) -> anyhow::Result<()> {
    user_ws_tx
        .send(Message::text(
            serde_json::to_string(&ServerMessage::ServerInfo {
                name: "test_server".to_string(),
                capabilities: vec![],
                supported_encodings: vec![],
                metadata: HashMap::default(),
                session_id: client_id.as_hyphenated().to_string(),
            })
            .unwrap(),
        ))
        .await?;

    let channel_messages = channels
        .read()
        .await
        .values()
        .map(|metadata| metadata.channel_message.clone())
        .collect();

    user_ws_tx
        .send(Message::text(
            serde_json::to_string(&ServerMessage::Advertise {
                channels: channel_messages,
            })
            .unwrap(),
        ))
        .await?;

    Ok(())
}

async fn handle_client_msg(
    tx: &mpsc::Sender<Message>,
    clients: &Arc<ClientState>,
    channels: &Arc<ChannelState>,
    client_id: &Uuid,
    ws_msg: &Message,
) -> anyhow::Result<()> {
    let msg = if ws_msg.is_text() {
        serde_json::from_str::<ClientMessage>(ws_msg.to_str().unwrap())?
    } else if ws_msg.is_binary() {
        return Err(anyhow!("Got binary message: unhandled at the moment."));
    } else if ws_msg.is_close() {
        ClientMessage::Close {}
    } else {
        return Err(anyhow!(
            "Got strage message, neither text nor binary: unhandled at the moment. {:?}",
            ws_msg
        ));
    };

    let mut clients = clients.clients.write().await;

    let channels = channels.channels.read().await;

    match msg {
        ClientMessage::Subscribe { ref subscriptions } => {
            let client = clients
                .get_mut(client_id)
                .ok_or(anyhow!("Client gone from client map?"))?;
            for ClientSubscriptionMessage { id, channel_id } in subscriptions {
                log::debug!(
                    "Client {} subscribed to {} with its own {}.",
                    client_id,
                    channel_id,
                    id
                );

                if let Some(ref channel_metadata) = channels.get(channel_id) {
                    client.subscriptions.insert(*channel_id, *id);
                    if let Some(message_data) =
                        channel_metadata.pinned_message.read().await.as_ref()
                    {
                        log::debug!("Sending latched: client {}.", client_id);
                        tx.send(message_data.build_message(*id)?).await?;
                    }
                }
            }
        }
        ClientMessage::Unsubscribe {
            ref subscription_ids,
        } => {
            let client = clients
                .get_mut(client_id)
                .ok_or(anyhow!("Client gone from client map?"))?;
            log::debug!("Client {} unsubscribes {:?}.", client_id, subscription_ids);
            client
                .subscriptions
                .retain(|_, subscription_id| !subscription_ids.contains(subscription_id));
        }
        ClientMessage::Close {} => {
            log::debug!("Client {} closed.", client_id);
            clients.remove(client_id);
        }
    }
    Ok(())
}

async fn client_connected(ws: WebSocket, clients: Arc<ClientState>, channels: Arc<ChannelState>) {
    // Split the socket into a sender and receive of messages.
    let (mut user_ws_tx, mut user_ws_rx) = ws.split();

    let client_id = Uuid::new_v4();
    log::info!("Client {} connected.", client_id);

    // Send server info.
    if let Err(err) = initialize_client(&mut user_ws_tx, &channels.channels, &client_id).await {
        log::error!("Failed to initialize client: {}.", err);
        return;
    }

    let (tx, rx) = mpsc::channel(20);
    let mut rx = ReceiverStream::new(rx);

    // Setup the sender queue task.
    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            user_ws_tx
                .send(message)
                .unwrap_or_else(|e| {
                    log::error!("Failed websocket send: {}.", e);
                })
                .await;
        }
    });

    // Save the sender in our list of connected users.
    clients.clients.write().await.insert(
        client_id,
        Client {
            id: client_id,
            tx: tx.clone(),
            subscriptions: HashMap::new(),
        },
    );

    while let Some(result) = user_ws_rx.next().await {
        let ws_msg = match result {
            Ok(ws_msg) => ws_msg,
            Err(err) => {
                log::error!("Failed receiving, websocket error: {}.", err);
                break;
            }
        };
        if let Err(err) = handle_client_msg(&tx, &clients, &channels, &client_id, &ws_msg).await {
            log::error!("Failed handling client message: {}.", err);
            break;
        }
    }
}

impl FoxgloveWebSocket {
    /// Creates a new Foxglove WebSocket service.
    pub fn new() -> Self {
        FoxgloveWebSocket::default()
    }

    /// Serves connecting clients.
    ///
    /// # Arguments
    ///
    /// `addr` -- Address to listen on.
    pub async fn serve(&self, addr: impl Into<SocketAddr>) {
        let clients = self.clients.clone();
        let clients = warp::any().map(move || clients.clone());
        let channels = self.channels.clone();
        let channels = warp::any().map(move || channels.clone());
        let foxglove_ws = warp::path::end().and(
            warp::ws()
                .and(warp::filters::header::value("sec-websocket-protocol"))
                .and(clients)
                .and(channels)
                .map(|ws: warp::ws::Ws, proto, clients, channels| {
                    let reply =
                        ws.on_upgrade(move |socket| client_connected(socket, clients, channels));
                    Ok(warp::reply::with_header(
                        reply,
                        "sec-websocket-protocol",
                        proto,
                    ))
                }),
        );
        warp::serve(foxglove_ws).run(addr).await;
    }

    /// Advertise a new publisher.
    ///
    /// There are several different message encoding schemes that are supported by Foxglove.
    /// <https://mcap.dev/spec/registry> contains more information on how to set the arguments to
    /// this function.
    ///
    /// # Arguments
    ///
    /// * `topic` - Name of the topic of this new channel.
    /// * `encoding` - Channel message encoding.
    /// * `schema_name` - Name of the schema.
    /// * `schema` - Schema describing the message format.
    /// * `scheme_encoding` - Encoding of this channel's schema.
    /// * `is_latching` - Whether messages sent of this channel are sticky. Each newly connecting
    ///    client will be message the last sticky message that was sent on this channel.
    pub async fn publish(
        &self,
        topic: String,
        encoding: String,
        schema_name: String,
        schema: String,
        schema_encoding: String,
        is_latching: bool,
    ) -> anyhow::Result<Channel> {
        let channel_id = self
            .channels
            .next_channel_id
            .fetch_add(1, Ordering::Relaxed);
        log::info!("Publish new channel {}: {}.", topic, channel_id);
        let channel = Channel {
            id: channel_id,
            topic: topic.clone(),
            is_latching,
            clients: self.clients.clone(),
            pinned_message: Arc::default(),
        };
        let channel_message = ServerChannelMessage {
            id: channel_id,
            topic,
            encoding,
            schema_name,
            schema,
            schema_encoding,
        };

        // Advertise the newly created channel.
        for client in self.clients.clients.read().await.values() {
            client
                .tx
                .send(Message::text(
                    serde_json::to_string(&ServerMessage::Advertise {
                        channels: vec![channel_message.clone()],
                    })
                    .unwrap(),
                ))
                .await?;
        }

        self.channels.channels.write().await.insert(
            channel_id,
            ChannelMetadata {
                channel_message,
                pinned_message: channel.pinned_message.clone(),
            },
        );

        Ok(channel)
    }
}
