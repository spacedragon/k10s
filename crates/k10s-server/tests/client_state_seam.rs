use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{ClientFrame, ClientKind, ServerFrame, ServerKind};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget, Query, QueryResult};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::test]
async fn client_state_hello_subscribe_ack_round_trips_over_loopback() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "seam-server"),
    )
    .await
    .unwrap();
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let (mut socket, _) = connect_async(&url).await.unwrap();
    let mut client = ClientState::new(ClientConfig::default());

    client.connect(ConnectTarget::new(url, "secret")).unwrap();
    send_client_frame(&mut socket, client.take_outbound().unwrap()).await;
    client
        .apply(receive_server_frame(&mut socket).await)
        .unwrap();

    let bootstrap = client.begin(Query::Bootstrap).unwrap();
    send_client_frame(&mut socket, client.take_outbound().unwrap()).await;
    client
        .apply(receive_server_frame(&mut socket).await)
        .unwrap();
    assert!(matches!(
        client.take(bootstrap),
        Some(QueryResult::Bootstrap(_))
    ));

    for expected_sequence in 1..=2 {
        let _subscription = client.subscribe_bootstrap_status().unwrap();
        send_client_frame(&mut socket, client.take_outbound().unwrap()).await;
        let subscribed = receive_server_frame(&mut socket).await;
        assert_eq!(subscribed.kind, ServerKind::Subscribed);
        assert_eq!(subscribed.sequence, Some(expected_sequence));
        client.apply(subscribed).unwrap();

        let ack = client.take_outbound().unwrap();
        assert_eq!(ack.kind, ClientKind::Ack);
        send_client_frame(&mut socket, ack).await;
        send_client_frame(
            &mut socket,
            ClientFrame {
                kind: ClientKind::Ping,
                request_id: None,
                subscription_id: None,
                sequence: None,
                payload: serde_json::Value::Null,
            },
        )
        .await;
        let pong = receive_server_frame(&mut socket).await;
        assert_eq!(pong.kind, ServerKind::Pong, "unexpected frame: {pong:?}");
    }

    assert_eq!(client.live_subscription_count(), 2);
    assert!(!client.server_state_invalid());
    server.shutdown().await.unwrap();
}

async fn send_client_frame(socket: &mut Socket, frame: ClientFrame) {
    socket
        .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
        .unwrap();
}

async fn receive_server_frame(socket: &mut Socket) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_millis(500), socket.next())
        .await
        .expect("server response timeout")
        .unwrap()
        .unwrap();
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
