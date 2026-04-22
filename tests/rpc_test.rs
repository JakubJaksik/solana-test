use tx_cutoff::rpc::{HttpRpcClient, SendOutcome, build_send_payload};

async fn spawn_mock(response_body: &'static str, status: u16) -> String {
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                status,
                response_body.len(),
                response_body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
    format!("http://{}", addr)
}

#[tokio::test]
async fn http_send_returns_tx_hash_on_success() {
    let url = spawn_mock(
        r#"{"jsonrpc":"2.0","id":1,"result":"0x0000000000000000000000000000000000000000000000000000000000000abc"}"#,
        200,
    )
    .await;
    let client = HttpRpcClient::new(&url).unwrap();
    let payload = build_send_payload(1, "0xf86c0101");
    let out = client
        .send_raw_transaction_prepared(&payload)
        .await
        .unwrap();
    match out {
        SendOutcome::Accepted { tx_hash } => {
            let s = format!("{:?}", tx_hash);
            // expect 0xabc... padding somewhere
            assert!(s.to_lowercase().ends_with("abc"), "got {}", s);
        }
        _ => panic!("expected Accepted"),
    }
}

#[tokio::test]
async fn http_send_maps_rpc_error_to_send_rejected() {
    let url = spawn_mock(
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}"#,
        200,
    )
    .await;
    let client = HttpRpcClient::new(&url).unwrap();
    let payload = build_send_payload(1, "0xf86c");
    let out = client
        .send_raw_transaction_prepared(&payload)
        .await
        .unwrap();
    match out {
        SendOutcome::Rejected { code, message } => {
            assert_eq!(code, -32000);
            assert!(message.contains("nonce too low"));
        }
        _ => panic!("expected Rejected"),
    }
}

#[test]
fn send_payload_is_well_formed_json_rpc() {
    let p = build_send_payload(42, "0xdeadbeef");
    assert!(p.contains(r#""jsonrpc":"2.0""#));
    assert!(p.contains(r#""method":"eth_sendRawTransaction""#));
    assert!(p.contains(r#""id":42"#));
    assert!(p.contains(r#"["0xdeadbeef"]"#));
}
