use http::Request;
use spectragql::clock::HlcClock;
use spectragql::dispatch::nats::NatsDispatch;
use spectragql::dispatch::webhook::WebhookDispatch;
use spectragql::dispatch::DispatchHandler;
use spectragql::payload::RequestInfo;
use spectragql::proxy::generate_command_receipt;

#[test]
fn test_mode_b_dispatch_failed_receipt_structure() {
    let (cmd_id, hlc) = HlcClock::global().now_uuidv7();
    let receipt = generate_command_receipt("submitOrder", &cmd_id, &hlc, "DISPATCH_FAILED");

    assert_eq!(
        receipt["data"]["submitOrder"]["status"],
        "DISPATCH_FAILED"
    );
    assert_eq!(
        receipt["data"]["submitOrder"]["commandId"],
        cmd_id.to_string()
    );
    assert_eq!(
        receipt["data"]["submitOrder"]["hlc"],
        hlc.to_compact_string()
    );
}

#[tokio::test]
async fn test_nats_unreachable_broker_returns_error() {
    // Unreachable local address
    let dispatch = NatsDispatch::new("127.0.0.1:1");

    let (request_id, hlc) = HlcClock::global().now_uuidv7();
    let req = Request::builder()
        .uri("/graphql")
        .method("POST")
        .body(())
        .unwrap();
    let (parts, _) = req.into_parts();
    let request_info = RequestInfo::new(request_id, hlc, parts);

    let result = dispatch.dispatch_request_info(&request_info).await;
    // Must return Err instead of silently returning Ok(())
    assert!(result.is_err(), "Expected NatsDispatch to fail when broker is unreachable");
}

#[tokio::test]
async fn test_webhook_unreachable_endpoint_returns_error() {
    let dispatch = WebhookDispatch::new("http://127.0.0.1:1/nonexistent");

    let (request_id, hlc) = HlcClock::global().now_uuidv7();
    let req = Request::builder()
        .uri("/api/events")
        .method("POST")
        .body(())
        .unwrap();
    let (parts, _) = req.into_parts();
    let request_info = RequestInfo::new(request_id, hlc, parts);

    let result = dispatch.dispatch_request_info(&request_info).await;
    assert!(result.is_err(), "Expected WebhookDispatch to fail when endpoint is unreachable");
}
