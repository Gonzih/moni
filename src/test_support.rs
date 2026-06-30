use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordedHttpRequest {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub(crate) struct DiscordHttpProxy {
    base_url: String,
    requests: Arc<Mutex<Vec<RecordedHttpRequest>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl DiscordHttpProxy {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = requests.clone();
        let thread_stop = stop.clone();
        let handle = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept() {
                    handle_connection(stream, &thread_requests);
                }
                thread::sleep(Duration::from_millis(5));
            }
        });

        Self {
            base_url: format!("http://{addr}"),
            requests,
            stop,
            handle: Some(handle),
        }
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn requests(&self) -> Vec<RecordedHttpRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub(crate) async fn wait_for_path(&self, needle: &str) -> RecordedHttpRequest {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(request) = self
                .requests()
                .into_iter()
                .find(|request| request.path.contains(needle))
            {
                return request;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for request path containing {needle}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for DiscordHttpProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(
            self.base_url
                .strip_prefix("http://")
                .expect("test proxy base url is host:port"),
        );
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_connection(mut stream: TcpStream, requests: &Arc<Mutex<Vec<RecordedHttpRequest>>>) {
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let Some((method, path, body)) = read_http_request(&mut stream) else {
        return;
    };
    requests.lock().unwrap().push(RecordedHttpRequest {
        method,
        path: path.clone(),
        body,
    });
    let (status, response_body) = response_for_path(&path);
    let response = if response_body.is_empty() {
        format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    } else {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        )
    };
    let _ = stream.write_all(response.as_bytes());
}

fn read_http_request(stream: &mut TcpStream) -> Option<(String, String, String)> {
    let mut buffer = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = headers.lines();
    let mut first = lines.next()?.split_whitespace();
    let method = first.next()?.to_string();
    let path = first.next()?.to_string();
    let content_length = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    Some((method, path, String::from_utf8_lossy(&body).to_string()))
}

fn response_for_path(path: &str) -> (&'static str, String) {
    if path.contains("/gateway") {
        return (
            "200 OK",
            json!({
                "url": "ws://127.0.0.1:9"
            })
            .to_string(),
        );
    }
    if path.contains("/typing") {
        return ("204 No Content", String::new());
    }
    if path.contains("/guilds/") && path.contains("/channels") {
        return (
            "200 OK",
            json!({
                "id": "999",
                "guild_id": "55",
                "type": 0,
                "name": "created-channel",
                "topic": "created topic"
            })
            .to_string(),
        );
    }
    if path.contains("/applications/") && path.contains("/commands") {
        return ("200 OK", "[]".to_string());
    }
    if path.contains("/interactions/") && path.contains("/callback") {
        return ("204 No Content", String::new());
    }
    if path.contains("/messages") {
        return ("200 OK", discord_message_json().to_string());
    }
    ("200 OK", "{}".to_string())
}

fn discord_message_json() -> serde_json::Value {
    json!({
        "id": "111",
        "channel_id": "123",
        "author": {
            "id": "999",
            "username": "moni",
            "discriminator": "0001",
            "global_name": null,
            "avatar": null,
            "bot": true,
            "system": false,
            "mfa_enabled": false,
            "banner": null,
            "accent_color": null,
            "locale": null,
            "verified": null,
            "email": null,
            "flags": 0,
            "premium_type": 0,
            "public_flags": 0
        },
        "content": "ok",
        "timestamp": "2020-01-01T00:00:00.000000+00:00",
        "edited_timestamp": null,
        "tts": false,
        "mention_everyone": false,
        "mentions": [],
        "mention_roles": [],
        "attachments": [],
        "embeds": [],
        "pinned": false,
        "type": 0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepted_stream_from_client(
        writer: impl FnOnce(TcpStream) + Send + 'static,
    ) -> (TcpStream, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let stream = TcpStream::connect(addr).unwrap();
            writer(stream);
        });
        let (stream, _) = listener.accept().unwrap();
        (stream, handle)
    }

    #[test]
    fn handle_connection_ignores_empty_streams() {
        let (stream, handle) = accepted_stream_from_client(drop);
        let requests = Arc::new(Mutex::new(Vec::new()));

        handle_connection(stream, &requests);
        handle.join().unwrap();

        assert!(requests.lock().unwrap().is_empty());
    }

    #[test]
    fn read_http_request_reads_body_after_headers() {
        let (mut stream, handle) = accepted_stream_from_client(|mut client| {
            client
                .write_all(b"POST /channels/1/messages HTTP/1.1\r\nContent-Length: 5\r\n\r\n")
                .unwrap();
            thread::sleep(Duration::from_millis(20));
            client.write_all(b"hello").unwrap();
        });

        let request = read_http_request(&mut stream).unwrap();
        handle.join().unwrap();

        assert_eq!(request.0, "POST");
        assert_eq!(request.1, "/channels/1/messages");
        assert_eq!(request.2, "hello");
    }

    #[test]
    fn read_http_request_accumulates_split_headers() {
        let (mut stream, handle) = accepted_stream_from_client(|mut client| {
            client
                .write_all(b"GET /split HTTP/1.1\r\nContent-")
                .unwrap();
            thread::sleep(Duration::from_millis(20));
            client.write_all(b"Length: 0\r\n\r\n").unwrap();
        });

        let request = read_http_request(&mut stream).unwrap();
        handle.join().unwrap();

        assert_eq!(request.0, "GET");
        assert_eq!(request.1, "/split");
        assert_eq!(request.2, "");
    }

    #[test]
    fn read_http_request_accepts_short_body_on_disconnect() {
        let (mut stream, handle) = accepted_stream_from_client(|mut client| {
            client
                .write_all(b"POST /channels/1/messages HTTP/1.1\r\nContent-Length: 5\r\n\r\n")
                .unwrap();
        });

        let request = read_http_request(&mut stream).unwrap();
        handle.join().unwrap();

        assert_eq!(request.2, "");
    }

    #[test]
    fn response_for_path_covers_discord_routes_and_fallback() {
        assert_eq!(
            response_for_path("/api/v10/channels/123/typing").0,
            "204 No Content"
        );
        assert!(
            response_for_path("/api/v10/guilds/55/channels")
                .1
                .contains("created-channel")
        );
        assert!(
            response_for_path("/api/v10/channels/123/messages")
                .1
                .contains("\"id\":\"111\"")
        );
        assert_eq!(response_for_path("/api/v10/unknown").1, "{}");
    }
}
