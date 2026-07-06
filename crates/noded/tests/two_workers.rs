//! M1.3 GATE: two `zeph` worker processes connect and exchange heartbeats,
//! visible in each other's logs.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Kills the child process on drop so failed asserts don't leak workers.
struct Worker {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_zeph(args: &[&str]) -> Worker {
    let mut child = Command::new(env!("CARGO_BIN_EXE_zeph"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn zeph");
    let (tx, rx) = mpsc::channel();
    let stdout = child.stdout.take().expect("stdout piped");
    let tx_out = tx.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = tx_out.send(line);
        }
    });
    let stderr = child.stderr.take().expect("stderr piped");
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    Worker { child, lines: rx }
}

fn wait_for(worker: &Worker, needle: &str, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match worker.lines.recv_timeout(remaining) {
            Ok(line) if line.contains(needle) => return Some(line),
            Ok(_) => continue,
            Err(_) => return None,
        }
    }
}

#[test]
fn two_workers_exchange_heartbeats() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let timeout = Duration::from_secs(30);

    let worker_a = spawn_zeph(&[
        "--data-dir",
        dir_a.path().to_str().unwrap(),
        "--reach",
        "local",
    ]);
    let addr_line = wait_for(&worker_a, "ZEPH_ADDR ", timeout).expect("A prints its address");
    let addr = addr_line
        .trim()
        .strip_prefix("ZEPH_ADDR ")
        .expect("well-formed addr line")
        .to_string();
    assert!(addr.contains('@'), "self address is dialable: {addr}");

    let worker_b = spawn_zeph(&[
        "--data-dir",
        dir_b.path().to_str().unwrap(),
        "--reach",
        "local",
        "--heartbeat-secs",
        "1",
        "--peer",
        &addr,
    ]);

    // B heartbeats A...
    assert!(
        wait_for(&worker_b, "peer alive", timeout).is_some(),
        "worker B never saw worker A alive"
    );
    // ...and A visibly served B's ping.
    assert!(
        wait_for(&worker_a, "ping served", timeout).is_some(),
        "worker A never logged serving a ping"
    );
}

/// MU.1 GATE: `zeph status` returns the live peer table over the control socket.
#[test]
fn status_subcommand_reports_live_peers() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let timeout = Duration::from_secs(30);

    let worker_a = spawn_zeph(&[
        "--data-dir",
        dir_a.path().to_str().unwrap(),
        "--reach",
        "local",
    ]);
    let addr_line = wait_for(&worker_a, "ZEPH_ADDR ", timeout).expect("A prints its address");
    let addr = addr_line
        .trim()
        .strip_prefix("ZEPH_ADDR ")
        .unwrap()
        .to_string();

    let worker_b = spawn_zeph(&[
        "--data-dir",
        dir_b.path().to_str().unwrap(),
        "--reach",
        "local",
        "--heartbeat-secs",
        "1",
        "--peer",
        &addr,
    ]);
    assert!(
        wait_for(&worker_b, "peer alive", timeout).is_some(),
        "worker B never saw worker A alive"
    );

    // Query B's control socket via the CLI, polling until the 1s control
    // sync has caught up with the first successful probe.
    let peer_id_prefix = &addr[..12];
    let stdout = poll_status(dir_b.path().to_str().unwrap(), timeout, |out| {
        out.contains(peer_id_prefix) && out.contains("alive")
    })
    .expect("peer A eventually shown alive in B's status");
    assert!(
        stdout.contains("node   "),
        "status shows own node id: {stdout}"
    );
    drop(worker_a);
    drop(worker_b);
}

/// Poll `zeph status` until `pred` holds or `timeout` elapses; returns the
/// last stdout on success.
fn poll_status(data_dir: &str, timeout: Duration, pred: impl Fn(&str) -> bool) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let output = Command::new(env!("CARGO_BIN_EXE_zeph"))
            .args(["status", "--data-dir", data_dir])
            .output()
            .expect("run zeph status");
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        if output.status.success() && pred(&stdout) {
            return Some(stdout);
        }
        if Instant::now() >= deadline {
            eprintln!("last status output:\n{stdout}");
            return None;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Minimal HTTP GET over std TcpStream (no client dependency).
fn http_get(port: u16, path: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect dashboard");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// MU.2 GATE: embedded dashboard serves on 127.0.0.1 with token auth; the
/// token persists across a daemon restart so an open page keeps working.
#[test]
fn dashboard_serves_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap();
    let port = free_port();
    let port_arg = port.to_string();
    let timeout = Duration::from_secs(30);

    let worker = spawn_zeph(&[
        "--data-dir",
        dir_path,
        "--reach",
        "local",
        "--dashboard-port",
        &port_arg,
    ]);
    wait_for(&worker, "dashboard listening", timeout).expect("dashboard came up");

    // Page is served, embedded (no external assets), token injected.
    let (status, body) = http_get(port, "/");
    assert_eq!(status, 200);
    assert!(body.contains("zeph"), "dashboard HTML served");
    assert!(!body.contains("__TOKEN__"), "token placeholder replaced");
    assert!(
        !body.contains("http://") || body.contains("127.0.0.1"),
        "no external asset URLs"
    );

    let token = std::fs::read_to_string(dir.path().join("control.token")).unwrap();
    let token = token.trim();

    // Authed API works; wrong token rejected.
    let (status, body) = http_get(port, &format!("/api/status?token={token}"));
    assert_eq!(status, 200);
    assert!(body.contains("node_id"), "status JSON: {body}");
    let (status, _) = http_get(port, "/api/status?token=wrong");
    assert_eq!(status, 401);

    // Restart the daemon: same data dir → same token still valid.
    drop(worker);
    let worker = spawn_zeph(&[
        "--data-dir",
        dir_path,
        "--reach",
        "local",
        "--dashboard-port",
        &port_arg,
    ]);
    wait_for(&worker, "dashboard listening", timeout).expect("dashboard restarted");
    let (status, body) = http_get(port, &format!("/api/status?token={token}"));
    assert_eq!(status, 200, "old token still valid after restart: {body}");
    drop(worker);
}

/// M1.5 GATE: five workers — join propagates through the overlay (everyone's
/// active view fills beyond its single bootstrap contact), and a killed
/// worker is marked dead in the others' peer tables.
#[test]
fn five_workers_membership_join_and_death() {
    let timeout = Duration::from_secs(60);
    let dirs: Vec<tempfile::TempDir> = (0..5).map(|_| tempfile::tempdir().unwrap()).collect();

    // A is the bootstrap contact; B..E join via A.
    let worker_a = spawn_zeph(&[
        "--data-dir",
        dirs[0].path().to_str().unwrap(),
        "--reach",
        "local",
        "--heartbeat-secs",
        "1",
    ]);
    let addr_a = wait_for(&worker_a, "ZEPH_ADDR ", timeout)
        .expect("A prints its address")
        .trim()
        .strip_prefix("ZEPH_ADDR ")
        .unwrap()
        .to_string();

    let mut workers = vec![worker_a];
    let mut addrs = vec![addr_a.clone()];
    for dir in dirs.iter().skip(1) {
        let w = spawn_zeph(&[
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--reach",
            "local",
            "--heartbeat-secs",
            "1",
            "--peer",
            &addr_a,
        ]);
        let addr = wait_for(&w, "ZEPH_ADDR ", timeout)
            .expect("worker prints its address")
            .trim()
            .strip_prefix("ZEPH_ADDR ")
            .unwrap()
            .to_string();
        workers.push(w);
        addrs.push(addr);
    }

    // Join propagation: every worker ends up with >=3 alive peers — more than
    // its single bootstrap contact, so FORWARD_JOIN demonstrably spread joins.
    for (i, dir) in dirs.iter().enumerate() {
        let out = poll_status(dir.path().to_str().unwrap(), timeout, |out| {
            out.matches(" alive ").count() >= 3
        });
        assert!(
            out.is_some(),
            "worker {i} never saw >=3 alive peers via join propagation"
        );
    }

    // Kill worker C (index 2); everyone else must mark it dead.
    let victim_prefix = addrs[2][..12].to_string();
    drop(workers.remove(2));
    let survivor_dirs = [&dirs[0], &dirs[1], &dirs[3], &dirs[4]];
    for (i, dir) in survivor_dirs.iter().enumerate() {
        let out = poll_status(dir.path().to_str().unwrap(), timeout, |out| {
            out.lines()
                .any(|l| l.starts_with(&victim_prefix) && l.contains(" down "))
        });
        assert!(
            out.is_some(),
            "survivor {i} never marked the killed worker down"
        );
    }
}
