use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use tauri::Manager;
use tauri_plugin_shell::process::CommandChild;
use tauri_plugin_shell::ShellExt;

const BACKEND_HOST: &str = "127.0.0.1";
const BACKEND_PORT: u16 = 53682;

struct SidecarState(Mutex<Option<CommandChild>>);

fn stop_sidecar(app: &tauri::AppHandle) {
    if let Some(state) = app.try_state::<SidecarState>() {
        if let Ok(mut lock) = state.0.lock() {
            if let Some(child) = lock.take() {
                let _ = child.kill();
            }
        }
    }
}

fn backend_status_ok(host: &str, port: u16, timeout_ms: u64) -> bool {
    let addr = format!("{host}:{port}");
    let socket = match addr.to_socket_addrs().ok().and_then(|mut it| it.next()) {
        Some(v) => v,
        None => return false,
    };
    let mut stream = match TcpStream::connect_timeout(&socket, Duration::from_millis(timeout_ms)) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let timeout = Some(Duration::from_millis(timeout_ms));
    let _ = stream.set_read_timeout(timeout);
    let _ = stream.set_write_timeout(timeout);
    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0_u8; 256];
    let read = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return false,
    };
    let head = String::from_utf8_lossy(&buf[..read]);
    head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200")
}

fn port_is_free(host: &str, port: u16) -> bool {
    TcpListener::bind((host, port)).is_ok()
}

fn wait_backend_ready(host: &str, port: u16, attempts: usize, sleep_ms: u64) -> bool {
    for _ in 0..attempts {
        if backend_status_ok(host, port, 450) {
            return true;
        }
        thread::sleep(Duration::from_millis(sleep_ms));
    }
    false
}

#[cfg(target_os = "windows")]
fn cleanup_stale_backend_processes() {
    let _ = Command::new("taskkill")
        .args(["/F", "/IM", "familybook-backend.exe"])
        .status();
}

#[cfg(not(target_os = "windows"))]
fn cleanup_stale_backend_processes() {
    let _ = Command::new("pkill").args(["-f", "familybook-backend"]).status();
}

fn redirect_to_backend(app: &tauri::AppHandle, host: &str, port: u16) {
    let target = format!("http://{host}:{port}/");
    let js = format!("window.location.replace({target:?});");
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.eval(&js);
        return;
    }
    for window in app.webview_windows().values() {
        let _ = window.eval(&js);
    }
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            app.manage(SidecarState(Mutex::new(None)));

            let mut spawned_sidecar = false;
            if !backend_status_ok(BACKEND_HOST, BACKEND_PORT, 350) {
                if !port_is_free(BACKEND_HOST, BACKEND_PORT) {
                    cleanup_stale_backend_processes();
                    thread::sleep(Duration::from_millis(400));
                }
                if !backend_status_ok(BACKEND_HOST, BACKEND_PORT, 350) {
                    if !port_is_free(BACKEND_HOST, BACKEND_PORT) {
                        return Err(format!(
                            "Port {} is busy and backend is unhealthy. Close other Familybook instances and retry.",
                            BACKEND_PORT
                        )
                        .into());
                    }
                    let (mut rx, child) = app
                        .shell()
                        .sidecar("familybook-backend")
                        .map_err(|err| format!("Unable to prepare sidecar: {err}"))?
                        .args(["--host", BACKEND_HOST, "--port", "53682"])
                        .spawn()
                        .map_err(|err| format!("Unable to spawn sidecar: {err}"))?;

                    if let Some(state) = app.try_state::<SidecarState>() {
                        if let Ok(mut lock) = state.0.lock() {
                            *lock = Some(child);
                        }
                    }

                    tauri::async_runtime::spawn(async move {
                        while let Some(event) = rx.recv().await {
                            if let tauri_plugin_shell::process::CommandEvent::Stderr(line) = event {
                                let msg = String::from_utf8_lossy(&line);
                                eprintln!("sidecar: {msg}");
                            }
                        }
                    });
                    spawned_sidecar = true;
                }
            }

            if !wait_backend_ready(BACKEND_HOST, BACKEND_PORT, 90, 220) {
                return Err("Backend startup timed out waiting for /api/status.".into());
            }

            if spawned_sidecar || backend_status_ok(BACKEND_HOST, BACKEND_PORT, 350) {
                redirect_to_backend(&app.handle(), BACKEND_HOST, BACKEND_PORT);
            }

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while running Familybook desktop")
        .run(|app, event| match event {
            tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit => stop_sidecar(app),
            _ => {}
        });
}
