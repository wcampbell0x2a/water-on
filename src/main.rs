use clap::Parser;
use rumqttc::{Client, Connection, Event, MqttOptions, Packet, QoS};
use std::process::exit;
use std::time::{Duration, Instant};

/// Turn on a zigbee2mqtt valve and confirm it reported the expected state.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// MQTT broker host.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// MQTT broker port.
    #[arg(long, default_value_t = 1883)]
    port: u16,

    /// MQTT username (omit for no auth).
    #[arg(long)]
    user: Option<String>,

    /// MQTT password (omit for no auth).
    #[arg(long)]
    pass: Option<String>,

    /// zigbee2mqtt device friendly name.
    #[arg(long, default_value = "garden_water_01")]
    device: String,

    /// JSON payload to publish to the device's /set topic.
    #[arg(long, default_value = r#"{"state":"ON"}"#)]
    payload: String,

    /// State field to check in the device's echo.
    #[arg(long, default_value = "state")]
    expect_key: String,

    /// Expected value for that field.
    #[arg(long, default_value = "ON")]
    expect_value: String,

    /// Maximum number of publish attempts.
    #[arg(long, default_value_t = 3)]
    max_attempts: u32,

    /// Seconds to wait for confirmation before re-publishing.
    #[arg(long, default_value_t = 10)]
    ack_timeout_s: u64,
}

/// Close the connection cleanly without blocking forever: send DISCONNECT, then
/// drain a bounded number of events so it flushes, then return.
fn shutdown(client: &Client, conn: &mut Connection) {
    client.disconnect().ok();
    for event in conn.iter().take(10) {
        if let Err(_) | Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect)) = event {
            break;
        }
    }
}

fn main() {
    let args = Args::parse();

    let set_topic = format!("zigbee2mqtt/{}/set", args.device);
    let state_topic = format!("zigbee2mqtt/{}", args.device);
    let ack_timeout = Duration::from_secs(args.ack_timeout_s);

    let mut opts = MqttOptions::new(
        format!("water-on-{}", std::process::id()),
        &args.host,
        args.port,
    );
    // Keep-alive doubles as our wakeup tick so the retry deadline is checked
    // even when no other packets arrive; keep it well under ack_timeout.
    opts.set_keep_alive(Duration::from_secs(2));
    if let (Some(u), Some(p)) = (&args.user, &args.pass) {
        opts.set_credentials(u.clone(), p.clone());
    }

    let (client, mut conn) = Client::new(opts, 10);

    // Subscribe to the state topic first so we can't miss the echo that follows
    // our publish.
    if let Err(e) = client.subscribe(&state_topic, QoS::AtLeastOnce) {
        eprintln!("subscribe failed: {e}");
        exit(1);
    }

    // What "confirmed" looks like in the JSON z2m publishes back, e.g. "state":"ON".
    let needle = format!("\"{}\":\"{}\"", args.expect_key, args.expect_value);

    let mut attempt = 0;
    let mut deadline = Instant::now(); // forces an immediate first publish
    let mut subscribed = false;

    for event in conn.iter() {
        let event = match event {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!("connection error: {e}");
                exit(1);
            }
        };

        match event {
            // Subscription confirmed — safe to start publishing.
            Event::Incoming(Packet::SubAck(_)) => subscribed = true,

            // The state echo we're waiting for.
            Event::Incoming(Packet::Publish(p)) if p.topic == state_topic => {
                let body = String::from_utf8_lossy(&p.payload);
                if body.contains(&needle) {
                    println!("confirmed: {} reported {needle}", args.device);
                    shutdown(&client, &mut conn);
                    exit(0);
                }
            }
            _ => {}
        }

        // (Re)publish when we're subscribed and the current attempt has timed
        // out. The keep-alive ping guarantees we reach this check periodically
        // even if no device traffic arrives.
        if subscribed && Instant::now() >= deadline {
            attempt += 1;
            if attempt > args.max_attempts {
                eprintln!(
                    "gave up after {} attempts: never saw {needle} on {state_topic}",
                    args.max_attempts
                );
                shutdown(&client, &mut conn);
                exit(1);
            }
            eprintln!(
                "attempt {attempt}/{}: publishing {} -> {set_topic}",
                args.max_attempts, args.payload
            );
            if let Err(e) =
                client.publish(&set_topic, QoS::AtLeastOnce, false, args.payload.as_bytes())
            {
                eprintln!("publish failed: {e}");
            }
            deadline = Instant::now() + ack_timeout;
        }
    }
}
