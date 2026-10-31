# matter-watchdog

This listener connects to the Matter websocket server and watches the configured
sensor nodes. When a node reports an open state, it starts a timer. If the
sensor remains open for the configured duration, the listener sends the
configured TCP payload to the configured host and port. The TCP payload is sent
immediately and then every minute, up to the configured maximum number of
sends. A closed-state update cancels any remaining sends.

Opening and closing the same sensor within two seconds cancels all currently
active alarms.

## Configuration

Edit `config.json` alongside `matter-watchdog.py`:

```json
{
  "websocket_url": "ws://127.0.0.1:5580/ws",
  "default_payload": "3,1000,1000",
  "default_max_tcp_sends": 360,
  "buzzer_host": "10.139.139.74",
  "buzzer_port": 65432,
  "nodes": {
    "4": {
      "alert_timeout_seconds": 45,
      "payload": "3,200,500",
      "max_tcp_sends": 10
    },
    "9": {
      "payload": "1,1000,0"
    }
  }
}
```

`default_payload` is the fallback payload for every node. Each node entry may
optionally override `payload`, `alert_timeout_seconds`, and `max_tcp_sends`.
Nodes that omit a key inherit the default values. Each payload is sent as ASCII
followed by a newline, which terminates the receiver's line-based command.

The listener handles boolean state updates for the configured node IDs. It
assumes `false` means open and `true` means closed. Verify this mapping against
the events from your Matter server before relying on alerts.

## Run manually

The listener requires Python 3 and the `websockets` package:

```bash
python3 -m pip install websockets
python3 matter-watchdog.py
```

## Run as a user systemd service

The repository includes `matter-watchdog.service`, configured for this
checkout at `/home/beck/smarthome`. Install and enable it for the current user:

```bash
mkdir -p ~/.config/systemd/user
ln -s /home/beck/smarthome/matter-watchdog.service \
  ~/.config/systemd/user/matter-watchdog.service
systemctl --user daemon-reload
systemctl --user enable --now matter-watchdog.service
```

The service restarts after exit or failure, with a five-second delay. Useful
commands:

```bash
systemctl --user status matter-watchdog.service
systemctl --user restart matter-watchdog.service
systemctl --user stop matter-watchdog.service
journalctl --user -u matter-watchdog.service -f
```

For the user service to continue running after logout and start at boot without
a user login, enable lingering for the account (administrator privileges may
be required):

```bash
sudo loginctl enable-linger "$USER"
```
