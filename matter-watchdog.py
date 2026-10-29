#!/usr/bin/env python3
# coding=utf-8


import asyncio
import json
import logging
from pathlib import Path
import socket
import websockets

CONFIG_PATH = Path(__file__).with_name("config.json")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("matter-watchdog")


def load_config() -> dict:
  try:
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
      config = json.load(config_file)
  except (OSError, json.JSONDecodeError) as e:
    raise RuntimeError(f"Unable to load configuration from {CONFIG_PATH}: {e}") from e

  websocket_url = config.get("websocket_url")
  if websocket_url is None:
    raise RuntimeError("Configuration is missing websocket_url.")

  nodes = config.get("nodes")
  if not isinstance(nodes, dict):
    raise RuntimeError("Configuration is missing a valid 'nodes' object.")

  default_payload = config.get("default_payload")
  default_max_tcp_sends = config.get("default_max_tcp_sends")
  default_alert_timeout = config.get("default_alert_timeout_seconds", 45)
  tcp_host = config.get("buzzer_host")
  tcp_port = config.get("buzzer_port")

  if default_payload is None or default_max_tcp_sends is None:
    raise RuntimeError("Configuration is missing the default TCP payload or send count.")
  if tcp_host is None or tcp_port is None:
    raise RuntimeError("Configuration is missing the buzzer/TCP host or port.")

  target_nodes: dict[int, str] = {}
  tcp_node_payloads: dict[str, str] = {}
  node_max_tcp_sends: dict[int, int] = {}
  node_alert_timeouts: dict[int, int] = {}

  for raw_node_id, node_config in nodes.items():
    node_id = int(raw_node_id)
    node_dict = node_config if isinstance(node_config, dict) else {}
    node_name = node_dict.get("name", "unnamed")
    if not isinstance(node_name, str):
      raise RuntimeError(f"Node {node_id} has an invalid name.")
    target_nodes[node_id] = node_name

    payload = node_dict.get("payload", default_payload)
    max_sends = node_dict.get("max_tcp_sends", default_max_tcp_sends)
    alert_timeout = node_dict.get("alert_timeout_seconds", default_alert_timeout)

    if payload is None:
      raise RuntimeError(
          f"Node {node_id} ({node_name}) is missing payload configuration."
      )

    tcp_node_payloads[str(node_id)] = payload
    node_max_tcp_sends[node_id] = int(max_sends)
    node_alert_timeouts[node_id] = int(alert_timeout)

  return {
      "websocket_url": websocket_url,
      "target_nodes": target_nodes,
      "default_alert_timeout_seconds": int(default_alert_timeout),
      "alert_timeout_seconds_by_node": {
          int(node_id): timeout for node_id, timeout in node_alert_timeouts.items()
      },
      "tcp_host": tcp_host,
      "tcp_port": tcp_port,
      "tcp_payload": default_payload,
      "tcp_node_payloads": tcp_node_payloads,
      "max_tcp_sends": int(default_max_tcp_sends),
      "max_tcp_sends_by_node": {
          int(node_id): max_sends for node_id, max_sends in node_max_tcp_sends.items()
      },
  }


def get_alert_timeout_seconds(node_id: int) -> int:
  return CONFIG["alert_timeout_seconds_by_node"].get(
      node_id, CONFIG["default_alert_timeout_seconds"]
  )


def get_max_tcp_sends(node_id: int) -> int:
  return CONFIG["max_tcp_sends_by_node"].get(node_id, CONFIG["max_tcp_sends"])


CONFIG = load_config()
active_timers: dict[int, asyncio.Task[None]] = {}
START_LISTENING_MESSAGE = json.dumps(
    {"message_id": "1", "command": "start_listening"}
)
TCP_REPEAT_INTERVAL_SECONDS = 60


def format_node(node_id: int) -> str:
  node_name = CONFIG["target_nodes"].get(node_id, "unknown")
  return f"Node {node_id} ({node_name})"


def get_sensor_frame(data: dict) -> tuple[object, object]:
  payload = data.get("data")
  node_id = payload[0] if isinstance(payload, list) and payload else None
  state = data.get("value")

  if isinstance(payload, dict):
    node_id = payload.get("node_id", node_id)
    state = payload.get("value", state)
  elif isinstance(payload, list):
    for item in payload[1:]:
      if isinstance(item, dict):
        state = item.get("value", state)
      else:
        if isinstance(item, bool):
          state = item

  return node_id, state


def send_tcp_payload(node_id: int) -> None:
  try:
    with socket.create_connection(
        (CONFIG["tcp_host"], CONFIG["tcp_port"]), timeout=15
    ) as connection:
      payload = CONFIG["tcp_node_payloads"].get(str(node_id), CONFIG["tcp_payload"])
      connection.sendall(payload.encode("ascii") + b"\n")
  except OSError as e:
    raise RuntimeError(f"TCP request failed: {e}") from e


async def trigger_alert(node_id: int) -> None:
  try:
    alert_timeout_seconds = get_alert_timeout_seconds(node_id)
    max_tcp_sends = get_max_tcp_sends(node_id)
    await asyncio.sleep(alert_timeout_seconds)
    logger.warning(
        f"🚨 ALERT: sensor ({format_node(node_id)}) has been OPEN for"
        f" {alert_timeout_seconds:g} seconds!"
    )
    for attempt in range(1, max_tcp_sends + 1):
      try:
        await asyncio.to_thread(send_tcp_payload, node_id)
        logger.info(
            f"TCP payload sent for {format_node(node_id)} "
            f"(attempt {attempt}/{max_tcp_sends})."
        )
      except RuntimeError as e:
        logger.error(
            f"Failed to send TCP payload for {format_node(node_id)} "
            f"(attempt {attempt}/{max_tcp_sends}): {e}"
        )

      if attempt < max_tcp_sends:
        await asyncio.sleep(TCP_REPEAT_INTERVAL_SECONDS)
  except (OSError, RuntimeError) as e:
    logger.error(f"Failed to send alert for {format_node(node_id)}: {e}")
  except asyncio.CancelledError:
    logger.info(f"✅ Timer cancelled for {format_node(node_id)} (Sensor closed).")


async def main() -> None:
  try:
    while True:
      try:
        logger.info(f"Connecting to matterjs-server at {CONFIG['websocket_url']}...")
        async with websockets.connect(
            CONFIG["websocket_url"], ping_interval=20, ping_timeout=20
        ) as websocket:
          logger.info("Connected! Sending start_listening.")
          await websocket.send(START_LISTENING_MESSAGE)

          async for message in websocket:
            try:
              data = json.loads(message)
            except json.JSONDecodeError:
              logger.debug("Ignoring invalid JSON message.")
              continue

            if not isinstance(data, dict):
              continue
            node_id, state = get_sensor_frame(data)
            if node_id not in CONFIG["target_nodes"] or not isinstance(state, bool):
              logger.debug(
                  f"Ignoring message for {format_node(node_id)} with state {state}."
              )
              continue

            if state:
              timer = active_timers.pop(node_id, None)
              if timer is not None:
                timer.cancel()
                logger.info(f"Sensor on {format_node(node_id)} closed.")
            else:
              if node_id not in active_timers:
                logger.info(f"Sensor on {format_node(node_id)} opened.")
                active_timers[node_id] = asyncio.create_task(
                    trigger_alert(node_id)
                )

      except (
          websockets.ConnectionClosed,
          ConnectionRefusedError,
          asyncio.TimeoutError,
      ) as e:
        logger.warning(f"Connection dropped ({e}). Reconnecting in 3 seconds...")
      except Exception as e:
        logger.error(f"Unexpected error: {e}. Reconnecting in 3 seconds...")
      await asyncio.sleep(3)
  finally:
    pending_timers = tuple(active_timers.values())
    active_timers.clear()
    for timer in pending_timers:
      timer.cancel()
    if pending_timers:
      await asyncio.gather(*pending_timers, return_exceptions=True)


if __name__ == "__main__":
  try:
    asyncio.run(main())
  except KeyboardInterrupt:
    logger.info("Stopped.")
