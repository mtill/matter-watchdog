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

  tcp = config["tcp"]
  return {
      "websocket_url": config["websocket_url"],
      "target_nodes": frozenset(config["target_nodes"]),
      "alert_timeout_seconds": config["alert_timeout_seconds"],
      "tcp_host": tcp["host"],
      "tcp_port": tcp["port"],
      "tcp_payload": tcp["payload"],
      "tcp_node_payloads": tcp.get("node_payloads", {}),
      "max_tcp_sends": tcp["max_tcp_sends"],
  }


CONFIG = load_config()
active_timers: dict[int, asyncio.Task[None]] = {}
START_LISTENING_MESSAGE = json.dumps(
    {"message_id": "1", "command": "start_listening"}
)
TCP_REPEAT_INTERVAL_SECONDS = 60


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
    await asyncio.sleep(CONFIG["alert_timeout_seconds"])
    logger.warning(
        f"🚨 ALERT: sensor (Node {node_id}) has been OPEN for"
        f" {CONFIG['alert_timeout_seconds']:g} seconds!"
    )
    max_tcp_sends = CONFIG["max_tcp_sends"]
    for attempt in range(1, max_tcp_sends + 1):
      try:
        await asyncio.to_thread(send_tcp_payload, node_id)
        logger.info(
            f"TCP payload sent for Node {node_id} "
            f"(attempt {attempt}/{max_tcp_sends})."
        )
      except RuntimeError as e:
        logger.error(
            f"Failed to send TCP payload for Node {node_id} "
            f"(attempt {attempt}/{max_tcp_sends}): {e}"
        )

      if attempt < max_tcp_sends:
        await asyncio.sleep(TCP_REPEAT_INTERVAL_SECONDS)
  except (OSError, RuntimeError) as e:
    logger.error(f"Failed to send alert for Node {node_id}: {e}")
  except asyncio.CancelledError:
    logger.info(f"✅ Timer cancelled for Node {node_id} (Sensor closed).")


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
              logger.debug(f"Ignoring message for Node {node_id} with state {state}.")
              continue

            if state:
              timer = active_timers.pop(node_id, None)
              if timer is not None:
                timer.cancel()
                logger.info(f"Sensor on Node {node_id} closed.")
            else:
              if node_id not in active_timers:
                logger.info(f"Sensor on Node {node_id} opened.")
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
