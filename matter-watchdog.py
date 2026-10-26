#!/usr/bin/env python3
# coding=utf-8


import asyncio
import base64
import json
import logging
from pathlib import Path
import urllib.request
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

  if not isinstance(config, dict):
    raise RuntimeError(f"Configuration in {CONFIG_PATH} must be a JSON object.")

  try:
    websocket_url = config["websocket_url"]
    target_nodes = config["target_nodes"]
    alert_timeout = config["alert_timeout_seconds"]
    ntfy = config["ntfy"]
    ntfy_url = ntfy["url"]
    ntfy_username = ntfy["username"]
    ntfy_password = ntfy["password"]
  except (KeyError, TypeError) as e:
    raise RuntimeError(f"Missing or invalid configuration setting: {e}") from e

  if not isinstance(websocket_url, str) or not websocket_url:
    raise RuntimeError("'websocket_url' must be a non-empty string.")
  if (
      not isinstance(target_nodes, list)
      or not target_nodes
      or any(not isinstance(node, int) or isinstance(node, bool) for node in target_nodes)
  ):
    raise RuntimeError("'target_nodes' must be a non-empty list of integers.")
  if (
      not isinstance(alert_timeout, (int, float))
      or isinstance(alert_timeout, bool)
      or alert_timeout <= 0
  ):
    raise RuntimeError("'alert_timeout_seconds' must be a positive number.")
  if not isinstance(ntfy_url, str) or not ntfy_url:
    raise RuntimeError("'ntfy.url' must be a non-empty string.")
  if not isinstance(ntfy_username, str) or not ntfy_username:
    raise RuntimeError("'ntfy.username' must be a non-empty string.")
  if not isinstance(ntfy_password, str) or not ntfy_password:
    raise RuntimeError("'ntfy.password' must be a non-empty string.")

  return {
      "websocket_url": websocket_url,
      "target_nodes": frozenset(target_nodes),
      "alert_timeout_seconds": alert_timeout,
      "ntfy_url": ntfy_url,
      "ntfy_username": ntfy_username,
      "ntfy_password": ntfy_password,
  }


CONFIG = load_config()
active_timers: dict[int, asyncio.Task[None]] = {}
START_LISTENING_MESSAGE = json.dumps(
    {"message_id": "1", "command": "start_listening"}
)


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


def send_ntfy_message(node_id: int) -> None:
  credentials = base64.b64encode(
      f"{CONFIG['ntfy_username']}:{CONFIG['ntfy_password']}".encode("utf-8")
  ).decode("ascii")
  request = urllib.request.Request(
      CONFIG["ntfy_url"],
      data=f"sensor (Node {node_id}) has been OPEN for "
           f"{CONFIG['alert_timeout_seconds']:g} seconds.".encode("utf-8"),
      headers={
          "Authorization": f"Basic {credentials}",
          "Title": "Sensor left open",
          "Content-Type": "text/plain; charset=utf-8",
      },
      method="POST",
  )

  with urllib.request.urlopen(request, timeout=15) as response:
    if not 200 <= response.status < 300:
      raise RuntimeError(f"ntfy returned HTTP {response.status}")


async def trigger_alert(node_id: int) -> None:
  try:
    await asyncio.sleep(CONFIG["alert_timeout_seconds"])
    logger.warning(
        f"🚨 ALERT: sensor (Node {node_id}) has been OPEN for"
        f" {CONFIG['alert_timeout_seconds']:g} seconds!"
    )
    await asyncio.to_thread(send_ntfy_message, node_id)
    logger.info(f"ntfy alert sent for Node {node_id}.")
  except (OSError, RuntimeError) as e:
    logger.error(f"Failed to send ntfy alert for Node {node_id}: {e}")
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
