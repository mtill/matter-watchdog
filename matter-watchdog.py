#!/usr/bin/env python3
# coding=utf-8


import asyncio
from datetime import datetime, time, timedelta
import json
import logging
from pathlib import Path
import socket
import time
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
  default_tcp_repeat_interval = config.get("default_tcp_repeat_interval_seconds", 60)
  alarm_cancel_window = config.get("alarm_cancel_window_seconds", 2)
  tcp_host = config.get("buzzer_host")
  tcp_port = config.get("buzzer_port")

  if default_payload is None or default_max_tcp_sends is None:
    raise RuntimeError("Configuration is missing the default TCP payload or send count.")
  if tcp_host is None or tcp_port is None:
    raise RuntimeError("Configuration is missing the buzzer/TCP host or port.")
  if (
      isinstance(default_tcp_repeat_interval, bool)
      or not isinstance(default_tcp_repeat_interval, (int, float))
      or default_tcp_repeat_interval < 0
  ):
    raise RuntimeError(
        "Configuration has an invalid default_tcp_repeat_interval_seconds; "
        "expected a non-negative number."
    )
  if (
      isinstance(alarm_cancel_window, bool)
      or not isinstance(alarm_cancel_window, (int, float))
      or alarm_cancel_window < 0
  ):
    raise RuntimeError(
        "Configuration has an invalid alarm_cancel_window_seconds; "
        "expected a non-negative number."
    )

  target_nodes: dict[int, str] = {}
  tcp_node_payloads: dict[str, str] = {}
  node_max_tcp_sends: dict[int, int] = {}
  node_alert_timeouts: dict[int, int] = {}
  node_tcp_repeat_intervals: dict[int, int | float] = {}
  no_alarm_between_by_node: dict[int, tuple[time, time]] = {}

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
    tcp_repeat_interval = node_dict.get(
        "tcp_repeat_interval_seconds", default_tcp_repeat_interval
    )
    no_alarm_between = node_dict.get("no_alarm_between")

    if (
        isinstance(tcp_repeat_interval, bool)
        or not isinstance(tcp_repeat_interval, (int, float))
        or tcp_repeat_interval < 0
    ):
      raise RuntimeError(
          f"Node {node_id} ({node_name}) has an invalid "
          "tcp_repeat_interval_seconds; expected a non-negative number."
      )

    if payload is None:
      raise RuntimeError(
          f"Node {node_id} ({node_name}) is missing payload configuration."
      )

    if no_alarm_between is not None:
      try:
        if (
            not isinstance(no_alarm_between, list)
            or len(no_alarm_between) != 2
            or not all(isinstance(value, str) for value in no_alarm_between)
        ):
          raise ValueError("expected a list containing two HH:MM strings")
        quiet_start, quiet_end = (
            datetime.strptime(value, "%H:%M").time()
            for value in no_alarm_between
        )
        if any(
            parsed.strftime("%H:%M") != value
            for parsed, value in zip(
                (quiet_start, quiet_end), no_alarm_between
            )
        ):
          raise ValueError("times must use the HH:MM format")
        if quiet_start == quiet_end:
          raise ValueError("start and end times must be different")
      except ValueError as e:
        raise RuntimeError(
            f"Node {node_id} ({node_name}) has invalid no_alarm_between; "
            'expected ["HH:MM", "HH:MM"] with different times.'
        ) from e
      no_alarm_between_by_node[node_id] = (quiet_start, quiet_end)

    tcp_node_payloads[str(node_id)] = payload
    node_max_tcp_sends[node_id] = int(max_sends)
    node_alert_timeouts[node_id] = int(alert_timeout)
    node_tcp_repeat_intervals[node_id] = tcp_repeat_interval

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
      "no_alarm_between_by_node": no_alarm_between_by_node,
      "default_tcp_repeat_interval_seconds": default_tcp_repeat_interval,
      "tcp_repeat_interval_seconds_by_node": node_tcp_repeat_intervals,
      "alarm_cancel_window_seconds": alarm_cancel_window,
  }


def get_alert_timeout_seconds(node_id: int) -> int:
  return CONFIG["alert_timeout_seconds_by_node"].get(
      node_id, CONFIG["default_alert_timeout_seconds"]
  )


def get_max_tcp_sends(node_id: int) -> int:
  return CONFIG["max_tcp_sends_by_node"].get(node_id, CONFIG["max_tcp_sends"])


def get_tcp_repeat_interval_seconds(node_id: int) -> int | float:
  return CONFIG["tcp_repeat_interval_seconds_by_node"].get(
      node_id, CONFIG["default_tcp_repeat_interval_seconds"]
  )


def seconds_until_alarm_allowed(node_id: int, now: datetime | None = None) -> float:
  quiet_period = CONFIG["no_alarm_between_by_node"].get(node_id)
  if quiet_period is None:
    return 0

  current_time = now or datetime.now()
  quiet_start, quiet_end = quiet_period
  current_clock = current_time.time()
  crosses_midnight = quiet_start > quiet_end
  is_quiet = (
      quiet_start <= current_clock < quiet_end
      if not crosses_midnight
      else current_clock >= quiet_start or current_clock < quiet_end
  )
  if not is_quiet:
    return 0

  quiet_end_datetime = datetime.combine(current_time.date(), quiet_end)
  if crosses_midnight and current_clock >= quiet_start:
    quiet_end_datetime += timedelta(days=1)
  return max(0, (quiet_end_datetime - current_time).total_seconds())


async def wait_for_alarm_allowed(node_id: int) -> None:
  while True:
    remaining = seconds_until_alarm_allowed(node_id)
    if remaining <= 0:
      return
    logger.info(
        f"Alarm for {format_node(node_id)} is paused until its quiet hours end."
    )
    await asyncio.sleep(remaining)


CONFIG = load_config()
active_timers: dict[int, asyncio.Task[None]] = {}
sensor_states: dict[int, bool] = {}
sensor_opened_at: dict[int, float] = {}
START_LISTENING_MESSAGE = json.dumps(
    {"message_id": "1", "command": "start_listening"}
)


def is_alarm_cancel_gesture(node_id: int, state: bool) -> bool:
  now = time.monotonic()
  previous_state = sensor_states.get(node_id)
  sensor_states[node_id] = state

  if not state:
    if previous_state is not False:
      sensor_opened_at[node_id] = now
    return False

  opened_at = sensor_opened_at.pop(node_id, None)
  return (
      opened_at is not None
      and now - opened_at <= CONFIG["alarm_cancel_window_seconds"]
  )


def cancel_all_active_alarms() -> None:
  pending_timers = tuple(active_timers.values())
  active_timers.clear()
  for timer in pending_timers:
    timer.cancel()


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
    await wait_for_alarm_allowed(node_id)
    logger.warning(
        f"🚨 ALERT: sensor ({format_node(node_id)}) has been OPEN for"
        f" {alert_timeout_seconds:g} seconds!"
    )
    for attempt in range(1, max_tcp_sends + 1):
      await wait_for_alarm_allowed(node_id)
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
        await asyncio.sleep(get_tcp_repeat_interval_seconds(node_id))
  except (OSError, RuntimeError) as e:
    logger.error(f"Failed to send alert for {format_node(node_id)}: {e}")
  except asyncio.CancelledError:
    logger.info(f"✅ Timer cancelled for {format_node(node_id)}.")


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

            previous_state = sensor_states.get(node_id)
            cancel_gesture = is_alarm_cancel_gesture(node_id, state)
            if state:
              timer = active_timers.pop(node_id, None)
              if timer is not None:
                timer.cancel()
                logger.info(f"Sensor on {format_node(node_id)} closed.")
            else:
              if previous_state is not False and node_id not in active_timers:
                logger.info(f"Sensor on {format_node(node_id)} opened.")
                active_timers[node_id] = asyncio.create_task(
                    trigger_alert(node_id)
                )

            if cancel_gesture:
              logger.info(
                  f"Open/close within {CONFIG['alarm_cancel_window_seconds']} seconds "
                  f"detected on {format_node(node_id)}; "
                  "cancelling all active alarms."
              )
              cancel_all_active_alarms()

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
