"""Terminal client: python -m chat.client --nick ali"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime

from .protocol import read_message, send

COLORS = {"system": "\033[90m", "error": "\033[91m", "private": "\033[95m", "event": "\033[93m", "reset": "\033[0m"}


def render(msg: dict) -> str:
    t = msg.get("type")
    ts = datetime.fromtimestamp(msg["ts"]).strftime("%H:%M") if "ts" in msg else ""
    c = COLORS
    if t == "msg":
        prefix = "(history) " if msg.get("history") else ""
        return f"{c['system']}{ts}{c['reset']} {prefix}[{msg['room']}] {msg['nick']}: {msg['text']}"
    if t == "private":
        return f"{c['private']}{ts} [pm] {msg['from']} -> {msg['to']}: {msg['text']}{c['reset']}"
    if t == "event":
        if msg["event"] == "rename":
            return f"{c['event']}* {msg['old']} is now {msg['nick']}{c['reset']}"
        verb = "joined" if msg["event"] == "join" else "left"
        return f"{c['event']}* {msg['nick']} {verb} {msg['room']}{c['reset']}"
    if t == "welcome":
        return f"{c['system']}connected as {msg['nick']}\n{msg['help']}{c['reset']}"
    if t in ("system", "error"):
        return f"{c[t]}{msg['text']}{c['reset']}"
    return str(msg)


async def stdin_lines():
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)  # input() would block the event loop
        if not line:
            return
        yield line.rstrip("\n")


async def run(host: str, port: int, nick: str) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    await send(writer, {"type": "hello", "nick": nick})

    async def receive():
        while (msg := await read_message(reader)) is not None:
            print(render(msg), flush=True)
        print("disconnected by server")

    async def transmit():
        async for line in stdin_lines():
            if line.strip():
                await send(writer, {"type": "msg", "text": line})
            if line.strip() == "/quit":
                return

    recv_task = asyncio.create_task(receive())
    send_task = asyncio.create_task(transmit())
    await asyncio.wait({recv_task, send_task}, return_when=asyncio.FIRST_COMPLETED)
    writer.close()


def main() -> None:
    p = argparse.ArgumentParser(description="TCP chat client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--nick", required=True)
    args = p.parse_args()
    try:
        asyncio.run(run(args.host, args.port, args.nick))
    except (KeyboardInterrupt, ConnectionError) as e:
        if isinstance(e, ConnectionError):
            print(f"cannot connect: {e}")
    finally:
        # readline() in the executor thread cannot be cancelled; exit without waiting for it
        import os
        os._exit(0)


if __name__ == "__main__":
    main()
