"""Wire protocol.

TCP is a byte stream, not a message stream: one send() can arrive as two recv()s,
or two sends as one recv(). So every message is framed:

    +----------------+---------------------------+
    | length: 4 bytes| payload: UTF-8 JSON object |
    | big-endian u32 |                           |
    +----------------+---------------------------+

Payload examples:
    {"type": "hello", "nick": "ali"}
    {"type": "msg", "text": "salom"}
    {"type": "event", "event": "join", "nick": "ali", "room": "lobby"}
"""

from __future__ import annotations

import asyncio
import json
import struct

HEADER = struct.Struct("!I")
MAX_MESSAGE = 64 * 1024  # protects the server from huge allocations


class ProtocolError(Exception):
    pass


def encode(message: dict) -> bytes:
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
    if len(payload) > MAX_MESSAGE:
        raise ProtocolError("message too large")
    return HEADER.pack(len(payload)) + payload


async def read_message(reader: asyncio.StreamReader) -> dict | None:
    """Reads one framed message. Returns None when the peer closed the connection."""
    try:
        header = await reader.readexactly(HEADER.size)
    except asyncio.IncompleteReadError:
        return None
    (length,) = HEADER.unpack(header)
    if length > MAX_MESSAGE:
        raise ProtocolError(f"message of {length} bytes exceeds limit")
    try:
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        return None
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError("invalid payload") from e
    if not isinstance(message, dict) or "type" not in message:
        raise ProtocolError("payload must be an object with a 'type'")
    return message


async def send(writer: asyncio.StreamWriter, message: dict) -> None:
    writer.write(encode(message))
    await writer.drain()  # back-pressure: wait if the client reads slowly
