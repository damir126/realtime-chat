"""Asynchronous multi-room chat server (one event loop, thousands of connections)."""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

from .protocol import ProtocolError, encode, read_message, send

log = logging.getLogger("chat.server")
NICK_RE = re.compile(r"^[A-Za-z0-9_\-]{2,20}$")
ROOM_RE = re.compile(r"^[a-z0-9_\-]{1,30}$")
HISTORY = 20             # messages replayed to someone who joins a room
RATE = (10, 5.0)         # at most 10 messages per 5 seconds per client

HELP = """commands:
  /join <room>        switch room (created automatically)
  /rooms              list rooms
  /who                users in your room
  /msg <nick> <text>  private message
  /nick <new>         change nickname
  /quit               disconnect"""


@dataclass(eq=False)
class Client:
    nick: str
    writer: asyncio.StreamWriter
    room: str = "lobby"
    sent: deque = field(default_factory=deque)  # timestamps for rate limiting

    def allowed(self) -> bool:
        now = time.monotonic()
        while self.sent and now - self.sent[0] > RATE[1]:
            self.sent.popleft()
        if len(self.sent) >= RATE[0]:
            return False
        self.sent.append(now)
        return True


class ChatServer:
    def __init__(self) -> None:
        self.clients: dict[str, Client] = {}  # nick -> client
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=HISTORY))

    # ---------- helpers ----------
    def members(self, room: str) -> list[Client]:
        return [c for c in self.clients.values() if c.room == room]

    async def broadcast(self, room: str, message: dict, exclude: Client | None = None) -> None:
        data = encode(message)
        for c in self.members(room):
            if c is not exclude:
                c.writer.write(data)
        # drain all writers concurrently; a dead client must not block the others
        await asyncio.gather(*(c.writer.drain() for c in self.members(room) if c is not exclude),
                             return_exceptions=True)

    async def system(self, client: Client, text: str, ok: bool = True) -> None:
        await send(client.writer, {"type": "system" if ok else "error", "text": text})

    # ---------- connection lifecycle ----------
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        client = None
        try:
            hello = await asyncio.wait_for(read_message(reader), timeout=10)
            if not hello or hello.get("type") != "hello":
                return
            nick = str(hello.get("nick", ""))
            if not NICK_RE.match(nick):
                await send(writer, {"type": "error", "text": "nick must be 2-20 letters, digits, _ or -"})
                return
            if nick in self.clients:
                await send(writer, {"type": "error", "text": f"nick '{nick}' is taken"})
                return

            client = Client(nick, writer)
            self.clients[nick] = client
            log.info("%s connected from %s", nick, peer)
            await send(writer, {"type": "welcome", "nick": nick, "room": client.room, "help": HELP})
            await self.enter_room(client, "lobby")

            while (msg := await read_message(reader)) is not None:
                await self.dispatch(client, msg)
        except (ProtocolError, asyncio.TimeoutError, ConnectionError) as e:
            log.info("dropping %s: %s", peer, e)
        finally:
            if client and self.clients.get(client.nick) is client:
                del self.clients[client.nick]
                await self.broadcast(client.room, {"type": "event", "event": "leave", "nick": client.nick, "room": client.room})
                log.info("%s disconnected", client.nick)
            writer.close()

    async def enter_room(self, client: Client, room: str) -> None:
        old = client.room
        client.room = room
        if old != room:
            await self.broadcast(old, {"type": "event", "event": "leave", "nick": client.nick, "room": old})
        for past in self.history[room]:
            await send(client.writer, {**past, "history": True})
        await self.broadcast(room, {"type": "event", "event": "join", "nick": client.nick, "room": room})

    # ---------- message handling ----------
    async def dispatch(self, client: Client, msg: dict) -> None:
        if msg["type"] != "msg":
            return await self.system(client, f"unknown message type {msg['type']!r}", ok=False)
        text = str(msg.get("text", "")).strip()
        if not text:
            return
        if not client.allowed():
            return await self.system(client, "slow down: too many messages", ok=False)
        if text.startswith("/"):
            return await self.command(client, text)
        out = {"type": "msg", "nick": client.nick, "room": client.room, "text": text[:2000], "ts": time.time()}
        self.history[client.room].append(out)
        await self.broadcast(client.room, out)

    async def command(self, client: Client, text: str) -> None:
        cmd, _, arg = text.partition(" ")
        arg = arg.strip()
        if cmd == "/join":
            room = arg.lower()
            if not ROOM_RE.match(room):
                return await self.system(client, "room name: lowercase letters, digits, _ or -", ok=False)
            await self.enter_room(client, room)
        elif cmd == "/rooms":
            rooms = defaultdict(int)
            for c in self.clients.values():
                rooms[c.room] += 1
            await self.system(client, "rooms: " + ", ".join(f"{r} ({n})" for r, n in sorted(rooms.items())))
        elif cmd == "/who":
            await self.system(client, f"in {client.room}: " + ", ".join(sorted(c.nick for c in self.members(client.room))))
        elif cmd == "/msg":
            to, _, body = arg.partition(" ")
            target = self.clients.get(to)
            if not target or not body:
                return await self.system(client, "usage: /msg <online nick> <text>", ok=False)
            pm = {"type": "private", "from": client.nick, "to": to, "text": body[:2000], "ts": time.time()}
            await send(target.writer, pm)
            if target is not client:
                await send(client.writer, pm)
        elif cmd == "/nick":
            if not NICK_RE.match(arg):
                return await self.system(client, "invalid nick", ok=False)
            if arg in self.clients:
                return await self.system(client, f"nick '{arg}' is taken", ok=False)
            old = client.nick
            del self.clients[old]
            client.nick = arg
            self.clients[arg] = client
            await self.broadcast(client.room, {"type": "event", "event": "rename", "nick": arg, "old": old, "room": client.room})
        elif cmd == "/help":
            await self.system(client, HELP)
        elif cmd == "/quit":
            client.writer.close()
        else:
            await self.system(client, f"unknown command {cmd}, try /help", ok=False)


async def serve(host: str = "127.0.0.1", port: int = 9000) -> asyncio.Server:
    chat = ChatServer()
    server = await asyncio.start_server(chat.handle, host, port)
    server.chat = chat  # handy for tests
    return server


def main() -> None:
    p = argparse.ArgumentParser(description="TCP chat server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    async def run():
        server = await serve(args.host, args.port)
        log.info("listening on %s:%d", args.host, args.port)
        async with server:
            await server.serve_forever()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
