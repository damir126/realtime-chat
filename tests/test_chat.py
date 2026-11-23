import asyncio
import struct
import unittest

from chat import server as srv
from chat.protocol import encode, read_message, send


class Bot:
    """Test client that collects everything it receives."""

    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    @classmethod
    async def connect(cls, port, nick):
        r, w = await asyncio.open_connection("127.0.0.1", port)
        bot = cls(r, w)
        await send(w, {"type": "hello", "nick": nick})
        return bot

    async def say(self, text):
        await send(self.writer, {"type": "msg", "text": text})

    async def next(self, type_=None, timeout=2):
        """Next message (optionally of a given type), skipping others."""
        while True:
            msg = await asyncio.wait_for(read_message(self.reader), timeout)
            if msg is None:
                return None
            if type_ is None or msg["type"] == type_:
                return msg

    def close(self):
        self.writer.close()


class ChatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = await srv.serve("127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.bots = []

    async def asyncTearDown(self):
        for b in self.bots:
            b.close()
        self.server.close()
        await self.server.wait_closed()

    async def bot(self, nick):
        b = await Bot.connect(self.port, nick)
        self.bots.append(b)
        welcome = await b.next()
        return b, welcome

    async def test_framing_handles_split_and_merged_packets(self):
        reader = asyncio.StreamReader()
        data = encode({"type": "a"}) + encode({"type": "b", "text": "salom, dunyo ✓"})
        reader.feed_data(data[:3])            # header split in the middle
        reader.feed_data(data[3:])            # rest + a second message in one chunk
        reader.feed_eof()
        self.assertEqual(await read_message(reader), {"type": "a"})
        self.assertEqual((await read_message(reader))["text"], "salom, dunyo ✓")
        self.assertIsNone(await read_message(reader))

    async def test_welcome_and_broadcast(self):
        ali, welcome = await self.bot("ali")
        self.assertEqual(welcome["type"], "welcome")
        vali, _ = await self.bot("vali")
        join = await ali.next("event")
        self.assertEqual((join["event"], join["nick"]), ("join", "ali"))  # own join event
        join = await ali.next("event")
        self.assertEqual(join["nick"], "vali")

        await vali.say("salom")
        msg = await ali.next("msg")
        self.assertEqual((msg["nick"], msg["text"]), ("vali", "salom"))

    async def test_rooms_are_isolated(self):
        ali, _ = await self.bot("ali")
        vali, _ = await self.bot("vali")
        await vali.say("/join games")
        while (await vali.next("event"))["room"] != "games":
            pass  # wait until the server has moved vali
        await ali.say("only lobby sees this")
        await vali.say("/who")
        who = await vali.next("system")
        self.assertEqual(who["text"], "in games: vali")
        # vali must not have received ali's lobby message after leaving
        await ali.say("/join games")
        await ali.say("hi games")
        msg = await vali.next("msg")
        self.assertEqual(msg["text"], "hi games")

    async def test_history_is_replayed(self):
        ali, _ = await self.bot("ali")
        await ali.say("first")
        await ali.next("msg")
        vali, _ = await self.bot("vali")
        past = await vali.next("msg")
        self.assertTrue(past["history"])
        self.assertEqual(past["text"], "first")

    async def test_private_message(self):
        ali, _ = await self.bot("ali")
        vali, _ = await self.bot("vali")
        await ali.say("/msg vali secret")
        pm = await vali.next("private")
        self.assertEqual((pm["from"], pm["text"]), ("ali", "secret"))

    async def test_duplicate_nick_rejected(self):
        await self.bot("ali")
        _, reply = await self.bot("ali")
        self.assertEqual(reply["type"], "error")

    async def test_rate_limit(self):
        ali, _ = await self.bot("ali")
        for i in range(srv.RATE[0] + 1):
            await ali.say(f"spam {i}")
        err = await ali.next("error")
        self.assertIn("slow down", err["text"])

    async def test_oversized_frame_drops_connection(self):
        r, w = await asyncio.open_connection("127.0.0.1", self.port)
        w.write(struct.pack("!I", 10_000_000))
        await w.drain()
        self.assertIsNone(await asyncio.wait_for(read_message(r), 2))
        w.close()


if __name__ == "__main__":
    unittest.main()
