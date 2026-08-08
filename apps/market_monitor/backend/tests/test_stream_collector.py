from __future__ import annotations

import json
import unittest
from unittest.mock import patch

try:
    from apps.market_monitor.backend import stream_collector
except ModuleNotFoundError:
    import stream_collector

process_message = stream_collector.process_message


class _FakePipeline:
    def __init__(self) -> None:
        self.operations = []
        self.executed = False

    def hset(self, key, mapping):
        self.operations.append((key, mapping))
        return self

    async def execute(self):
        self.executed = True
        return []


class _FakeRedis:
    def __init__(self) -> None:
        self.pipelines = []

    async def mget(self, keys):
        if keys and str(keys[0]).startswith("supply:"):
            return ["1000" for _ in keys]
        return ["2000" for _ in keys]

    def pipeline(self):
        pipeline = _FakePipeline()
        self.pipelines.append(pipeline)
        return pipeline


class _ControlOnlyWebSocket:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def recv(self):
        return json.dumps({"result": None, "id": 1})


class StreamCollectorMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_ticker_message_updates_market_data(self) -> None:
        client = _FakeRedis()
        message = json.dumps(
            {
                "stream": "!ticker@arr",
                "data": [
                    {
                        "s": "BTCUSDT",
                        "c": "100.0",
                        "P": "5.5",
                        "q": "1200000",
                        "E": 1_786_176_000_000,
                    }
                ],
            }
        )

        processed = await process_message(client, message)

        self.assertTrue(processed)
        self.assertEqual(len(client.pipelines), 1)
        key, mapping = client.pipelines[0].operations[0]
        self.assertEqual(key, "market_data:BTCUSDT")
        self.assertEqual(mapping["price"], 100.0)
        self.assertEqual(mapping["mc"], 100_000.0)
        self.assertEqual(mapping["fdv"], 200_000.0)
        self.assertTrue(client.pipelines[0].executed)

    async def test_mark_price_message_updates_funding(self) -> None:
        client = _FakeRedis()
        message = json.dumps(
            {
                "stream": "!markPrice@arr",
                "data": [{"s": "BTCUSDT", "r": "0.0001"}],
            }
        )

        processed = await process_message(client, message)

        self.assertTrue(processed)
        key, mapping = client.pipelines[0].operations[0]
        self.assertEqual(key, "market_data:BTCUSDT")
        self.assertEqual(mapping["funding_rate"], 0.0001)

    async def test_control_message_is_not_counted_as_market_data(self) -> None:
        client = _FakeRedis()

        processed = await process_message(client, json.dumps({"result": None, "id": 1}))

        self.assertFalse(processed)
        self.assertEqual(client.pipelines, [])

    async def test_control_messages_cannot_mask_missing_market_data(self) -> None:
        client = _FakeRedis()
        websocket = _ControlOnlyWebSocket()

        with patch.object(stream_collector, "NO_MESSAGE_RECONNECT_SEC", 0.01), patch.object(
            stream_collector.websockets,
            "connect",
            return_value=websocket,
        ):
            with self.assertRaises(stream_collector.StreamerDataStalled):
                await stream_collector._run_stream_loop_direct(client)


if __name__ == "__main__":
    unittest.main()
