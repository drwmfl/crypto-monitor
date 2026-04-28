# Market Monitor

This application now keeps only the Binance Futures anomaly monitor and Telegram alerting stack.

- `backend/alert_monitor.py`: main alert loop and Telegram push entrypoint.
- `backend/stream_collector.py`: Binance Futures websocket stream collector for Redis-backed realtime data.
- `backend/strategy_pipeline.py`: candidate scoring, risk control, and alert policy pipeline.
- `backend/factors/`: Binance derivatives, order book, liquidation, microstructure, and accumulation-pool factors.
- `backend/alerts/`: alert policy and Telegram message formatting.
- `config/config.json`: runtime alert configuration.

Generated runtime state lives in [`data/runtime/market_monitor`](/E:/cursor/crypto-monitor/data/runtime/market_monitor).
Historical research exports live in [`data/reports/market_monitor`](/E:/cursor/crypto-monitor/data/reports/market_monitor).
