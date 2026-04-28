# Workspace Architecture

The repository is now reduced to one product: Binance Futures anomaly monitoring with Telegram alerts.

## Runtime Services

- `redis`: shared runtime cache and cooldown state.
- `streamer`: collects Binance Futures ticker and mark-price websocket data into Redis.
- `alert_monitor`: evaluates price/volume/OI/order-book/liquidation factors and sends Telegram alerts.
- `accumulation_scanner`: optional scheduled Binance Futures accumulation-pool scan with Telegram summary.

## Code Layout

- [`apps/market_monitor/backend`](/E:/cursor/crypto-monitor/apps/market_monitor/backend): alert monitor, data feed, rules, factors, scoring, and backtest utilities.
- [`apps/market_monitor/config`](/E:/cursor/crypto-monitor/apps/market_monitor/config): alert runtime configuration.
- [`packages/notifier`](/E:/cursor/crypto-monitor/packages/notifier): shared Telegram sender.
- [`utils`](/E:/cursor/crypto-monitor/utils): legacy compatibility shim for Telegram imports.
- [`data/runtime/market_monitor`](/E:/cursor/crypto-monitor/data/runtime/market_monitor): generated runtime state.
- [`data/reports/market_monitor`](/E:/cursor/crypto-monitor/data/reports/market_monitor): generated research/backtest reports.
