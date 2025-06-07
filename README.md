# r-cron

A simple, async cron job scheduler written in Rust, supporting YAML configuration, task deduplication, and graceful shutdown.

---

一个用 Rust 编写的简单异步定时任务调度器，支持 YAML 配置、任务去重和优雅关闭。

## Features | 功能特性

- Async cron scheduling (基于 tokio 的异步定时任务)
- YAML configuration (YAML 格式配置)
- Prevents duplicate task execution (防止同一任务重复执行)
- Graceful shutdown on signal (信号优雅关闭)
- Logging (日志输出)

## Quick Start | 快速开始

### 1. Build & Run | 构建与运行

```bash
cargo build --release
./target/release/r-cron --config config.yaml
```

### 2. Configuration | 配置文件

Edit `config.yaml` to define your scheduled jobs:

```yaml
timezone: Asia/Shanghai
comms:
  - cron: "0 * * * * * *"
    command: "echo"
    args: "Hello, world!"
```

- `timezone`: 时区设置 (如 Asia/Shanghai)
- `cron`: Cron 表达式 (支持秒级)
- `command`: 要执行的命令
- `args`: 命令参数

### 3. Logging | 日志

Logs are printed to stdout by default. 日志默认输出到标准输出。

## How It Works | 工作原理

- Loads tasks from YAML config at startup (启动时加载 YAML 配置)
- Uses `tokio-cron-scheduler` for async scheduling (用 tokio-cron-scheduler 异步调度)
- Uses a HashMap to prevent duplicate execution of the same task (用 HashMap 防止同一任务重复执行)
- Handles shutdown signals gracefully (优雅处理关闭信号)

## License | 许可协议

MIT

---

Questions or suggestions? PRs and issues welcome!
如有疑问或建议，欢迎提交 PR 或 Issue。
