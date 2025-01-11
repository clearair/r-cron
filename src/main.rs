use anyhow::Result;
use chrono::Local;
use tokio::{fs::File, process::Command, sync::Mutex};
use tokio_cron_scheduler::{JobScheduler, Job};
use serde::{Serialize, Deserialize};
use std::{fs::File as StdFile, process, str::FromStr, sync::Arc, time::Duration};
use tokio::signal::unix::{signal, SignalKind}; // 用于捕获信号
use log::{debug, info, warn};
use env_logger::{Builder, Env};
use std::io::{Write};
use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    config_file: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Conf {
    timezone: String,
    comms: Vec<Comm>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Comm {
    command: String,
    args: String,
    cron: String,
}

// static CONFIG_STR: &'static str = include_str!("../config.yaml"); 

async fn run() -> anyhow::Result<()> {
    Builder::from_env(Env::default().default_filter_or("info"))
        .format(|buf, record| {
            let current_time = Local::now().format("%Y-%m-%d %H:%M:%S");
            writeln!(buf, "{} - {} - {} - {}", current_time, record.level(), record.target(), record.args())
        })
        .init();

    // Parse command-line arguments
    let args = Args::parse();
    let file = StdFile::open(args.config_file)?;
    let config: Conf = serde_yaml::from_reader(file)?;

    let sched = Arc::new(Mutex::new(JobScheduler::new().await?));

    let tz: chrono_tz::Tz = chrono_tz::Tz::from_str(&config.timezone)?;

    for comm in config.comms {
        let cron = comm.cron.clone();

        sched.lock().await.add(
            Job::new_async_tz(cron, tz, move |_uuid, mut _l| {
                let command = comm.command.clone();
                let args = comm.args.clone();

                Box::pin(async move {
                    info!("执行任务: {} {}", command, args);

                    let output: Result<std::process::Output, std::io::Error> = Command::new(command)
                        .args(args.split(' ').collect::<Vec<_>>())
                        .output()
                        .await;

                    if let Ok(output) = output {
                        info!("{}", String::from_utf8_lossy(&output.stdout));
                        if !output.status.success() {
                            warn!("错误信息: {}", String::from_utf8_lossy(&output.stderr));
                        }
                    } else {
                        warn!("错误信息: {}", output.unwrap_err());
                    }
                })
            })?
        ).await?;
    }

    let mut shutdown_signal = signal(SignalKind::interrupt())?;

    let sched_clone = Arc::clone(&sched);
    let sched_handle = tokio::spawn(async move {
        sched_clone.lock().await.start().await.unwrap();
    });

    tokio::select! {
        _ = shutdown_signal.recv() => {
            info!("正在退出...");
            sched.lock().await.shutdown().await?;
        }
    }
    terminate_rcron_process().await?;
    sched_handle.await.unwrap();

    info!("退出完成.");

    Ok(())
}

// todo 不清楚为什么存在一个 r-cron 进程 所以退出时候 强制清除 r-cron 进程
async fn terminate_rcron_process() -> Result<(), std::io::Error> {
    let current_pid = process::id();  // 获取当前进程的PID

    let output = Command::new("pgrep")
        .arg("-f")
        .arg("r-cron")
        .output()
        .await?;

    let pid_list = String::from_utf8_lossy(&output.stdout);
    let pids = pid_list.split('\n').filter_map(|pid| pid.parse::<u32>().ok());

    for pid in pids {
        if pid == current_pid {
            continue;
        }
        Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .output()
            .await?;
    }
    Ok(())
}
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

