use anyhow::Result;
use chrono::Local;
use tokio::{fs::File, io::{AsyncBufReadExt, BufReader}, process::Command, sync::{Mutex, Notify}};
use tokio_cron_scheduler::{JobScheduler, Job};
use serde::{Serialize, Deserialize};
use std::{f64::consts::E, fs::File as StdFile, process::{self, Stdio}, str::FromStr, sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::Duration};
use tokio::signal::unix::{signal, SignalKind}; // 用于捕获信号
use log::{debug, info, warn};
use env_logger::{Builder, Env};
use std::io::{Write};
use clap::Parser;
use std::error::Error;


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

static CONFIG_STR: &'static str = include_str!("../config.yaml"); 

async fn run() -> anyhow::Result<()> {
    Builder::from_env(Env::default().default_filter_or("info"))
        .format(|buf, record| {
            let current_time = Local::now().format("%Y-%m-%d %H:%M:%S");
            writeln!(buf, "{} - {} - {} - {}", current_time, record.level(), record.target(), record.args())
        })
        .init();

    // Parse config
    let config: Conf = serde_yaml::from_str(CONFIG_STR)?;

    let sched = Arc::new(Mutex::new(JobScheduler::new().await?));
    let is_shutdown = Arc::new(Mutex::new(false)); // 标记是否接收到关闭信号
    // let notify = Arc::new(Notify::new()); // 用于通知任务完成

    let remaining_tasks = Arc::new(AtomicUsize::new(0)); // 用于跟踪剩余任务数量

    let tz: chrono_tz::Tz = chrono_tz::Tz::from_str(&config.timezone)?;

    // Add jobs to the scheduler
    for comm in config.comms {
        let cron = comm.cron.clone();
        let remaining_tasks = remaining_tasks.clone();
        // let notify = notify.clone();
        let is_shutdown = is_shutdown.clone();

        sched.lock().await.add(
            Job::new_async_tz(cron, tz, move |uuid, mut _l| {
                let command = comm.command.clone();
                let args = comm.args.clone();
                let remaining_tasks = remaining_tasks.clone();
                // let notify = notify.clone();
                let is_shutdown = is_shutdown.clone();

                Box::pin(async move {
                    let shutdown_guard = is_shutdown.lock().await;
                    if *shutdown_guard {
                        info!("{}, 收到关闭信号，正在退出...", uuid);
                        return;
                    }
                    
                    remaining_tasks.fetch_add(1, Ordering::SeqCst); // 增加剩余任务计数

                    info!("{}, 执行任务: {} {}", uuid, command, args);

                    let child = Command::new(command)
                        .args(args.split_whitespace())
                        // .stdout(Stdio::null())
                        .spawn();
                    
                    if let Err(e) = child {
                        warn!("{}, 执行命令失败: {}", uuid, e);
                        remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }

                    let child = child.unwrap();

                    let out = child.wait_with_output().await;

                    if let Err(e) = out {
                        warn!("{}, 执行命令失败: {}", uuid, e);
                        remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }

                    let out = out.unwrap();

                    if out.status.success() {
                        info!("{}, 执行成功", uuid);
                        info!("返回信息{}", String::from_utf8_lossy(&out.stdout))
                    } else {
                        warn!("{}, 执行失败", uuid);
                        info!("返回信息{}", String::from_utf8_lossy(&out.stderr))
                    }
                    // let mut child = child.expect("ls command failed to start").await;

                    // let stdout = match child.stdout.take() {
                    //     Some(out) => {
                    //         BufReader::new(out)
                    //     }
                    //     None => {
                    //         remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                    //         warn!("{}, 警告: 命令没有标准输出", uuid);
                    //         return;
                    //     }
                    // };
                    // let stderr = match child.stderr.take() {
                    //     Some(out) => {
                    //         BufReader::new(out)
                    //     }
                    //     None => {
                    //         remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                    //         warn!("{}, 警告: 命令没有标准错误输出", uuid);
                    //         return;
                    //     }
                    // };


                    // // 创建异步任务来处理标准输出
                    // let stdout_handler = tokio::spawn(async move {
                    //     let mut lines = stdout.lines();
                    //     while let Some(line) = lines.next_line().await.unwrap() {
                    //         // 处理标准输出
                    //         info!("stdout: {}", line);
                    //     }
                    // });

                    // // 创建异步任务来处理标准错误
                    // let stderr_handler = tokio::spawn(async move {
                    //     let mut lines = stderr.lines();
                    //     while let Some(line) = lines.next_line().await.unwrap() {
                    //         // 处理标准错误
                    //         info!("stderr: {}", line);
                    //     }
                    // });

                    // 等待子进程完成
                    // let status = child.wait().await;

                    // if let Err(e) = status {
                    //     warn!("{}, 执行命令失败: {}", uuid, e);
                    //     remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                    //     return;
                    // }
                    // let status = status.unwrap();
                    // println!("命令执行完毕，退出状态：{}", status);

                    // 等待输出处理任务完成
                    // stdout_handler.await;
                    // stderr_handler.await;
                   
                    info!("执行命令: {}", remaining_tasks.load(Ordering::SeqCst));

                    remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                    // 任务完成后，减少任务计数

                    // info!("任务完成，剩余任务数量: {}", remaining_tasks.load(Ordering::SeqCst));

                    // let shutdown_guard = is_shutdown.lock().await;
            
                    // info!("信号: {}", *shutdown_guard);

                    // 如果所有任务都完成了，通知主线程
                    // if remaining_tasks.load(Ordering::SeqCst) == 0 {
                    //     notify.notify_one();
                    // }

                    // info!("执行完成: {}", remaining_tasks.load(Ordering::SeqCst));
                })
            })?
        ).await?;
    }

    // 捕获退出信号
    let mut shutdown_signal = signal(SignalKind::interrupt())?;

    let sched_clone = Arc::clone(&sched);
    let sched_handle = tokio::spawn(async move {
        sched_clone.lock().await.start().await.unwrap();
    });

    // 等待关闭信号或任务完成
    tokio::select! {
        _ = shutdown_signal.recv() => {
            info!("收到关闭信号，正在退出...");
            let mut shutdown_guard = is_shutdown.lock().await;
            *shutdown_guard = true;

            info!("正在等待任务完成...{}", remaining_tasks.load(Ordering::SeqCst));
            // 等待所有任务完成

            // 持续监控 remaining_tasks，直到它变为 0
            while remaining_tasks.load(Ordering::SeqCst) > 0 {
                info!("等待任务完成... 剩余任务: {}", remaining_tasks.load(Ordering::SeqCst));
                // 等待一段时间后再次检查 remaining_tasks
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            // 停止调度器
            sched.lock().await.shutdown().await?;

        }

        // _ = notify.notified() => {
        //     // 确保所有任务完成后再继续
        //     let shutdown_guard = is_shutdown.lock().await;
        //     if *shutdown_guard {
        //         info!("所有任务已完成.");
        //         sched.lock().await.shutdown().await?;
        //     }
            
        // }
    }

    // 终止 r-cron 进程
    terminate_rcron_process().await?;
    sched_handle.await.unwrap();

    info!("退出完成.");
    Ok(())
}

// 清除 `r-cron` 进程
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
