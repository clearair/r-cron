use chrono::Local;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, fs::File as StdFile, process::{self}, str::FromStr, sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::Duration};
use tokio::{process::Command, signal::unix::{signal, SignalKind}, sync::Mutex}; // 用于捕获信号
use log::{info, warn};
use env_logger::{Builder, Env};
use std::io::Write;
use clap::Parser;


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

    let args = Args::parse();
    let config: Conf = serde_yaml::from_reader(StdFile::open(args.config_file)?)?;

    let sched = Arc::new(Mutex::new(JobScheduler::new().await?));
    let is_shutdown = Arc::new(Mutex::new(false)); // 标记是否接收到关闭信号

    let remaining_tasks = Arc::new(AtomicUsize::new(0)); // 用于跟踪剩余任务数量

    let tz: chrono_tz::Tz = chrono_tz::Tz::from_str(&config.timezone)?;
    let running_tasks: Arc<Mutex<HashMap<Uuid, ()>>> = Arc::new(Mutex::new(HashMap::new()));

    for comm in config.comms {
        let cron = comm.cron.clone();
        let remaining_tasks = remaining_tasks.clone();
        let is_shutdown = is_shutdown.clone();
        let running_tasks = running_tasks.clone();

        sched.lock().await.add(
            Job::new_async_tz(cron, tz, move |u, mut _l| {
                let uuid = Uuid::new_v4();
                let command = comm.command.clone();
                let args = comm.args.clone();
                let remaining_tasks = remaining_tasks.clone();
                let is_shutdown = is_shutdown.clone();
                let running_tasks = running_tasks.clone();

                Box::pin(async move {
                    let execute_result = async {
                        let shutdown_guard = is_shutdown.lock().await;
                        if *shutdown_guard {
                            return Err(format!("{} 收到关闭信号，正在退出...", uuid));
                        }

                        let mut running_tasks_guard = running_tasks.lock().await;
                    
                        if running_tasks_guard.contains_key(&u) {
                            return Err(format!("{} 任务已在执行中，跳过", u));
                        }
                    
                        // 标记任务为运行中
                        running_tasks_guard.insert(u.clone(), ());
                        drop(running_tasks_guard);

                        remaining_tasks.fetch_add(1, Ordering::SeqCst); // 增加剩余任务计数
                        info!("{}, 执行任务: {} {}", uuid, command, args);
                        info!("任务数 {}", remaining_tasks.load(Ordering::SeqCst));
                        let child = Command::new(command)
                            .args(args.split_whitespace())
                            .process_group(0)
                            // .stdout(Stdio::null())
                            .spawn();
                        if let Err(e) = child {
                            return Err(format!("{} 执行命令失败: {}", uuid, e));
                        }
    
                        let child = child.unwrap();
    
                        let out = child.wait_with_output().await;
    
                        if let Err(e) = out {
                            return Err(format!("{} 执行命令失败: {}", uuid, e));
                        }

                        let out = out.unwrap();

                        if out.status.success() {
                            info!("{}, 执行成功", uuid);
                            info!("返回信息{}", String::from_utf8_lossy(&out.stdout))
                        } else {
                            warn!("{}, 执行失败", uuid);
                            info!("返回信息{}", String::from_utf8_lossy(&out.stderr))
                        }
                        
                        Ok(())
                    }.await;

                    match execute_result {
                        Ok(_) => {
                            info!("{} 执行成功", uuid);
                            info!("执行命令: {}", remaining_tasks.load(Ordering::SeqCst));
                            remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                            let mut running_tasks_guard = running_tasks.lock().await;
                            running_tasks_guard.remove(&u);
                        }
                        Err(e) => {
                            warn!("{} 执行失败: {}", uuid, e);
                            remaining_tasks.fetch_sub(1, Ordering::SeqCst);
                            let mut running_tasks_guard = running_tasks.lock().await;
                            running_tasks_guard.remove(&u);
                        }
                    }
                    
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

            // 持续监控 remaining_tasks，直到它变为 0 或者超时
            let mut count = 0;
            while remaining_tasks.load(Ordering::SeqCst) > 0 {
                info!("等待任务完成... 剩余任务: {}", remaining_tasks.load(Ordering::SeqCst));
                // 等待一段时间后再次检查 remaining_tasks
                tokio::time::sleep(Duration::from_secs(1)).await;
                count += 1;
                if count > 20 {
                    break;
                }
            }
            // 停止调度器
            sched.lock().await.shutdown().await?;

        }

    }

    // 终止 r-cron 进程
    sched_handle.await.unwrap();

    info!("退出完成.");
    Ok(())
}

fn main() {
    let _ = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}
