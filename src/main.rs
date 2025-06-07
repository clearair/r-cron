use chrono::Local;
use nix::{sys::signal::{kill, Signal}, unistd::Pid};
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, fs::File as StdFile, str::FromStr, sync::Arc, time::Duration};
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
    timeout: u8,
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



    let tz: chrono_tz::Tz = chrono_tz::Tz::from_str(&config.timezone)?;
    let running_tasks: Arc<Mutex<HashMap<Uuid, ()>>> = Arc::new(Mutex::new(HashMap::new()));
    let children: Arc<Mutex<HashMap<u32, ()>>> = Arc::new(Mutex::new(HashMap::new()));
    for comm in config.comms {
        let cron = comm.cron.clone();
        
        let is_shutdown = is_shutdown.clone();
        let running_tasks = running_tasks.clone();
        let children = children.clone();

        sched.lock().await.add(
            Job::new_async_tz(cron, tz, move |u, mut _l| {
                let uuid = Uuid::new_v4();
                let command = comm.command.clone();
                let args = comm.args.clone();
                
                let is_shutdown = is_shutdown.clone();
                let running_tasks = running_tasks.clone();
                let children = children.clone();
                info!("{} 开始执行任务: {} {}", uuid, command, args);
                Box::pin(async move {
                    let execute_result = async {
                        {
                            let shutdown_guard = is_shutdown.lock().await;
                            if *shutdown_guard {
                                return Err(format!("{} 收到关闭信号，正在退出...", uuid));
                            }
                        }
    
                        let child = Command::new(command)
                            .args(args.split_whitespace())
                            .process_group(0)
                            .spawn();

                        let pid;
                        let child = match child {
                            Ok(child) => {
                                pid = child.id().unwrap();
                                children.lock().await.insert(pid, ());
                                child
                            },
                            Err(e) => return Err(format!("{} 执行命令失败: {}", uuid, e)),
                        };
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
                        
                        Ok(pid)
                    }.await;

                    match execute_result {
                        Ok(pid) => {
                            info!("{} 执行成功", uuid);
                            
                            
                            let mut running_tasks_guard = running_tasks.lock().await;
                            running_tasks_guard.remove(&u);
                            let mut children_guard = children.lock().await;
                            children_guard.remove(&pid);
                        }
                        Err(e) => {
                            warn!("{} 执行失败: {}", uuid, e);
                            
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
            {
                let mut shutdown_guard = is_shutdown.lock().await;
                *shutdown_guard = true;
            }

            // 等待所有任务完成

            
            let mut count = 0u8;
            
            let mut children_num;
            {
               children_num = children.lock().await.len()
            }
            while children_num > 0 {
                info!("等待任务完成... 剩余任务: {}", children_num);
                
                tokio::time::sleep(Duration::from_secs(1)).await;
                count += 1;
                info!("等待任务完成... {} 秒", count);
                if count > config.timeout {
                    warn!("等待任务完成超时，强制退出");
                    break;
                }
                children_num = children.lock().await.len()
            }
            let children = children.lock().await;
            for pid in children.keys() {
                let _ = kill(Pid::from_raw(*pid as i32), Some(Signal::SIGKILL));
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use tokio_cron_scheduler::{Job, JobScheduler};

    use super::*;
    use serde_yaml;
    use clap::Parser;

    #[test]
    fn test_conf_deserialize() {
        let yaml = r#"timezone: 'Asia/Shanghai'
comms:
  - command: 'echo'
    args: 'hello world'
    cron: '* * * * * *'
timeout: 10
"#;
        let conf: Conf = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(conf.timezone, "Asia/Shanghai");
        assert_eq!(conf.comms.len(), 1);
        assert_eq!(conf.comms[0].command, "echo");
        assert_eq!(conf.comms[0].args, "hello world");
        assert_eq!(conf.comms[0].cron, "* * * * * *");
        assert_eq!(conf.timeout, 10);
    }

    #[test]
    fn test_args_parse() {
        let args = vec!["prog", "-c", "config.yaml"];
        let args = Args::parse_from(args);
        assert_eq!(args.config_file, "config.yaml");
    }

    #[tokio::test]
    async fn test_scheduler_triggers_job() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let mut sched = JobScheduler::new().await.unwrap();
        sched.add(Job::new_async("*/1 * * * * *", move |_uuid, _l| {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        }).unwrap()).await.unwrap();

        sched.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        sched.shutdown().await.unwrap();

        assert!(counter.load(Ordering::SeqCst) >= 1);
    }
}

