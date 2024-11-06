use crate::{
    alloc::RedisAlloc, key::RedisKey, redis_module, Context, ContextFlags, KeyType, KeysCursor, NextArg, RedisError, RedisResult, RedisString, RedisValue, Status, ThreadSafeContext
};
use chrono::{DateTime, Local, TimeDelta};
use lazy_static::lazy_static;
use std::{
    collections::HashMap, fmt, sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Mutex,
    }, thread, time::Duration
};

const SYSTEM_ID_LEN: usize = 4;
const SAMPLE_KEY_SIZE: usize = 10;

lazy_static! {
    static ref IS_SHUTDOWN: AtomicBool = AtomicBool::new(false);

    static ref IS_ENABLE: AtomicBool = AtomicBool::new(true);
    static ref INTERVAL: AtomicI64 = AtomicI64::new(2 * 60 * 60);  // 运行间隔 (second)
    static ref ITEMS_THRESHOLD: AtomicI64 = AtomicI64::new(10000); // 集合类型的元素个数
    static ref MEMORY_THRESHOLD: AtomicI64 = AtomicI64::new(10 * 1024 * 1024); // 10M
    static ref TTL_THRESHOLD: AtomicI64 = AtomicI64::new(604800 * 1000);  // ms, 7 days

    static ref CURRENT_TASK_INFO: Mutex<TaskInfo> = Mutex::new(TaskInfo {
        is_running: false,
        start_time: None,
        cost_time: TimeDelta::zero(),
        dbsize: 0,
        cnt: 0,
        progress: 0.0,
        data: HashMap::new(),
    });

    static ref INVALID_SYSTEM_ID: String = String::from("9999");
}

struct TaskInfo {
    is_running: bool,
    start_time: Option<DateTime<Local>>,
    cost_time: TimeDelta,
    // 用于统计扫描进度, progress = cnt / dbsize
    dbsize: u64,
    cnt: u64,            
    progress: f32,
    data: HashMap<String, SystemUsage>,
}

impl TaskInfo {
    fn start(&mut self, dbsize: u64) {
        self.data.clear();
        self.is_running = true;
        self.start_time = Some(Local::now());
        self.dbsize = dbsize;
    }

    fn stop(&mut self, data: HashMap<String, SystemUsage>, cnt: u64) {
        self.update(cnt);
        self.is_running = false;
        if let Some(t) = self.start_time {
            self.cost_time = Local::now() - t;
        }
        self.data = data;
    }

    fn update(&mut self, cnt: u64) {
        self.cnt = cnt;
        self.progress = (cnt / self.dbsize) as f32;
    }
}

impl fmt::Display for TaskInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("# Status\r\n")?;
        f.write_fmt(format_args!("enable:{}\r\n", IS_ENABLE.load(Ordering::Relaxed)))?;
        f.write_fmt(format_args!("running:{}\r\n", self.is_running))?;
        let str = match self.start_time {
            Some(t) => t.format("%Y-%m-%d %H:%M:%S:%s").to_string(),
            None => "None".to_string(),
        };
        f.write_fmt(format_args!("start_time:{}\r\n", str))?;
        f.write_fmt(format_args!("cost_time:{}\r\n", self.cost_time))?;
        f.write_fmt(format_args!("progress:{:.2} ({} / {})\r\n", self.progress * 100.0, self.cnt, self.dbsize))?;

        // 总共扫描的key个数和容量, SystemUsage中key_cnt和capacity累加
        let total_key_cnt: u32 = self.data.values().map(|v| v.key_cnt).sum();
        let total_capacity: u64 = self.data.values().map(|v| v.capacity).sum();
        f.write_fmt(format_args!("scan_key:{}\r\n", total_key_cnt))?;
        f.write_fmt(format_args!("scan_memory:{}\r\n", total_capacity))?;

        f.write_str("\r\n# System Capactiy\r\n")?;
        for entry in &self.data {
            f.write_fmt(format_args!("{}: memory:{},key_num:{},big_key:{},ttl_key:{}\r\n", 
                entry.0, entry.1.capacity, entry.1.key_cnt, entry.1.big_key_cnt, entry.1.ttl_key_cnt))?;
        }
        fmt::Result::Ok(())
    }
}

#[derive(Debug)]
struct SystemUsage {
    capacity: u64,
    key_cnt: u32,
    big_key_cnt: u32,
    big_key_sample: Vec<String>,
    ttl_key_cnt: u32,
    ttl_key_sample: Vec<String>,
}

impl SystemUsage {
    fn new() -> SystemUsage {
        SystemUsage {
            capacity: 0,
            key_cnt: 0,
            big_key_cnt: 0,
            big_key_sample: Vec::new(),
            ttl_key_cnt: 0,
            ttl_key_sample: Vec::new(),
        }
    }

    fn update(&mut self, size: u64, big_key: Option<String>, ttl_key: Option<String>) {
        self.capacity += size;
        self.key_cnt += 1;
        if let Some(k) = big_key {
            self.big_key_cnt += 1;
            if self.big_key_sample.len() < SAMPLE_KEY_SIZE {
                self.big_key_sample.push(k);
            }
        }
        if let Some(k) = ttl_key {
            self.ttl_key_cnt += 1;
            if self.ttl_key_sample.len() < SAMPLE_KEY_SIZE {
                self.ttl_key_sample.push(k);
            }
        }
    }
}

fn init(ctx: &Context, _args: &[RedisString]) -> Status {
    let info = format!(
        "config is enable:{}, interval:{}, items:{}, mem:{}, ttl:{}",
        IS_ENABLE.load(Ordering::Relaxed),
        INTERVAL.load(Ordering::Relaxed),
        ITEMS_THRESHOLD.load(Ordering::Relaxed),
        MEMORY_THRESHOLD.load(Ordering::Relaxed),
        TTL_THRESHOLD.load(Ordering::Relaxed)
    );
    ctx.log_notice(info.as_str());

    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::new();
        loop {
            std::thread::sleep(Duration::from_secs((INTERVAL.load(Ordering::Relaxed)) as u64));
            
            let ctx = thread_ctx.lock();
            if !IS_ENABLE.load(Ordering::Relaxed) {
                ctx.log_notice("inspector disable for config");
                continue;
            } else if ctx.get_flags().contains(ContextFlags::MASTER) {
                ctx.log_notice("inspector disable on master node");
                continue;
            } else if IS_SHUTDOWN.load(Ordering::Relaxed) {
                ctx.log_notice("inspector thread exit");
                break;
            }

            // TODO: context携带用户数据
            let mut sys_usage_map: HashMap<String, SystemUsage> = HashMap::new();
            // 初始化游标, 扫描所有key
            let cursor = KeysCursor::new();
            // scan命令的回调函数, 统计内存使用
            let scan_callback = |ctx: &Context, name: RedisString, key: Option<&RedisKey>| {
                if let Some(k) = key {
                    let sys_id = extract_sys_id(&name);
                    let mem_info = is_big_key(ctx, &name, k);
                    let is_ttl = is_ttl_key(k);
                    match sys_usage_map.get_mut(&sys_id) {
                        Some(u) => {
                            u.update(
                                mem_info.0,
                                if mem_info.1 { Option::Some(name.to_string()) } else { Option::None },
                                if is_ttl { Option::Some(name.to_string()) } else { Option::None },
                            );
                        }
                        None => {
                            if sys_usage_map.len() >= 2000 {
                                ctx.log_warning("scan system too mush");
                                return;
                            }
                            let mut u = SystemUsage::new();
                            u.update(
                                mem_info.0,
                                if mem_info.1 { Option::Some(name.to_string()) } else { Option::None },
                                if is_ttl { Option::Some(name.to_string()) } else { Option::None },);
                            sys_usage_map.insert(sys_id, u);
                        }
                    }
                }
            };
        
            {        
                let mut task_info = CURRENT_TASK_INFO.lock().unwrap();
                /* let server_info = ctx.server_info("memory").field("used_memory_dataset")
                                    .map_or(0, |v| v.parse_unsigned_integer().map_or(0, |v| v)); */
                task_info.start(ctx.dbsize());
                ctx.log_notice(format!("start scan, db_size:{}", task_info.dbsize).as_str());
            }

            // 避免cpu过高, 每扫1000个key暂停100微秒
            let mut cnt: u64 = 0;
            while cursor.scan(&ctx, &scan_callback) {    
                cnt += 1;
                if cnt % 1000 == 0 {
                    let mut task_info = CURRENT_TASK_INFO.lock().unwrap();
                    task_info.update(cnt);
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            
            {
                let mut task_info = CURRENT_TASK_INFO.lock().unwrap();
                task_info.stop(sys_usage_map, cnt);
                ctx.log_notice(format!("finish scan, cost time:{}", task_info.cost_time).as_str());
            }
        }
    });

    Status::Ok
}

// TODO: 清除数据
fn deinit(_ctx: &Context) -> Status {
    IS_SHUTDOWN.store(true, Ordering::Relaxed);
    Status::Ok
}

/// 手动触发扫描 主节点  正在进行  enable
/* fn inspect_trigger(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    Ok(RedisValue::SimpleStringStatic("OK"))
} */

/// 获取统计结果
fn inspect_info(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let task_info = CURRENT_TASK_INFO.lock().unwrap();
    Ok(RedisValue::BulkString(task_info.to_string()))
}

/// 开启或关闭扫描功能
/// eg: INSPECTOR.ENABLE YES / NO
fn inspect_enable(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let flag = args.next_str()?;
    if "yes" == flag {
        IS_ENABLE.store(true, Ordering::Relaxed);    
    } else if "no" == flag {
        IS_ENABLE.store(false, Ordering::Relaxed);
    } else {
        return Err(RedisError::Str("ERR Flag must be yes or no"));
    }
    Ok(RedisValue::SimpleStringStatic("OK"))
}

/// 指定子系统, 获取扫描到的big_key或ttl_key
/// eg: INSPECTOR.KEYS TTL_KEY 5036 
///     INSPECTOR.KEYS BIG_KEY 5036 
fn inspect_keys(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let types = args.next_str()?.to_uppercase();
    if types != "TTL_KEY" && types != "BIG_KEY" {
        return Err(RedisError::Str("ERR Type must be TTL_KEY or BIG_KEY"));
    }

    let system = args.next_str()?;
    let mut result = Vec::new();
    let task_info = CURRENT_TASK_INFO.lock().unwrap();
    if let Some(u) = task_info.data.get(system) {
        if types == "TTL_KEY" {
            for ele in &u.ttl_key_sample {
                result.push(RedisValue::BulkString(ele.clone()));
            }
        } else if types == "BIG_KEY" {
            for ele in &u.big_key_sample {
                result.push(RedisValue::BulkString(ele.clone()));
            }
        }
    }
    Ok(RedisValue::Array(result))
}


/// 判断key是否为big_key 
fn is_big_key(ctx: &Context, name: &RedisString, key: &RedisKey) -> (u64, bool) {
    let size: u64;
    let reply = ctx.call("MEMORY", &["USAGE", name.to_string().as_str(), "SAMPLES", "0"]);
    if let RedisResult::Ok(RedisValue::Integer(i)) = reply {
        size = i.try_into().unwrap();
        if i > MEMORY_THRESHOLD.load(Ordering::Relaxed) {
            return (size, true);
        }
    } else {
        ctx.log_warning(format!("unexpected result type of memory usage, reply:{:?}", reply).as_str());   
        return (0, false);
    }

    let num_result = match key.key_type() {
        KeyType::Empty => RedisResult::Ok(RedisValue::Integer(0)),
        KeyType::String => RedisResult::Ok(RedisValue::Integer(0)),
        KeyType::List => ctx.call("llen", &[name.to_string().as_str()]),
        KeyType::Hash => ctx.call("hlen", &[name.to_string().as_str()]),
        KeyType::Set => ctx.call("scard", &[name.to_string().as_str()]),
        KeyType::ZSet => ctx.call("zcard", &[name.to_string().as_str()]),
        KeyType::Module => {
            // TODO: exstrtype tairhash-
            RedisResult::Ok(RedisValue::Integer(0))
        },
        t => {
            ctx.log_warning(format!("unsupport type:{:?}", t).as_str());
            RedisResult::Ok(RedisValue::Integer(0))
        },
    };

    if let RedisResult::Ok(RedisValue::Integer(v)) = num_result {
        if v > ITEMS_THRESHOLD.load(Ordering::Relaxed) {
            return (size, true)
        }
    }
    (size, false)
}

/// 判断key是否设置ttl或ttl超过阈值
/// get_expire()返回剩余ttl, 单位ms
fn is_ttl_key(key: &RedisKey) -> bool {
    let expire = key.get_expire();
    expire == -1 || expire > TTL_THRESHOLD.load(Ordering::Relaxed)
}

/// 提取utf-8编码的前4个字符为子系统id  TODO: review
fn extract_sys_id(name: &RedisString) -> String {
    let mut chars = name
        .as_slice()
        .iter()
        .copied()
        .map(char::from)
        .take(SYSTEM_ID_LEN);
    let mut prefix = String::new();
    for _ in 0..SYSTEM_ID_LEN {
        if let Some(c) = chars.next() {
            prefix.push(c);
        } else {
            break;
        }
    }
    if prefix.len() < SYSTEM_ID_LEN {
        INVALID_SYSTEM_ID.to_string()
    } else {
        prefix.to_string()
    }
}

//////////////////////////////////////////////////////

redis_module! {
    name: "inspector",
    version: 1,
    allocator: (RedisAlloc, RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
        ["inspector.info", inspect_info, "readonly", 0, 0, 0],
        ["inspector.enable", inspect_enable, "write", 0, 0, 0],
        ["inspector.keys", inspect_keys, "readonly", 0, 0, 0],
    ],
    /* configurations: [
        i64: [
            ["interval", &*INTERVAL, 3600, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
            ["iters", &*ITEMS_THRESHOLD, 10000, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
            ["memory", &*MEMORY_THRESHOLD, 1048576, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
            ["ttl", &*TTL_THRESHOLD, 60480000, 0, i64::MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
        ],
        bool: [
            ["enable", &*IS_ENABLE, true, ConfigurationFlags::DEFAULT, None],
        ],
        enum: [
        ],
        module_args_as_configuration: true,
    ] */
}
