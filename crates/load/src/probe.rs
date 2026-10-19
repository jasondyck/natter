//! CPU time and memory of the server under test: a process from
//! `/proc/<pid>`, or a whole container from its cgroup (every process in
//! it, as `bench/lk-bench.sh` measures LiveKit).

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, bail};

#[derive(Debug, Clone)]
pub enum Probe {
    Pid(u32),
    Cgroup(PathBuf),
}

#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub cpu: Duration,
    pub memory: u64,
}

/// Linux reports process times in USER_HZ ticks, 100 a second everywhere
/// that matters.
const TICK: Duration = Duration::from_millis(10);

impl Probe {
    pub fn sample(&self) -> anyhow::Result<Sample> {
        match self {
            Probe::Pid(pid) => process(&format!("/proc/{pid}")),
            Probe::Cgroup(dir) => {
                let stat = fs::read_to_string(dir.join("cpu.stat"))
                    .with_context(|| format!("reading {}/cpu.stat", dir.display()))?;
                let usec = field(&stat, "usage_usec").context("no usage_usec in cpu.stat")?;
                let memory = fs::read_to_string(dir.join("memory.current"))?
                    .trim()
                    .parse()?;
                // Page cache counts toward memory.current; report what the
                // processes hold, as `docker stats` does.
                let inactive = fs::read_to_string(dir.join("memory.stat"))
                    .ok()
                    .and_then(|s| field(&s, "inactive_file"))
                    .unwrap_or(0);
                Ok(Sample {
                    cpu: Duration::from_micros(usec),
                    memory: u64::saturating_sub(memory, inactive),
                })
            }
        }
    }

    pub fn this_process() -> anyhow::Result<Sample> {
        process("/proc/self")
    }
}

fn process(dir: &str) -> anyhow::Result<Sample> {
    let stat = fs::read_to_string(format!("{dir}/stat")).with_context(|| format!("{dir}/stat"))?;
    // Fields after the command name, which may itself hold spaces.
    let rest = stat.rsplit_once(')').context("odd stat")?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() < 22 {
        bail!("short stat");
    }
    let ticks: u64 = fields[11].parse::<u64>()? + fields[12].parse::<u64>()?;
    let status = fs::read_to_string(format!("{dir}/status"))?;
    let rss_kb = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    Ok(Sample {
        cpu: TICK * ticks as u32,
        memory: rss_kb * 1024,
    })
}

fn field(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == name).then(|| v.trim().parse().ok())?
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_this_process() {
        let s = Probe::Pid(std::process::id()).sample().unwrap();
        assert!(s.memory > 0);
        assert_eq!(field("usage_usec 42\nuser_usec 1", "usage_usec"), Some(42));
    }
}
