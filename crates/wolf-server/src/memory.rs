//! Memory high-watermark configuration and total/used memory sampling.

use std::io;

/// Fraction of the system total above which the memory watermark is exceeded.
const CGROUP_V1_UNLIMITED_SENTINEL: u64 = 1 << 62;

/// Memory high-watermark configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryHighWatermark {
    /// Fraction of the detected total memory, in the inclusive range `0.0..=1.0`.
    Relative(f64),
    /// Absolute number of bytes.
    Absolute(u64),
}

impl MemoryHighWatermark {
    /// Converts this watermark to a threshold in bytes for `total_bytes`.
    ///
    /// Relative watermarks truncate towards zero. Absolute watermarks are
    /// returned unchanged and are never clamped to `total_bytes`.
    pub fn threshold_bytes(self, total_bytes: u64) -> u64 {
        match self {
            Self::Absolute(bytes) => bytes,
            Self::Relative(relative) => (total_bytes as f64 * relative) as u64,
        }
    }
}

/// Origin of the detected total memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySource {
    /// cgroup v2 (`memory.max`).
    CgroupV2,
    /// cgroup v1 (`memory.limit_in_bytes`).
    CgroupV1,
    /// Total system RAM.
    System,
}

/// Total memory available to the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryTotal {
    /// Detected total memory in bytes.
    pub bytes: u64,
    /// Where the total was read from.
    pub source: MemorySource,
}

/// Resolved memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimit {
    /// Detected total memory in bytes.
    pub total_bytes: u64,
    /// Where the total was read from.
    pub source: MemorySource,
    /// Threshold in bytes derived from the configured high watermark.
    pub high_watermark_bytes: u64,
}

/// Resolves the total memory from raw cgroup values and system RAM.
///
/// `Some` means the corresponding cgroup file was present. When cgroup v2 is
/// present it is authoritative: a valid limited value wins, otherwise the
/// system RAM is used and cgroup v1 is ignored. When cgroup v2 is absent, a
/// valid cgroup v1 value is used. Finally, a positive system RAM value is used,
/// otherwise `None` is returned.
pub fn resolve_total_memory(
    cgroup_v2_max: Option<&str>,
    cgroup_v1_limit: Option<&str>,
    system_ram_bytes: Option<u64>,
) -> Option<MemoryTotal> {
    if let Some(raw) = cgroup_v2_max {
        if let Some(bytes) = parse_cgroup_limit(raw, false) {
            return Some(MemoryTotal {
                bytes,
                source: MemorySource::CgroupV2,
            });
        }
        return system_total(system_ram_bytes);
    }

    if let Some(raw) = cgroup_v1_limit {
        if let Some(bytes) = parse_cgroup_limit(raw, true) {
            return Some(MemoryTotal {
                bytes,
                source: MemorySource::CgroupV1,
            });
        }
    }

    system_total(system_ram_bytes)
}

/// Resolves the total memory and the high-watermark threshold in bytes.
pub fn resolve_memory_limit(
    watermark: MemoryHighWatermark,
    cgroup_v2_max: Option<&str>,
    cgroup_v1_limit: Option<&str>,
    system_ram_bytes: Option<u64>,
) -> Option<MemoryLimit> {
    resolve_total_memory(cgroup_v2_max, cgroup_v1_limit, system_ram_bytes).map(|total| {
        MemoryLimit {
            total_bytes: total.bytes,
            source: total.source,
            high_watermark_bytes: watermark.threshold_bytes(total.bytes),
        }
    })
}

fn system_total(system_ram_bytes: Option<u64>) -> Option<MemoryTotal> {
    system_ram_bytes
        .filter(|bytes| *bytes > 0)
        .map(|bytes| MemoryTotal {
            bytes,
            source: MemorySource::System,
        })
}

fn parse_cgroup_limit(raw: &str, sentinel_is_unlimited: bool) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("max") || trimmed.eq_ignore_ascii_case("unlimited") {
        return None;
    }
    let bytes: u64 = trimmed.parse().ok()?;
    if bytes == 0 {
        return None;
    }
    if sentinel_is_unlimited && bytes >= CGROUP_V1_UNLIMITED_SENTINEL {
        return None;
    }
    Some(bytes)
}

/// Returns the total memory detected for the current platform.
pub fn total_memory() -> io::Result<MemoryTotal> {
    platform::total_memory()
}

/// Returns the resident set size of the current process in bytes.
pub fn used_memory() -> io::Result<u64> {
    platform::used_memory()
}

#[cfg(any(target_os = "linux", test))]
fn parse_meminfo_total(contents: &str) -> Option<u64> {
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("MemTotal:") {
            continue;
        }
        let value: u64 = fields.next()?.parse().ok()?;
        if !fields.next()?.eq_ignore_ascii_case("kB") {
            return None;
        }
        return value.checked_mul(1024);
    }
    None
}

#[cfg(any(target_os = "linux", test))]
fn parse_status_rss(contents: &str) -> Option<u64> {
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("VmRSS:") {
            continue;
        }
        let value: u64 = fields.next()?.parse().ok()?;
        if fields.next()? != "kB" {
            return None;
        }
        return value.checked_mul(1024);
    }
    None
}

#[cfg(any(target_os = "macos", test))]
fn parse_ps_rss(contents: &str) -> Option<u64> {
    let value: u64 = contents.split_whitespace().next()?.parse().ok()?;
    value.checked_mul(1024)
}

#[cfg(target_os = "linux")]
mod platform {
    use std::fs;
    use std::io;

    use super::{MemoryTotal, parse_meminfo_total, parse_status_rss, resolve_total_memory};

    const CGROUP_V2_MAX: &str = "/sys/fs/cgroup/memory.max";
    const CGROUP_V1_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    const MEMINFO: &str = "/proc/meminfo";
    const PROCESS_STATUS: &str = "/proc/self/status";

    pub fn total_memory() -> io::Result<MemoryTotal> {
        let cgroup_v2 = read_optional(CGROUP_V2_MAX)?;
        let cgroup_v1 = read_optional(CGROUP_V1_LIMIT)?;
        let system_ram = read_meminfo_total()?;
        resolve_total_memory(cgroup_v2.as_deref(), cgroup_v1.as_deref(), system_ram).ok_or_else(
            || io::Error::other("unable to determine total memory from cgroups or /proc/meminfo"),
        )
    }

    pub fn used_memory() -> io::Result<u64> {
        let status = fs::read_to_string(PROCESS_STATUS)?;
        parse_status_rss(&status)
            .ok_or_else(|| io::Error::other("VmRSS is missing from /proc/self/status"))
    }

    fn read_optional(path: &str) -> io::Result<Option<String>> {
        match fs::read_to_string(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn read_meminfo_total() -> io::Result<Option<u64>> {
        match fs::read_to_string(MEMINFO) {
            Ok(contents) => Ok(parse_meminfo_total(&contents)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::io;
    use std::process::Command;

    use super::{MemoryTotal, parse_ps_rss, resolve_total_memory};

    pub fn total_memory() -> io::Result<MemoryTotal> {
        let output = Command::new("sysctl").args(["-n", "hw.memsize"]).output()?;
        if !output.status.success() {
            return Err(io::Error::other("sysctl hw.memsize failed"));
        }
        let contents = String::from_utf8_lossy(&output.stdout);
        let bytes: u64 = contents
            .trim()
            .parse()
            .map_err(|_| io::Error::other("invalid hw.memsize value"))?;
        resolve_total_memory(None, None, Some(bytes))
            .ok_or_else(|| io::Error::other("unable to determine total memory from hw.memsize"))
    }

    pub fn used_memory() -> io::Result<u64> {
        let pid = std::process::id().to_string();
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other("ps failed to report process RSS"));
        }
        let contents = String::from_utf8_lossy(&output.stdout);
        parse_ps_rss(&contents)
            .ok_or_else(|| io::Error::other("unable to parse process RSS from ps"))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use std::io;

    use super::MemoryTotal;

    pub fn total_memory() -> io::Result<MemoryTotal> {
        Err(io::Error::other(
            "total memory detection is only supported on Linux and macOS",
        ))
    }

    pub fn used_memory() -> io::Result<u64> {
        Err(io::Error::other(
            "process RSS detection is only supported on Linux and macOS",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SENTINEL: u64 = 1 << 62;

    fn limit(watermark: MemoryHighWatermark) -> Option<MemoryLimit> {
        resolve_memory_limit(watermark, None, None, Some(1_000_000))
    }

    #[test]
    fn memory_limit_resolution() {
        let watermark = MemoryHighWatermark::Relative(0.5);

        let v2 = resolve_memory_limit(watermark, Some("1048576"), Some("2097152"), Some(4096))
            .expect("v2 value is usable");
        assert_eq!(v2.total_bytes, 1_048_576);
        assert_eq!(v2.source, MemorySource::CgroupV2);
        assert_eq!(v2.high_watermark_bytes, 524_288);

        let v1 = resolve_memory_limit(watermark, None, Some("2097152"), Some(4096))
            .expect("v1 value is usable");
        assert_eq!(v1.total_bytes, 2_097_152);
        assert_eq!(v1.source, MemorySource::CgroupV1);
        assert_eq!(v1.high_watermark_bytes, 1_048_576);

        let v2_max = resolve_memory_limit(watermark, Some("max"), Some("2097152"), Some(4096))
            .expect("system RAM is usable");
        assert_eq!(v2_max.total_bytes, 4096);
        assert_eq!(v2_max.source, MemorySource::System);

        for raw in ["max", "unlimited", &SENTINEL.to_string()] {
            let resolved = resolve_memory_limit(watermark, None, Some(raw), Some(4096))
                .expect("system RAM is usable");
            assert_eq!(resolved.total_bytes, 4096);
            assert_eq!(resolved.source, MemorySource::System);
        }

        let system =
            resolve_memory_limit(watermark, None, None, Some(4096)).expect("system RAM is usable");
        assert_eq!(system.total_bytes, 4096);
        assert_eq!(system.source, MemorySource::System);

        let absolute = resolve_memory_limit(
            MemoryHighWatermark::Absolute(10_000_000),
            Some("1048576"),
            None,
            Some(4096),
        )
        .expect("v2 value is usable");
        assert_eq!(absolute.total_bytes, 1_048_576);
        assert_eq!(absolute.high_watermark_bytes, 10_000_000);
    }

    #[test]
    fn v2_invalid_values_fall_back_to_system_and_ignore_v1() {
        for raw in ["0", "bogus", "-1", " "] {
            let resolved = resolve_total_memory(Some(raw), Some("2097152"), Some(4096))
                .expect("system RAM is usable");
            assert_eq!(resolved.bytes, 4096);
            assert_eq!(resolved.source, MemorySource::System);
        }
    }

    #[test]
    fn cgroup_values_are_trimmed() {
        let resolved = resolve_total_memory(Some(" 1048576\n"), None, None)
            .expect("trimmed v2 value is usable");
        assert_eq!(resolved.bytes, 1_048_576);
        assert_eq!(resolved.source, MemorySource::CgroupV2);
    }

    #[test]
    fn absent_system_ram_yields_no_limit() {
        assert_eq!(resolve_total_memory(None, None, None), None);
        assert_eq!(resolve_total_memory(None, None, Some(0)), None);
    }

    #[test]
    fn relative_threshold_truncates() {
        assert_eq!(MemoryHighWatermark::Relative(0.5).threshold_bytes(3), 1);
        assert_eq!(
            MemoryHighWatermark::Relative(0.4).threshold_bytes(1000),
            400
        );
    }

    #[test]
    fn absolute_threshold_is_not_clamped() {
        let limit = limit(MemoryHighWatermark::Absolute(u64::MAX)).expect("system RAM is usable");
        assert_eq!(limit.high_watermark_bytes, u64::MAX);
    }

    #[test]
    fn meminfo_total_is_parsed_as_kibibytes() {
        let contents = "MemTotal:       16316468 kB\nMemFree:         1234 kB\n";
        assert_eq!(parse_meminfo_total(contents), Some(16_316_468 * 1024));
        assert_eq!(parse_meminfo_total("MemFree: 1 kB\n"), None);
        assert_eq!(parse_meminfo_total("MemTotal: 1 MB\n"), None);
    }

    #[test]
    fn status_rss_is_parsed_as_kibibytes() {
        let contents = "Name:\twolf\nVmRSS:\t   2048 kB\n";
        assert_eq!(parse_status_rss(contents), Some(2_097_152));
        assert_eq!(parse_status_rss("Name:\twolf\n"), None);
        assert_eq!(parse_status_rss("VmRSS:\t 12 kB\n"), Some(12_288));
    }

    #[test]
    fn ps_rss_is_parsed_as_kibibytes() {
        assert_eq!(parse_ps_rss("   2048\n"), Some(2_097_152));
        assert_eq!(parse_ps_rss("2048 3072\n"), Some(2_097_152));
        assert_eq!(parse_ps_rss("not a number"), None);
    }
}
