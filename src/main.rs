mod html_parser;

use anyhow::{Context, Result};
use bstr::ByteSlice;
use hdrhistogram::Histogram;
use memmap2::Mmap;
use num_cpus;
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, Read};
use std::thread;
use std::time::Instant;

#[derive(Debug, Deserialize, Clone)]
struct Row {
    #[serde(rename = "timeStamp")]
    time_stamp: i64, // ms since epoch
    elapsed: u64, // response time ms
    label: String,
    #[serde(rename = "responseCode")]
    response_code: String,
    #[serde(rename = "responseMessage")]
    response_message: String,
    #[serde(deserialize_with = "de_bool")]
    success: bool,
    #[serde(deserialize_with = "de_optional_u64")]
    bytes: Option<u64>, // received bytes
    #[serde(alias = "SentBytes", alias = "sentBytes", deserialize_with = "de_optional_u64")]
    sent_bytes: Option<u64>,
}

#[derive(serde::Serialize)]
struct OverallOut {
    samples: u64,
    failures: u64,
    error_pct: f64,
    avg_ms: f64,
    min_ms: u64,
    max_ms: u64,
    p50_ms: u64,
    p90_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    duration_sec: f64,
    tps: f64,
    kbps_recv: f64,
    kbps_sent: f64,
}

#[derive(serde::Serialize)]
struct LabelOut {
    label: String,
    count: u64,
    fails: u64,
    error_pct: f64,
    avg_ms: f64,
    min_ms: u64,
    max_ms: u64,
    p50_ms: u64,
    p90_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    tps: f64,
    kbps_recv: f64,
    kbps_sent: f64,
}

#[derive(serde::Serialize)]
struct ErrTypeOut {
    response_code: String,
    response_message: String,
    count: u64,
    error_pct: f64,
    sample_pct: f64,
}

#[derive(Clone)]
struct Agg {
    count: u64,
    fails: u64,
    recv_bytes: u128,
    sent_bytes: u128,
    first_ts: i64,
    last_end_ts: i64,
    hist: Histogram<u64>,
    min_elapsed: u64,
    max_elapsed: u64,
}

impl Agg {
    fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 604_800_000, 3).unwrap();
        Self {
            count: 0,
            fails: 0,
            recv_bytes: 0,
            sent_bytes: 0,
            first_ts: i64::MAX,
            last_end_ts: i64::MIN,
            hist,
            min_elapsed: u64::MAX,
            max_elapsed: 0,
        }
    }

    fn add(&mut self, r: &Row) {
        self.count += 1;
        if !r.success {
            self.fails += 1;
        }

        let e = r.elapsed.max(1); // HDR histogram requires >= 1; treat 0ms as 1ms consistently
        if let Err(err) = self.hist.record(e) {
            eprintln!("hist record failed (elapsed={}): {err}", r.elapsed);
        }

        self.min_elapsed = self.min_elapsed.min(e);
        self.max_elapsed = self.max_elapsed.max(e);

        if let Some(b) = r.bytes {
            self.recv_bytes += b as u128;
        }
        if let Some(sb) = r.sent_bytes {
            self.sent_bytes += sb as u128;
        }

        self.first_ts = self.first_ts.min(r.time_stamp);
        let end = r.time_stamp.saturating_add(r.elapsed as i64);
        self.last_end_ts = self.last_end_ts.max(end);
    }

    fn merge(&mut self, other: Agg) {
        self.count += other.count;
        self.fails += other.fails;
        self.recv_bytes += other.recv_bytes;
        self.sent_bytes += other.sent_bytes;
        self.first_ts = self.first_ts.min(other.first_ts);
        self.last_end_ts = self.last_end_ts.max(other.last_end_ts);
        self.min_elapsed = self.min_elapsed.min(other.min_elapsed);
        self.max_elapsed = self.max_elapsed.max(other.max_elapsed);
        self.hist
            .add(&other.hist)
            .expect("histograms must be compatible");
    }

    fn duration_secs(&self) -> f64 {
        if self.first_ts == i64::MAX || self.last_end_ts <= self.first_ts {
            0.0
        } else {
            (self.last_end_ts - self.first_ts) as f64 / 1000.0
        }
    }

    fn avg_ms(&self) -> f64 {
        self.hist.mean()
    }

    fn q(&self, p: f64) -> u64 {
        self.hist.value_at_quantile(p)
    }
}

impl Default for Agg {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct Shard {
    overall: Agg,
    per_label: HashMap<String, Agg>,
    error_types: HashMap<(String, String), u64>,
}

impl Shard {
    fn new() -> Self {
        Self {
            overall: Agg::new(),
            per_label: HashMap::new(),
            error_types: HashMap::new(),
        }
    }

    fn add(&mut self, r: &Row) {
        if !r.success {
            *self
                .error_types
                .entry((r.response_code.clone(), r.response_message.clone()))
                .or_default() += 1;
        }
        self.overall.add(r);
        self.per_label.entry(r.label.clone()).or_default().add(r);
    }

    fn merge(&mut self, other: Shard) {
        self.overall.merge(other.overall);
        for (k, v) in other.per_label {
            match self.per_label.entry(k) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let existing = e.get_mut();
                    existing.merge(v);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(v);
                }
            }
        }
        for (k, v) in other.error_types {
            *self.error_types.entry(k).or_default() += v;
        }
    }
}

fn de_bool<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Cow<'de, str> = Cow::deserialize(d)?;
    Ok(s.eq_ignore_ascii_case("true"))
}

fn de_optional_u64<'de, D>(d: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Cow<'de, str> = Cow::deserialize(d)?;
    if s.is_empty() || s.eq_ignore_ascii_case("null") {
        Ok(None)
    } else {
        s.parse().map(Some).map_err(serde::de::Error::custom)
    }
}

fn kbps(bytes: u128, secs: f64) -> f64 {
    if secs <= 0.0 {
        0.0
    } else {
        (bytes as f64 * 8.0 / 1000.0) / secs
    }
}

fn open_reader(path: Option<&str>, delim: u8) -> Result<csv::Reader<Box<dyn Read + Send>>> {
    let boxed: Box<dyn Read + Send> = match path {
        Some("-") | None => Box::new(io::stdin()),
        Some(p) => Box::new(File::open(p).with_context(|| format!("open {p}"))?),
    };
    Ok(csv::ReaderBuilder::new()
        .delimiter(delim)
        .has_headers(true)
        .from_reader(boxed))
}

/// Optimized CSV parser using memory-mapped files and manual parsing
fn parse_csv_parallel(path: &str, delim: u8) -> Result<Shard> {
    eprintln!("Starting optimized parsing with memory-mapped file...");

    let file = File::open(path).with_context(|| format!("open {}", path))?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = mmap.as_ref();
    
    let threads = num_cpus::get();
    eprintln!("Using {} threads for processing", threads);

    // Find header
    let header_end = data.find_byte(b'\n').unwrap_or(0) + 1;
    let header = &data[..header_end];
    let body = &data[header_end..];
    
    // Split into chunks for parallel processing
    let chunk_size = (body.len() + threads - 1) / threads;
    let mut chunk_starts = Vec::with_capacity(threads);
    let mut chunk_ends = Vec::with_capacity(threads);
    
    for i in 0..threads {
        let start = i * chunk_size;
        let end = (start + chunk_size).min(body.len());
        
        if start >= end {
            continue;
        }
        
        // Adjust start to beginning of a line (unless it's the first chunk)
        let mut chunk_start = start;
        if i > 0 {
            while chunk_start > 0 && body[chunk_start - 1] != b'\n' {
                chunk_start -= 1;
            }
        }
        
        // Adjust end to end of a line (unless it's the last chunk)
        let mut chunk_end = end;
        if i < threads - 1 && chunk_end < body.len() {
            while chunk_end < body.len() && body[chunk_end] != b'\n' {
                chunk_end += 1;
            }
            if chunk_end < body.len() {
                chunk_end += 1; // Include the newline
            }
        }
        
        if chunk_start >= chunk_end {
            continue;
        }
        
        chunk_starts.push(chunk_start);
        chunk_ends.push(chunk_end);
    }
    
    eprintln!("Processing {} chunks in parallel...", chunk_starts.len());
    
    // Process chunks in parallel using thread pool
    let mut handles = Vec::with_capacity(chunk_starts.len());
    
    for i in 0..chunk_starts.len() {
        let start = chunk_starts[i];
        let end = chunk_ends[i];
        let chunk = &body[start..end];
        
        // Create a new chunk that includes the header
        let mut chunk_with_header = Vec::with_capacity(header.len() + chunk.len());
        chunk_with_header.extend_from_slice(header);
        chunk_with_header.extend_from_slice(chunk);
        
        let handle = thread::spawn(move || {
            let mut shard = Shard::new();
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(delim)
                .has_headers(true)
                .flexible(true)
                .from_reader(chunk_with_header.as_slice());
            
            for rec in rdr.deserialize::<Row>() {
                match rec {
                    Ok(row) => {
                        shard.add(&row);
                    }
                    Err(e) => {
                        // Log first few errors for debugging
                        if i == 0 {
                            eprintln!("Skipping bad row in chunk {}: {}", i, e);
                        }
                    }
                }
            }
            
            shard
        });

        eprintln!("Processed {} rows, chunk {}/{}", chunk_size * (i + 1), i + 1, chunk_starts.len());
        
        handles.push(handle);
    }
    
    // Merge all shards
    eprintln!("Merging results from {} shards...", handles.len());
    let mut total = Shard::new();
    for handle in handles {
        let shard = handle.join().expect("thread panicked");
        total.merge(shard);
    }
    
    eprintln!("Optimized parsing completed successfully");
    Ok(total)
}

/// Parse CSV data sequentially (for stdin or small files)
fn parse_csv_sequential(mut rdr: csv::Reader<Box<dyn Read + Send>>) -> Result<Shard> {
    eprintln!("Starting sequential parsing...");
    let mut shard = Shard::new();
    let mut seen: u64 = 0;
    let mut last_log = Instant::now();
    
    for rec in rdr.deserialize::<Row>() {
        match rec {
            Ok(row) => {
                seen += 1;
                if seen % 1_000_000 == 0 {
                    let elapsed = last_log.elapsed();
                    eprintln!("Read {seen} rows... ({:.1} rows/sec)", 1_000_000.0 / elapsed.as_secs_f64());
                    last_log = Instant::now();
                }
                shard.add(&row);
            }
            Err(e) => {
                eprintln!("Skipping bad row: {e}");
            }
        }
    }
    
    eprintln!("Sequential parsing completed, processed {} rows", seen);
    Ok(shard)
}

fn main() -> Result<()> {
    let t0 = Instant::now();

    let args: Vec<String> = env::args().collect();
    let input_path = args.get(1).map(|s| s.as_str());
    let delim = args
        .get(2)
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',');
    
    let total = if let Some(path) = input_path {
        if path == "-" {
            // Use sequential parsing for stdin
            let rdr = open_reader(input_path, delim)?;
            parse_csv_sequential(rdr)?
        } else {
            // Try optimized parsing first (memory-mapped, parallel)
            match parse_csv_parallel(path, delim) {
                Ok(shard) => shard,
                Err(e) => {
                    eprintln!("Optimized parsing failed: {}", e);
                    eprintln!("Falling back to sequential parsing...");
                     let rdr = open_reader(input_path, delim)?;
                    parse_csv_sequential(rdr)?
                }
            }
        }
    } else {
        // stdin
        let rdr = open_reader(input_path, delim)?;
        parse_csv_sequential(rdr)?
    };
    
    let overall = &total.overall;
    
    let dur = overall.duration_secs();
    let tps = if dur > 0.0 {
        overall.count as f64 / dur
    } else {
        0.0
    };
    let overall_out = OverallOut {
        samples: overall.count,
        failures: overall.fails,
        error_pct: if overall.count == 0 {
            0.0
        } else {
            (overall.fails as f64 / overall.count as f64) * 100.0
        },
        avg_ms: overall.avg_ms(),
        min_ms: if overall.min_elapsed == u64::MAX {
            0
        } else {
            overall.min_elapsed
        },
        max_ms: overall.max_elapsed,
        p50_ms: overall.q(0.50),
        p90_ms: overall.q(0.90),
        p95_ms: overall.q(0.95),
        p99_ms: overall.q(0.99),
        duration_sec: dur,
        tps,
        kbps_recv: kbps(overall.recv_bytes, dur),
        kbps_sent: kbps(overall.sent_bytes, dur),
    };
    
    let mut labels_out: Vec<LabelOut> = total
        .per_label
        .into_iter()
        .map(|(label, agg)| {
            let dur = agg.duration_secs();
            let tps = if dur > 0.0 {
                agg.count as f64 / dur
            } else {
                0.0
            };
            LabelOut {
                label,
                count: agg.count,
                fails: agg.fails,
                error_pct: if agg.count == 0 {
                    0.0
                } else {
                    (agg.fails as f64 / agg.count as f64) * 100.0
                },
                avg_ms: agg.avg_ms(),
                min_ms: if agg.min_elapsed == u64::MAX {
                    0
                } else {
                    agg.min_elapsed
                },
                max_ms: agg.max_elapsed,
                p50_ms: agg.q(0.50),
                p90_ms: agg.q(0.90),
                p95_ms: agg.q(0.95),
                p99_ms: agg.q(0.99),
                tps,
                kbps_recv: kbps(agg.recv_bytes, dur),
                kbps_sent: kbps(agg.sent_bytes, dur),
            }
        })
        .collect();
    labels_out.sort_by(|a, b| b.count.cmp(&a.count));
    
    let total_errors: u64 = total.error_types.values().sum();
    let errs_out: Vec<ErrTypeOut> = {
        let mut v: Vec<_> = total.error_types.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.into_iter()
            .map(|((code, msg), cnt)| ErrTypeOut {
                response_code: code,
                response_message: msg,
                count: cnt,
                error_pct: if total_errors == 0 {
                    0.0
                } else {
                    (cnt as f64 / total_errors as f64) * 100.0
                },
                sample_pct: if overall.count == 0 {
                    0.0
                } else {
                    (cnt as f64 / overall.count as f64) * 100.0
                },
            })
            .collect()
    };

    let out_path = args.get(3).map(|s| s.as_str()).unwrap_or("jtl_report.html");
    let title = input_path.unwrap_or("stdin");
    let html = html_parser::render_html(overall_out, labels_out, errs_out, title);
    std::fs::write(out_path, html).with_context(|| format!("write {}", out_path))?;
    eprintln!("Total time: {:.3}s", t0.elapsed().as_secs_f64());
    eprintln!("Wrote HTML report to {}", out_path);

    Ok(())
}
