mod html_parser;

use ahash::AHashMap;
use anyhow::{Context, Result};
use bstr::ByteSlice;
use chrono;
use hdrhistogram::Histogram;
use memmap2::Mmap;
use num_cpus;
use serde::Deserialize;
use std::borrow::Cow;
use std::env;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::thread;
use std::time::Instant;

// Macro for conditional verbose output
#[cfg(feature = "verbose")]
macro_rules! verbose_eprintln {
    ($($arg:tt)*) => {
        eprintln!($($arg)*)
    };
}

#[cfg(not(feature = "verbose"))]
macro_rules! verbose_eprintln {
    ($($arg:tt)*) => {
        // No output in non-verbose mode
    };
}

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
    p75_ms: u64,
    p85_ms: u64,
    p90_ms: u64,
    p95_ms: u64,
    p99_ms: u64,
    duration_sec: f64,
    duration_hours: f64,
    duration_minutes: f64,
    start_timestamp: i64,
    start_date: String,
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
    p75_ms: u64,
    p85_ms: u64,
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
    per_label: AHashMap<String, Agg>,
    error_types: AHashMap<(String, String), u64>,
}

impl Shard {
    fn new() -> Self {
        Self {
            overall: Agg::new(),
            per_label: AHashMap::default(),
            error_types: AHashMap::default(),
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

/// Adaptive CSV parser: uses memory-mapped for files < 20GB, streaming for larger files
fn parse_csv_adaptive(path: &str, delim: u8) -> Result<Shard> {
    // Check file size to decide which parser to use
    let file = File::open(path).with_context(|| format!("open {}", path))?;
    let file_size = file.metadata()?.len();
    
    if file_size < 20_000_000_000 { // < 20GB
        verbose_eprintln!("File size: {:.1} GB - using memory-mapped parallel parser", 
            file_size as f64 / 1_000_000_000.0);
        parse_csv_memory_mapped(path, delim)
    } else {
        verbose_eprintln!("File size: {:.1} GB - using memory-efficient streaming parser", 
            file_size as f64 / 1_000_000_000.0);
        parse_csv_streaming(path, delim)
    }
}

/// Memory-mapped CSV parser for files < 20GB (fastest)
fn parse_csv_memory_mapped(path: &str, delim: u8) -> Result<Shard> {
    verbose_eprintln!("Starting memory-mapped parallel parsing...");

    let file = File::open(path).with_context(|| format!("open {}", path))?;
    let mmap = unsafe { Mmap::map(&file)? };
    
    let threads = num_cpus::get();
    verbose_eprintln!("Using {} threads for parallel processing", threads);

    // Find header
    let data = mmap.as_ref();
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
        
        // Adjust start to beginning of a line
        let mut chunk_start = start;
        if i > 0 {
            while chunk_start > 0 && body[chunk_start - 1] != b'\n' {
                chunk_start -= 1;
            }
        }
        
        // Adjust end to end of a line
        let mut chunk_end = end;
        if i < threads - 1 && chunk_end < body.len() {
            while chunk_end < body.len() && body[chunk_end] != b'\n' {
                chunk_end += 1;
            }
            if chunk_end < body.len() {
                chunk_end += 1;
            }
        }
        
        if chunk_start >= chunk_end {
            continue;
        }
        
        chunk_starts.push(chunk_start);
        chunk_ends.push(chunk_end);
    }
    
    eprintln!("Processing {} chunks in parallel...", chunk_starts.len());
    
    // Process chunks in parallel
    let mut handles = Vec::with_capacity(chunk_starts.len());
    
    for i in 0..chunk_starts.len() {
        let starts = chunk_starts.clone();
        let start = chunk_starts[i];
        let end = chunk_ends[i];
        let chunk = &body[start..end];
        
        // Create a slice that includes header + chunk for CSV parsing
        // We need to create a new vector since CSV reader needs contiguous memory
        let header_len = header.len();
        let chunk_len = chunk.len();
        let total_len = header_len + chunk_len;
        
        // Clone header data for thread
        let header_vec = header.to_vec();
        let chunk_vec = chunk.to_vec();
        
        let handle = thread::spawn(move || {
            let mut shard = Shard::new();
            
            // Create buffer for CSV parsing
            let mut buffer = Vec::with_capacity(total_len);
            buffer.extend_from_slice(&header_vec);
            buffer.extend_from_slice(&chunk_vec);
            
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(delim)
                .has_headers(true)
                .flexible(true)
                .from_reader(buffer.as_slice());
            
            for rec in rdr.deserialize::<Row>() {
                match rec {
                    Ok(row) => {
                        shard.add(&row);
                    }
                    Err(_) => {
                        // Skip bad rows
                    }
                }
            }

            eprintln!("Processed {}/{} shards, {} rows", i + 1, starts.len() + 1, chunk_size * (i+1));

            shard
        });

        handles.push(handle);
    }

    // Merge all shards
    eprintln!("Merging results from {} shards...", handles.len());
    let mut total = Shard::new();
    for handle in handles {
        let shard = handle.join().expect("thread panicked");
        total.merge(shard);
    }
    
    eprintln!("Memory-mapped parsing completed successfully");
    Ok(total)
}

/// Parallel streaming CSV parser for large files (>20GB) using multiple file cursors with fixed chunk size
fn parse_csv_streaming(path: &str, delim: u8) -> Result<Shard> {
    eprintln!("Starting memory-efficient parallel streaming parser for large files...");
    
    // Get file size and header info
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    
    // Read header to get its size
    let mut header_buf = Vec::new();
    let mut header_reader = std::io::BufReader::new(&file);
    std::io::BufRead::read_until(&mut header_reader, b'\n', &mut header_buf)?;
    let header_size = header_buf.len() as u64;
    let data_size = file_size - header_size;
    
    eprintln!("File size: {:.1} GB, Data size: {:.1} GB", 
        file_size as f64 / 1_000_000_000.0,
        data_size as f64 / 1_000_000_000.0);
    
    // Use fixed chunk size to avoid memory issues (1GB chunks)
    let chunk_size = 512 * 1024 * 1024; // 1GB
    let num_chunks = ((data_size + chunk_size - 1) / chunk_size) as usize;
    
    // Use thread pool with work stealing - each thread processes multiple small chunks
    let threads = num_cpus::get();
    eprintln!("Using {} threads with {} chunks ({} MB each)", 
        threads, num_chunks, chunk_size / (1024 * 1024));
    
    // Share header buffer between threads using Arc
    use std::sync::Arc;
    let header_buf_arc = Arc::new(header_buf);
    
    // Create work queue using atomic counter
    use std::sync::atomic::{AtomicUsize, Ordering};
    let next_chunk = Arc::new(AtomicUsize::new(0));
    
    // Spawn worker threads
    let mut handles = Vec::with_capacity(threads);
    
    for thread_id in 0..threads {
        let path = path.to_string();
        let header_buf_clone = Arc::clone(&header_buf_arc);
        let next_chunk_clone = Arc::clone(&next_chunk);
        let file_size = file_size;
        let header_size = header_size;
        let chunk_size = chunk_size;
        
        let handle = thread::spawn(move || {
            let mut shard = Shard::new();
            let mut total_lines_processed = 0;
            
            // Each thread opens its own file handle
            match File::open(&path) {
                Ok(mut file) => {
                    // Process chunks using atomic counter
                    loop {
                        let chunk_idx = next_chunk_clone.fetch_add(1, Ordering::Relaxed);
                        if chunk_idx >= num_chunks {
                            break;
                        }
                        
                        let start = header_size + (chunk_idx as u64) * chunk_size;
                        let end = std::cmp::min(start + chunk_size, file_size);
                        
                        if start >= end {
                            continue;
                        }
                        
                        // Seek to chunk start
                        if let Err(e) = file.seek(std::io::SeekFrom::Start(start)) {
                            eprintln!("Thread {} seek error: {}", thread_id, e);
                            continue;
                        }
                        
                        // Read chunk data (max 100MB)
                        let chunk_size_bytes = (end - start) as usize;
                        let mut chunk = vec![0; chunk_size_bytes];
                        
                        if let Err(e) = file.read_exact(&mut chunk) {
                            eprintln!("Thread {} read error: {}", thread_id, e);
                            continue;
                        }
                        
                        // Adjust chunk boundaries to line boundaries
                        let mut chunk_start = 0;
                        let mut chunk_end = chunk.len();
                        
                        if chunk_idx > 0 {
                            // Find first newline after start
                            while chunk_start < chunk.len() && chunk[chunk_start] != b'\n' {
                                chunk_start += 1;
                            }
                            if chunk_start < chunk.len() {
                                chunk_start += 1; // Skip the newline
                            }
                        }
                        
                        if end < file_size {
                            // Find last newline before end (for all but last chunk)
                            while chunk_end > 0 && chunk[chunk_end - 1] != b'\n' {
                                chunk_end -= 1;
                            }
                        }
                        
                        if chunk_start >= chunk_end {
                            continue;
                        }
                        
                        // Create CSV reader for this chunk (need to include header)
                        let header_len = header_buf_clone.len();
                        let data_len = chunk_end - chunk_start;
                        let total_len = header_len + data_len;
                        
                        let mut buffer = Vec::with_capacity(total_len);
                        buffer.extend_from_slice(&header_buf_clone);
                        buffer.extend_from_slice(&chunk[chunk_start..chunk_end]);
                        
                        let mut rdr = csv::ReaderBuilder::new()
                            .delimiter(delim)
                            .has_headers(true)
                            .flexible(true)
                            .from_reader(buffer.as_slice());
                        
                        let mut chunk_lines = 0;
                        // Parse rows
                        for rec in rdr.deserialize::<Row>() {
                            match rec {
                                Ok(row) => {
                                    chunk_lines += 1;
                                    shard.add(&row);
                                }
                                Err(_) => {
                                    // Skip bad rows
                                }
                            }
                        }
                        
                        total_lines_processed += chunk_lines;
                        
                        // Report progress every 10 chunks
                        if chunk_idx % 10 == 0 {
                            eprintln!("Thread {}: processed chunk {}/{} ({} lines total)", 
                                thread_id, chunk_idx + 1, num_chunks, total_lines_processed);
                        }
                    }
                    
                    eprintln!("Thread {} completed: processed {} total lines", thread_id, total_lines_processed);
                }
                Err(e) => {
                    eprintln!("Thread {} failed to open file: {}", thread_id, e);
                }
            }
            
            shard
        });
        
        handles.push(handle);
    }
    
    // Merge results
    let mut total = Shard::new();
    for handle in handles {
        let shard = handle.join().expect("thread panicked");
        total.merge(shard);
    }
    
    eprintln!("Parallel streaming parsing completed successfully");
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
            // Use adaptive parser based on file size
            match parse_csv_adaptive(path, delim) {
                Ok(shard) => shard,
                Err(e) => {
                    eprintln!("Adaptive parsing failed: {}", e);
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
    // Calculate duration in hours and minutes
    let duration_hours = dur / 3600.0;
    let duration_minutes = dur / 60.0;
    
    // Format start date from timestamp
    let start_timestamp = if overall.first_ts == i64::MAX { 0 } else { overall.first_ts };
    let start_date = if start_timestamp > 0 {
        let seconds = start_timestamp / 1000;
        let nanos = ((start_timestamp % 1000) * 1_000_000) as u32;
        let dt = chrono::DateTime::from_timestamp(seconds, nanos).unwrap_or_default();
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        "N/A".to_string()
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
        p75_ms: overall.q(0.75),
        p85_ms: overall.q(0.85),
        p90_ms: overall.q(0.90),
        p95_ms: overall.q(0.95),
        p99_ms: overall.q(0.99),
        duration_sec: dur,
        duration_hours,
        duration_minutes,
        start_timestamp,
        start_date,
        tps,
        kbps_recv: kbps(overall.recv_bytes, dur),
        kbps_sent: kbps(overall.sent_bytes, dur),
    };
    
    // Prepare label outputs
    let mut label_outs: Vec<LabelOut> = total.per_label
        .into_iter()
        .map(|(label, agg)| {
            let dur = agg.duration_secs();
            let tps = if dur > 0.0 { agg.count as f64 / dur } else { 0.0 };
            LabelOut {
                label,
                count: agg.count,
                fails: agg.fails,
                error_pct: if agg.count == 0 { 0.0 } else { (agg.fails as f64 / agg.count as f64) * 100.0 },
                avg_ms: agg.avg_ms(),
                min_ms: if agg.min_elapsed == u64::MAX { 0 } else { agg.min_elapsed },
                max_ms: agg.max_elapsed,
                p50_ms: agg.q(0.50),
                p75_ms: agg.q(0.75),
                p85_ms: agg.q(0.85),
                p90_ms: agg.q(0.90),
                p95_ms: agg.q(0.95),
                p99_ms: agg.q(0.99),
                tps,
                kbps_recv: kbps(agg.recv_bytes, dur),
                kbps_sent: kbps(agg.sent_bytes, dur),
            }
        })
        .collect();
    label_outs.sort_by(|a, b| b.count.cmp(&a.count));
    
    // Prepare error type outputs
    let mut err_outs: Vec<ErrTypeOut> = total.error_types
        .into_iter()
        .map(|((response_code, response_message), count)| {
            let error_pct = if overall.fails == 0 { 0.0 } else { (count as f64 / overall.fails as f64) * 100.0 };
            let sample_pct = if overall.count == 0 { 0.0 } else { (count as f64 / overall.count as f64) * 100.0 };
            ErrTypeOut {
                response_code,
                response_message,
                count,
                error_pct,
                sample_pct,
            }
        })
        .collect();
    err_outs.sort_by(|a, b| b.count.cmp(&a.count));
    
    let title = input_path.unwrap_or("stdin");
    let html = html_parser::render_html(overall_out, label_outs, err_outs, title);
    let output_path = args.get(3).map(|s| s.as_str()).unwrap_or("jtl_report.html");
    std::fs::write(output_path, html)?;
    
    let total_time = t0.elapsed();
    eprintln!("Total processing time: {:.2}s", total_time.as_secs_f64());

    Ok(())
}
