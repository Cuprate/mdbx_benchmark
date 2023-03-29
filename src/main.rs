//#![allow(dead_code, unused_variables)]
extern crate libmdbx;
extern crate rand;

use std::{sync::atomic::AtomicU64, sync::Arc, time::{Duration, Instant}, process::exit, io::{stdin}, path::{Path, PathBuf}, thread::spawn, fs::File, fmt::Write};
use indicatif::{ProgressStyle, ProgressState, ProgressBar};
use libmdbx::{DatabaseBuilder, WriteMap, TableFlags, WriteFlags, Geometry, SyncMode, DatabaseFlags, Mode, DatabaseKind, NoWriteMap};
use clap::Parser;
use mdbx_bench::{benchmark_put_large_table, benchmark_put_small_table, recreate_db, benchmark_read_large_table, benchmark_read_small_table, benchmark_put_small_table_dup, recreate_db_dup, benchmark_read_small_table_dup};
use serde::{Serialize};

pub mod mdbx_bench;

const BENCHMARK_RANGE: u64 = 3;
const BENCHMARK_MAX: u64 = 3000000; 
const DB_PATH: &str = "benchmark.mdbx";
const JOB_DIVIDE: u64 = 1000;
const MAX_MAP_SIZE: usize = 1024usize.pow(4)*4;
const GROWTH_STEP: Option<isize> = None;
const TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ ";

const BENCHMARK_TABLES: [&str; 2] =
[
	"sim_blockheight",
	"sim_blocks",
];

const BENCHMARK_TABLES_KEY_SIZE: [usize; 2] = 
[
	8, 
	32
];

const BENCHMARK_TABLES_DATA_SIZE: [usize; 2] = 
[
	32,
	60*1024,
];

const BENCHMARK_TABLES_COMMITS: [usize; 2] =
[
	3000000,
	40000,
];

#[derive(Parser, Debug)]
#[command(author, version , about, long_about = None)]
struct Args {
	/// Path to use to create the database
	#[arg(long)]
	path: PathBuf,
	/// Useless do not enable
	#[arg(long,  default_value_t = false)]
	lmdb: bool,
	/// Enable mdbx benchmark
	#[arg(long, default_value_t = false)]
	mdbx: bool,
	/// Benchmark with a different job division value
	#[arg(long, default_value_t = false)]
	extended: bool,
}

#[derive(Serialize)]
struct Benchmark {
	name: String,
	durations_size: Vec<(Vec<([f64; 2], u64)>, String)>,
}

fn main() {

	// ---- Arguments ----

	let mut args = Args::parse();

	if !args.lmdb && !args.mdbx {
		println!("None of the modules have been selected, benchmarking both\n");
		(args.lmdb, args.mdbx) = (true, true);
	}

	// ---- Info & Consent ----

	println!("Thanks you for doing this benchmark. Note that this benchmark is going to test 10 differents configurations for each database engine 3 times, and that the test is going write 3GB at each iteration. This is likely to hurt the lifespan of your SSD (unless your bencharmking on an HDD) and add fragmentation to your filesystem. If you've your ssd for more than 5 years you can cancel it now. The benchmark is likely to run between 30 minutes & 2 hours (if you've an hdd). In this time, please do not copy files on your disk, it will create artifacts in the report");

	let stdin = stdin();
	println!("\nAre you sure you want to continue ? [Y/N] ");
	let mut input = String::new();
	stdin.read_line(&mut input).unwrap();

	if input.starts_with('N') | input.starts_with('n') {
		println!("\n Benchmark cancelled");
		exit(0);
	}

	if args.mdbx {

		// Create the benchmark struct
		let mut benchmarks: Vec<Benchmark> = Vec::new();
		
		// Traditionnal transactional key/pair insert
		let mut benchmark_mdbx_trad = Benchmark { name: String::from("Benchmark MDBX Traditionnal Key/Pair"), durations_size: Vec::new() };

		let put_benchmark1 = mdbx_benchmark_put::<WriteMap>("T K/P SM::Durable | WriteMap", args.path.clone(), SyncMode::Durable, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((put_benchmark1, "T K/P SM::Durable | WriteMap".to_string()));
		println!("Changing SyncMode");
		let put_benchmark2 = mdbx_benchmark_put::<WriteMap>("T K/P SM::SafeNoSync | WriteMap", args.path.clone(), SyncMode::SafeNoSync, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((put_benchmark2, "T K/P SM::SafeNoSync | WriteMap".to_string()));
		println!("Changing to NoWriteMap");
		let put_benchmark3 = mdbx_benchmark_put::<NoWriteMap>("T K/P SM::Durable | NoWriteMap", args.path.clone(), SyncMode::Durable, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((put_benchmark3, "T K/P SM::Durable | NoWriteMap".to_string()));
		println!("Changing SyncMode");
		let put_benchmark4 = mdbx_benchmark_put::<NoWriteMap>("T K/P SM::SafeNoSync | NoWriteMap", args.path.clone(), SyncMode::SafeNoSync, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((put_benchmark4, "T K/P SM::SafeNoSync | NoWriteMap".to_string()));
		if args.extended {
			let put_benchmark5 = mdbx_benchmark_put::<WriteMap>("T K/P SM::Durable | WriteMap | J10K", args.path.clone(), SyncMode::Durable, 10000, 3);
			benchmark_mdbx_trad.durations_size.push((put_benchmark5, "T K/P SM::Durable | WriteMap | J10K".to_string()));
			println!("Changing SyncMode");
			let put_benchmark6 = mdbx_benchmark_put::<WriteMap>("T K/P SM::SafeNoSync | WriteMap | J10K", args.path.clone(), SyncMode::SafeNoSync, 10000, 3);
			benchmark_mdbx_trad.durations_size.push((put_benchmark6, "T K/P SM::SafeNoSync | WriteMap | J10K".to_string()));
		}

		// Traditionnal transactional key/pair get
		let read_benchmark1 = mdbx_benchmark_read::<WriteMap>("T K/P Read | WriteMap", args.path.clone(), SyncMode::UtterlyNoSync, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((read_benchmark1, "T K/P Read | WriteMap".to_string()));
		let read_benchmark2 = mdbx_benchmark_read::<NoWriteMap>("T K/P Read | NoWriteMap", args.path.clone(), SyncMode::UtterlyNoSync, 1000, 3);
		benchmark_mdbx_trad.durations_size.push((read_benchmark2, "T K/P Read | NoWriteMap".to_string()));

		benchmarks.push(benchmark_mdbx_trad);
		
		// Zerokval & dummykeys insert
		let mut benchmark_mdbx_zkdup = Benchmark { name: String::from("Benchmark MDBX ZeroKey value w cursors"), durations_size: Vec::new() };
		let put_benchmark1 = mdbx_benchmark_put_dup::<WriteMap>("ZKey SM::Durable | WriteMap", args.path.clone(), SyncMode::Durable, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((put_benchmark1, "ZKey SM::Durable | WriteMap".to_string()));
		println!("Changing SyncMode");
		let put_benchmark2 = mdbx_benchmark_put_dup::<WriteMap>("ZKey SM::SafeNoSync | WriteMap", args.path.clone(), SyncMode::SafeNoSync, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((put_benchmark2, "ZKey SM::SafeNoSync | WriteMap".to_string()));
		println!("Changing to NoWriteMap");
		let put_benchmark3 = mdbx_benchmark_put_dup::<NoWriteMap>("ZKey SM::Durable | NoWriteMap", args.path.clone(), SyncMode::Durable, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((put_benchmark3, "ZKey SM::Durable | NoWriteMap".to_string()));
		println!("Changing SyncMode");
		let put_benchmark4 = mdbx_benchmark_put_dup::<NoWriteMap>("ZKey SM::SafeNoSync | NoWriteMap", args.path.clone(), SyncMode::SafeNoSync, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((put_benchmark4, "ZKey SM::SafeNoSync | NoWriteMap".to_string()));
		if args.extended {
			let put_benchmark5 = mdbx_benchmark_put_dup::<WriteMap>("ZKey SM::Durable | WriteMap | J10K", args.path.clone(), SyncMode::Durable, 10000, 3);
			benchmark_mdbx_zkdup.durations_size.push((put_benchmark5, "ZKey SM::Durable | WriteMap | J10K".to_string()));
			println!("Changing SyncMode");
			let put_benchmark6 = mdbx_benchmark_put_dup::<WriteMap>("ZKey SM::SafeNoSync | WriteMap | J10K", args.path.clone(), SyncMode::SafeNoSync, 10000, 3);
			benchmark_mdbx_zkdup.durations_size.push((put_benchmark6, "ZKey SM::SafeNoSync | WriteMap | J10K".to_string()));
		}

		// Zerokval & dummykeys get
		let read_benchmark1 = mdbx_benchmark_read_dup::<WriteMap>("ZKey Read | WriteMap", args.path.clone(), SyncMode::UtterlyNoSync, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((read_benchmark1, "ZKey Read | WriteMap".to_string()));
		let read_benchmark2 = mdbx_benchmark_read_dup::<NoWriteMap>("ZKey Read | NoWriteMap", args.path.clone(), SyncMode::UtterlyNoSync, 1000, 3);
		benchmark_mdbx_zkdup.durations_size.push((read_benchmark2, "ZKey Read | NoWriteMap".to_string()));

		benchmarks.push(benchmark_mdbx_zkdup);

		let yaml_trad_mdbx = serde_json::to_string(&benchmarks).unwrap();
		let mut file_path = args.path.clone();
		file_path.push("mdbx_report.json");
		if std::fs::File::open(&file_path).is_ok() {
			std::fs::remove_file(&file_path).expect("Can't delete previous report");
		}
		let mut file = std::fs::File::create(&file_path).unwrap();
		std::io::Write::write_all(&mut file, yaml_trad_mdbx.as_bytes()).expect("Failed to write report");
		println!("Thanks a lot for having destroyed your disk. The final report can be found under : {}\nIf you have a Github account please post your benchmark here : https://github.com/Cuprate/mdbx_benchmark/issues. Don't forget to add the <Benchmark> label and tell us what your disk is (SSD, NVMe, HDD, USB?, MicroSD??, FloppyDisk?????). Otherwise, if you don't have a github account, you can also join our Revolt server, contact us on matrix (see cargo.toml) or directly send us an email (there are our GPG keys). You can find all these informations here : https://github.com/Cuprate/cuprate", file_path.clone().as_path().display());
	}

	
}

fn mdbx_benchmark_put<R: DatabaseKind>(
	msg: &'static str,
	path: PathBuf,
	sync_mode: SyncMode,
	job_divide: u64,
	num_iter: u64,)
	-> Vec<([f64; 2], u64)> 
{
	let mut measurements: Vec<([f64; 2], u64)> = Vec::new();

	(0..num_iter).for_each(|iteration| {

		let mut path = path.clone();
		path.push("benchmark.mdbx");

		let shared_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

		let handle = spawn(move || {

			let mut measurements: (Vec<f64>, u64) = (Vec::new(), 0u64);

			let db = recreate_db::<R>(&path, sync_mode);

			// Start RNG Thread
			let mut rng = rand::thread_rng();

			// Actual benchmark
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]+BENCHMARK_TABLES_COMMITS[1]) as u64);

			let _ = benchmark_put_large_table(&progress_bar, &db, &mut rng, job_divide, &mut measurements);
			let _ = benchmark_put_small_table(&progress_bar, &db, &mut rng, job_divide, &mut measurements);

			progress_bar.finish();
			
			// Get the size & send the duration
			path.push("mdbx.dat");
			let file = File::open(path).unwrap();
			let size = file.metadata().unwrap().len();
			measurements.1 = size;
			measurements
		});

		let mut res = handle.join().unwrap();
		let array = [res.0.remove(0),res.0.remove(0)];
		measurements.push((array,res.1))
	});	
	
	measurements
}

fn mdbx_benchmark_put_dup<R: DatabaseKind>(
	msg: &'static str,
	path: PathBuf,
	sync_mode: SyncMode,
	job_divide: u64,
	num_iter: u64,)
	-> Vec<([f64; 2], u64)> 
{
	let mut measurements: Vec<([f64; 2], u64)> = Vec::new();

	(0..num_iter).for_each(|iteration| {

		let mut path = path.clone();
		path.push("benchmark.mdbx");

		let shared_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

		let handle = spawn(move || {

			let mut measurements: (Vec<f64>, u64) = (Vec::new(), 0u64);

			let db = recreate_db_dup::<R>(&path, sync_mode);

			// Start RNG Thread
			let mut rng = rand::thread_rng();

			// Actual benchmark
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]) as u64);

			println!("small db");
			let _ = benchmark_put_small_table_dup(&progress_bar, &db, &mut rng, job_divide, &mut measurements);
			println!("large db");

			progress_bar.finish();
			
			// Get the size & send the duration
			path.push("mdbx.dat");
			let file = File::open(path).unwrap();
			let size = file.metadata().unwrap().len();
			measurements.1 = size;
			measurements
		});

		let mut res = handle.join().unwrap();
		let array = [0f64,res.0.remove(0)];
		measurements.push((array,res.1))
		
	});	
	
	measurements
}

fn mdbx_benchmark_read<R: DatabaseKind>(
	msg: &'static str,
	path: PathBuf,
	sync_mode: SyncMode,
	job_divide: u64,
	num_iter: u64,)
	-> Vec<([f64; 2], u64)>
{
	let mut measurements: Vec<([f64; 2], u64)> = Vec::new();

	(0..num_iter).for_each(|iteration| {

		let mut path = path.clone();
		path.push("benchmark.mdbx");

		let shared_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

		let handle = spawn(move || {

			let mut measurements: (Vec<f64>, u64) = (Vec::new(), 0u64);

			let db = recreate_db::<R>(&path, sync_mode);

			// Start RNG Thread
			let mut rng = rand::thread_rng();

			// Actual benchmark
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]+BENCHMARK_TABLES_COMMITS[1]) as u64);

			let data_large = benchmark_put_large_table(&progress_bar, &db, &mut rng, job_divide, &mut measurements);
			let data_small = benchmark_put_small_table(&progress_bar, &db, &mut rng, job_divide, &mut measurements);
			progress_bar.finish();
			measurements.0 = Vec::new();
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]+BENCHMARK_TABLES_COMMITS[1]) as u64);
			benchmark_read_large_table(&progress_bar, &db, &mut rng, job_divide, data_large,&mut measurements);
			benchmark_read_small_table(&progress_bar, &db, &mut rng, job_divide, data_small,&mut measurements);
			progress_bar.finish();
			
			// Get the size & send the duration
			path.push("mdbx.dat");
			let file = File::open(path).unwrap();
			let size = file.metadata().unwrap().len();
			measurements.1 = size;
			measurements
		});

		let mut res = handle.join().unwrap();
		let array = [res.0.remove(0),res.0.remove(0)];
		measurements.push((array,res.1))
		
	});	
	
	measurements
}

fn mdbx_benchmark_read_dup<R: DatabaseKind>(
	msg: &'static str,
	path: PathBuf,
	sync_mode: SyncMode,
	job_divide: u64,
	num_iter: u64,)
	-> Vec<([f64; 2], u64)>
{
	let mut measurements: Vec<([f64; 2], u64)> = Vec::new();

	(0..num_iter).for_each(|iteration| {

		let mut path = path.clone();
		path.push("benchmark.mdbx");

		let shared_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

		let handle = spawn(move || {

			let mut measurements: (Vec<f64>, u64) = (Vec::new(), 0u64);

			let db = recreate_db_dup::<R>(&path, sync_mode);

			// Start RNG Thread
			let mut rng = rand::thread_rng();

			// Actual benchmark
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]) as u64);

			let data_small = benchmark_put_small_table_dup(&progress_bar, &db, &mut rng, job_divide, &mut measurements);
			progress_bar.finish();
			measurements.0 = Vec::new();
			let progress_bar = get_progress_bar(iteration, msg, (BENCHMARK_TABLES_COMMITS[0]) as u64);
			benchmark_read_small_table_dup(&progress_bar, &db, &mut rng, job_divide, data_small,&mut measurements);
			progress_bar.finish();
			
			// Get the size & send the duration
			path.push("mdbx.dat");
			let file = File::open(path).unwrap();
			let size = file.metadata().unwrap().len();
			measurements.1 = size;
			measurements
		});

		let mut res = handle.join().unwrap();
		let array = [0f64,res.0.remove(0)];
		measurements.push((array,res.1))
		
	});	
	
	measurements
}

fn get_progress_bar(iteration: u64, msg: &str, len: u64) -> ProgressBar {
	let progress_bar = ProgressBar::new(len);
			progress_bar.set_style(ProgressStyle::with_template("{spinner:.green} {msg:<30} [{elapsed_precise}] {wide_bar:.cyan/blue} | {pos}/{len} ({eta})")
			.unwrap()			
			.with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
			.progress_chars("██░"));
			progress_bar.set_message(format!("{} Iteration {}#", msg, iteration));
			progress_bar.enable_steady_tick(Duration::from_millis(80));
	
	progress_bar
}