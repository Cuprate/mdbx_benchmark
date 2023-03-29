use std::{time::{Duration, Instant}, path::{Path, PathBuf}};

use indicatif::ProgressBar;
use libmdbx::{DatabaseKind, WriteFlags, Database, DatabaseBuilder, DatabaseFlags, Mode, Geometry, SyncMode, TableFlags};
use rand::{Rng, rngs::ThreadRng, seq::SliceRandom, RngCore};

use crate::{BENCHMARK_TABLES_KEY_SIZE, BENCHMARK_TABLES_DATA_SIZE, BENCHMARK_TABLES_COMMITS, BENCHMARK_TABLES, MAX_MAP_SIZE};

pub fn recreate_db<R: DatabaseKind>(path: &Path, sync_mode: SyncMode) -> Database<R> {

	// Erase last database
	if std::fs::read_dir(path).is_ok() {
		std::fs::remove_dir_all(path).expect("Can't delete previous database");
	}

	// Recreate the database
	let mut db_builder: DatabaseBuilder<R> = libmdbx::Database::new();
	let db = db_builder.set_max_tables(14).set_max_readers(32)
	.set_flags(DatabaseFlags::from(Mode::ReadWrite { sync_mode }))
	.set_geometry(Geometry { size: Some(0..MAX_MAP_SIZE), growth_step: Some(1024isize.pow(2)*256), shrink_threshold: None, page_size: None })
	.open(path)
	.expect("failed to open database");

	// Create tables
	let rw_tx = db.begin_rw_txn().unwrap();
	(0..2).for_each(|t| { rw_tx.create_table(Some(BENCHMARK_TABLES[t]), TableFlags::empty()).unwrap(); });
	rw_tx.commit().unwrap();
	
	db
}

pub fn recreate_db_dup<R: DatabaseKind>(path: &Path, sync_mode: SyncMode) -> Database<R> {

	// Erase last database
	if std::fs::read_dir(path).is_ok() {
		std::fs::remove_dir_all(path).expect("Can't delete previous database");
	}

	// Recreate the database
	let mut db_builder: DatabaseBuilder<R> = libmdbx::Database::new();
	let db = db_builder.set_max_tables(14).set_max_readers(32)
	.set_flags(DatabaseFlags::from(Mode::ReadWrite { sync_mode }))
	.set_geometry(Geometry { size: Some(0..MAX_MAP_SIZE), growth_step: Some(1024isize.pow(2)*256), shrink_threshold: None, page_size: None })
	.open(path)
	.expect("failed to open database");

	// Create tables
	let rw_tx = db.begin_rw_txn().unwrap();
	(0..2).for_each(|t| { rw_tx.create_table(Some(BENCHMARK_TABLES[t]), TableFlags::DUP_SORT.union(TableFlags::DUP_FIXED)).unwrap(); });
	rw_tx.commit().unwrap();
	
	db
}

pub fn benchmark_put_small_table<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	measurements: &mut (Vec<f64>, u64)) 
	-> Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[0]],[u8; BENCHMARK_TABLES_DATA_SIZE[0]])>
{
	// Open table
	let ro_tx = db.begin_ro_txn().expect("failed to generate the ro tx");
	let table = ro_tx.open_table(Some(BENCHMARK_TABLES[0])).expect("failed to open table");
	ro_tx.prime_for_permaopen(table);
	let table = ro_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Generate random data
	let data: Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[0]],[u8; BENCHMARK_TABLES_DATA_SIZE[0]])> = (0..BENCHMARK_TABLES_COMMITS[0]).map(|_| rng.gen()).collect(); 
	let mut data_iter = data.iter();

	let data_returned = data.clone();

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[0]/job_divide as usize)).for_each(|_| {

		let rw_tx = db.begin_rw_txn().unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			rw_tx.put(&table, data.0, data.1, WriteFlags::empty()).unwrap();
		});

		rw_tx.commit().unwrap();
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());
	data_returned
}


pub fn benchmark_put_large_table<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	measurements: &mut (Vec<f64>, u64)) 
	-> Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[1]],[u8; BENCHMARK_TABLES_DATA_SIZE[1]])>
{
	// Open table
	let ro_tx = db.begin_ro_txn().expect("failed to generate the ro tx");
	let table = ro_tx.open_table(Some(BENCHMARK_TABLES[1])).expect("failed to open table");
	ro_tx.prime_for_permaopen(table);
	let table = ro_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Generate random data
	let data: Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[1]],[u8; BENCHMARK_TABLES_DATA_SIZE[1]])> = (0..BENCHMARK_TABLES_COMMITS[1])
		.map(|_| {
			let mut buf: Box<[u8; BENCHMARK_TABLES_DATA_SIZE[1]]> = Box::new([0u8; BENCHMARK_TABLES_DATA_SIZE[1]]); 
			rng.fill_bytes(&mut *buf); 
			(rng.gen(),*buf)
		}).collect(); 
	let mut data_iter = data.iter();

	let data_cloned = data.clone();

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[1]/job_divide as usize)).for_each(|_| {

		let rw_tx = db.begin_rw_txn().unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			rw_tx.put(&table, data.0, data.1, WriteFlags::empty()).unwrap();
		});

		rw_tx.commit().unwrap();
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());

	data_cloned
}

pub fn benchmark_read_large_table<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	mut data: Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[1]],[u8; BENCHMARK_TABLES_DATA_SIZE[1]])>,
	measurements: &mut (Vec<f64>, u64)) 
{
	// Open table
	let ro_tx = db.begin_ro_txn().expect("failed to generate the ro tx");
	let table = ro_tx.open_table(Some(BENCHMARK_TABLES[1])).expect("failed to open table");
	ro_tx.prime_for_permaopen(table);
	let table = ro_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Generate random data
	data.shuffle(rng);
	let mut data_iter = data.iter();

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[1]/job_divide as usize)).for_each(|_| {

		let ro_tx = db.begin_ro_txn().unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			let _: [u8; BENCHMARK_TABLES_DATA_SIZE[1]] = ro_tx.get(&table, &data.0).unwrap().unwrap();
		});
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());
}

pub fn benchmark_read_small_table<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	mut data: Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[0]],[u8; BENCHMARK_TABLES_DATA_SIZE[0]])>,
	measurements: &mut (Vec<f64>, u64)) 
{
	// Open table
	let ro_tx = db.begin_ro_txn().expect("failed to generate the ro tx");
	let table = ro_tx.open_table(Some(BENCHMARK_TABLES[0])).expect("failed to open table");
	ro_tx.prime_for_permaopen(table);
	let table = ro_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Generate random data
	data.shuffle(rng);
	let mut data_iter = data.iter();

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[0]/job_divide as usize)).for_each(|_| {

		let ro_tx = db.begin_ro_txn().unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			let _: Option<[u8; BENCHMARK_TABLES_DATA_SIZE[0]]> = ro_tx.get(&table, &data.0).unwrap();
		});
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());
}

pub fn benchmark_read_small_table_dup<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	mut data: Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]])>,
	measurements: &mut (Vec<f64>, u64)) 
{
	// Open table
	let ro_tx = db.begin_ro_txn().expect("failed to generate the ro tx");
	let table = ro_tx.open_table(Some(BENCHMARK_TABLES[0])).expect("failed to open table");
	ro_tx.prime_for_permaopen(table);
	let table = ro_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Generate random data
	data.shuffle(rng);
	let mut data_iter = data.iter();

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[0]/job_divide as usize)).for_each(|_| {

		let ro_tx = db.begin_ro_txn().unwrap();
		let mut cursor = ro_tx.cursor(&table).unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			let _: Option<[u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]]> = cursor.get_both(&[0u8; 0], &data[..BENCHMARK_TABLES_KEY_SIZE[0]]).unwrap();
		});
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());
}

pub fn benchmark_put_small_table_dup<R: DatabaseKind>(
	pg: &ProgressBar,
	db: &libmdbx::Database<R>, 
	rng: &mut ThreadRng, 
	job_divide: u64, 
	measurements: &mut (Vec<f64>, u64)) 
	-> Vec<([u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]])>
{
	// Generate random data
	let data: Vec<[u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]]> = (0..BENCHMARK_TABLES_COMMITS[0]).map(|_| {
		let mut buf: [u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]] = [0u8; BENCHMARK_TABLES_KEY_SIZE[0]+BENCHMARK_TABLES_DATA_SIZE[0]];
		rng.fill_bytes(&mut buf);
		buf
	}).collect(); 
	let mut data_iter = data.iter();

	let data_returned = data.clone();

	// Open table
	let rw_tx = db.begin_rw_txn().expect("failed to generate the ro tx");
	let table = rw_tx.open_table(Some(BENCHMARK_TABLES[0])).expect("failed to open table");
	rw_tx.prime_for_permaopen(table);
	let table = rw_tx.commit_and_rebind_open_dbs().unwrap().1.remove(0);

	// Measurement
	let instant = Instant::now();

	(0..(BENCHMARK_TABLES_COMMITS[0]/job_divide as usize)).for_each(|_| {

		// Open cursor
		let rw_tx = db.begin_rw_txn().unwrap();
		let mut cursor = rw_tx.cursor(&table).unwrap();
					
		(0..job_divide).for_each(|_| {

			let data = data_iter.next().unwrap();
			cursor.put(&[0u8; 0], data, WriteFlags::empty()).unwrap();
		});

		rw_tx.commit().unwrap();
		pg.inc(job_divide);
	});

	measurements.0.push(instant.elapsed().as_secs_f64());
	data_returned
}