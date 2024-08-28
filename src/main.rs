use async_compression::tokio::write::GzipEncoder;
use run_info::RunInfo;
use sample_sheet::SampleSheet;
use serde_xml_rs::from_str;

use std::{fs, sync::Arc};
use tokio::{
    fs::File,
    io::{self, AsyncWriteExt, BufWriter},
    spawn,
    sync::{mpsc, Semaphore},
    task::spawn_blocking,
};


mod bcl_iterator;
mod run_info;
mod sample_sheet;

fn get_tile_numbers(path: &str) -> Vec<u32> {
    let mut tile_numbers = Vec::new();
    let basecalls_path: String = format!("{}/Data/Intensities/BaseCalls/L001/C1.1/", path);

    for entry in fs::read_dir(basecalls_path).expect("could not read directory") {
        let entry = entry.expect("could not get entry");
        let path = entry.path();
        let path = path.to_str().expect("could not convert path to string");
        let path = path.split("/").last().expect("could not get last element");
        let path = path.split(".").next().expect("could not get first element");
        let path = path.split("_").collect::<Vec<&str>>();
        let tile_number = path[2].parse::<u32>().expect("could not parse tile number");
        tile_numbers.push(tile_number);
    }
    tile_numbers
}

async fn process_bcl_files(
    path: &str,
    run_info: Arc<RunInfo>,
    sample_sheet: Arc<SampleSheet>,
    lane: u32,
    tile: u32,
    sender: mpsc::Sender<(usize, usize, String)>,
) {
    println!("Processing lane {} tile {}", lane, tile);
    let total_cycles: u32 = run_info.run.reads.calculate_total_cycles();

    let bcl_file_iterator: bcl_iterator::BclIterator = bcl_iterator::BclIterator::new(
        total_cycles,
        path.to_string(),
        &run_info.run.reads,
        lane,
        tile,
    );

    let first_cluster_count = bcl_file_iterator.cluster_count[0];
    let mut read_cache: Vec<String> = vec![String::new(); run_info.run.reads.read.len()];
    let mut qual_cache: Vec<String> = vec![String::new(); run_info.run.reads.read.len()];

    let mut processed = 0;

    for r in bcl_file_iterator {
        let mut index: u16 = 0;
        let mut index_2: u16 = 0;
        for (i, (base, qual, pos, is_idx)) in r.iter().enumerate() {
            read_cache[*pos as usize].push(*base);
            qual_cache[*pos as usize].push((*qual + 33) as char);

            if *is_idx && pos == &1 {
                index = index << 2;
                index += match base {
                    'A' => 0,
                    'C' => 1,
                    'G' => 2,
                    'T' => 3,
                    _ => 0,
                };
            }

            if *is_idx && pos == &2 {
                index_2 = index_2 << 2;
                index_2 += match base {
                    'A' => 0,
                    'C' => 1,
                    'G' => 2,
                    'T' => 3,
                    _ => 0,
                };
            }
        }

        let sample_r1_match = sample_sheet.get_r1_sample_idx(index);
        let sample_r2_match = sample_sheet.get_r2_sample_idx(index_2);

        let sample = if sample_r1_match == sample_r2_match {
            sample_r1_match
        } else {
            0
        };

        for actual_read in read_cache.iter().enumerate() {
            let record = format!(
                "@{}\n{}\n+\n{}\n",
                sample, actual_read.1, qual_cache[actual_read.0]
            );
            sender
                .send((sample as usize, actual_read.0, record))
                .await
                .expect("Failed to send record");
        }

        for i in 0..run_info.run.reads.read.len() {
            read_cache[i].clear();
            qual_cache[i].clear();
        }

        processed += 1;
        if processed % 10000 == 0 {
            println!(
                "Tile {} < processed {} records out of {} ({}%)",
                tile,
                processed,
                first_cluster_count,
                (processed as f64 / first_cluster_count as f64) * 100.0
            );
        }
    }

    println!("Finished processing lane {} tile {}", lane, tile);
}

async fn writer_task(
    mut receiver: mpsc::Receiver<(usize, usize, String)>,
    mut output_files: Vec<Vec<BufWriter<GzipEncoder<File>>>>,
) {
    let mut buffers: Vec<Vec<Vec<String>>> =
        vec![vec![Vec::new(); output_files[0].len()]; output_files.len()];
    const BATCH_SIZE: usize = 1_000;

    let mut batches = 0;
    while let Some((sample_idx, read_idx, record)) = receiver.recv().await {
        buffers[sample_idx][read_idx].push(record);

        if buffers[sample_idx][read_idx].len() >= BATCH_SIZE {
            let data = buffers[sample_idx][read_idx].join("");
            output_files[sample_idx][read_idx]
                .write_all(data.as_bytes())
                .await
                .expect("Failed to write data");
            buffers[sample_idx][read_idx].clear();

            batches += 1;
            if batches % 10 == 0 {
                println!("Processed {} batches", batches);
            }
        }
    }

    // Write any remaining data
    for (sample_idx, sample_buffers) in buffers.iter_mut().enumerate() {
        for (read_idx, buffer) in sample_buffers.iter_mut().enumerate() {
            if !buffer.is_empty() {
                let data = buffer.join("");
                output_files[sample_idx][read_idx]
                    .write_all(data.as_bytes())
                    .await
                    .expect("Failed to write data");
            }
        }
    }

    for sample_files in output_files.iter_mut() {
        for writer in sample_files.iter_mut() {
            writer.flush().await.expect("Failed to flush data");
        }
    }
}

#[tokio::main]
async fn main() {
    
    let path = std::env::args().nth(1).expect("missing path to run data");

    let tile_numbers = get_tile_numbers(&path);
    println!("Tile numbers: {:?}", tile_numbers);
    let run_info_path = format!("{}/RunInfo.xml", path);

    println!("Reading RunInfo.xml from {}", run_info_path);
    let run_info = std::fs::read_to_string(run_info_path).expect("could not read RunInfo.xml");
    let run_info: RunInfo = from_str(&run_info).expect("could not parse RunInfo.xml");

    let lanes = run_info.run.flowcell_layout.lane_count;

    let sample_sheet: SampleSheet = "AAGAGGCA,TCGACTAG,E-coli_1ng_input-rep01
GCTCATGA,CGTCTAAT,E-coli_1ng_input-rep02
AGGCAGAA,GCGTAAGA,E-coli_1ng_input-rep03
TAAGGCGA,ACTGCATA,E-coli_100ng_input-rep04
CGTACTAG,GTAAGGAG,E-coli_100ng_input-rep05
TAGCGCTC,TATCCTCT,E-coli_100ng_input-rep06
CGGAGCCT,CCTAGAGT,B-cereus_1ng_input-rep07
GCTCATGA,TCGACTAG,B-cereus_1ng_input-rep08
CGAGGCTG,CCTAGAGT,B-cereus_1ng_input-rep09
TAAGGCGA,TTATGCGA,B-cereus_100ng_input-rep10
CGTACTAG,TCGACTAG,B-cereus_100ng_input-rep11
AGGCAGAA,CGTCTAAT,B-cereus_100ng_input-rep12
CTCTCTAC,ACTGCATA,R-sphaeroides_1ng_input-rep13
TAGCGCTC,GTAAGGAG,R-sphaeroides_1ng_input-rep14
GGACTCCT,GCGTAAGA,R-sphaeroides_100ng_input-rep15
GTAGAGGA,TATCCTCT,R-sphaeroides_100ng_input-rep16
AAGAGGCA,CCTAGAGT,R-sphaeroides_100ng_input-rep17
CGTACTAG,CGTCTAAT,R-sphaeroides_100ng_input-rep18
"
    .parse()
    .expect("could not parse sample sheet");

    let run_info = Arc::new(run_info);
    let sample_sheet = Arc::new(sample_sheet);

    let mut output_files = vec![];
    for name in &sample_sheet.names {
        let mut output_files_for_sample = vec![];
        for i in 0..run_info.run.reads.read.len() {
            let output_file = format!("out/{}_L001_R{}_001.fastq.gz", name, i);
            let file = File::create(&output_file)
                .await
                .expect("could not create file");

            let gz_encoder = GzipEncoder::with_quality(file, async_compression::Level::Fastest);
            
            let buf_writer = BufWriter::new(gz_encoder);
            output_files_for_sample.push(buf_writer);
        }
        output_files.push(output_files_for_sample);
    }

    let (sender, receiver) = mpsc::channel(10000);

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = vec![];

    let sem = Arc::new(Semaphore::new(40));

    let writer_task = tokio::spawn(writer_task(receiver, output_files));

    for lane in 1..=lanes {
        for &tile in &tile_numbers {
            let permit = Arc::clone(&sem).acquire_owned().await;

            let run_info_clone = Arc::clone(&run_info);
            let sample_sheet_clone = Arc::clone(&sample_sheet);
            let path_clone = path.clone();
            let sender_clone = sender.clone();

            let task = tokio::spawn(async move {
                let _permit = permit;

                process_bcl_files(
                    &path_clone,
                    run_info_clone,
                    sample_sheet_clone,
                    lane,
                    tile,
                    sender_clone,
                )
                .await;
            });

            tasks.push(task);
        }
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await.unwrap();
    }

    // Drop the sender to close the channel
    drop(sender);

    println!("All tasks completed, waiting for writer task to finish...");

    // Wait for the writer task to finish
    writer_task.await.expect("Failed to wait for writer task");
    let elapsed = std::time::Instant::now().elapsed();
    println!("Total time taken to write fastq files: {:?}", elapsed);
}
