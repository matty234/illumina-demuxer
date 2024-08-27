use run_info::RunInfo;
use sample_sheet::SampleSheet;
use serde_xml_rs::from_str;

use rayon::prelude::*;
use std::sync::Arc;
use std::{fs, fs::File, io::BufWriter, io::Write};

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

fn process_bcl_files(
    path: &str,
    run_info: &RunInfo,
    sample_sheet: &SampleSheet,
    lane: u32,
    tile: u32,
) -> Vec<Vec<Vec<String>>> {
    println!("Processing lane {} tile {}", lane, tile);
    let total_cycles: u32 = run_info.run.reads.calculate_total_cycles();

    let bcl_file_iterator: bcl_iterator::BclIterator = bcl_iterator::BclIterator::new(
        total_cycles,
        path.to_string(),
        &run_info.run.reads,
        lane,
        tile,
    );

    let mut read_cache: Vec<String> = vec![String::new(); run_info.run.reads.read.len()];
    let mut qual_cache: Vec<String> = vec![String::new(); run_info.run.reads.read.len()];
    let mut output_buffers: Vec<Vec<Vec<String>>> = vec![Vec::new(); sample_sheet.names.len()];

    for i in 0..sample_sheet.names.len() {
        output_buffers[i] = vec![Vec::new(); run_info.run.reads.read.len()];
    }

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


        let output_buffer = &mut output_buffers[sample as usize];

        for actual_read in read_cache.iter().enumerate() {
            let record = format!(
                "@{}\n{}\n+\n{}\n",
                sample, actual_read.1, qual_cache[actual_read.0]
            );
            output_buffer[actual_read.0].push(record);
        }

        for i in 0..run_info.run.reads.read.len() {
            read_cache[i].clear();
            qual_cache[i].clear();
        }
    }

    println!("Finished processing lane {} tile {}", lane, tile);

    output_buffers
}

fn main() {
    // Set up Rayon to use at most 4 threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build_global()
        .unwrap();

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
GGACTCCT,GCGTAAGA,R-sphaeroides_1ng_input-rep15
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
            let output_file = format!("out/{}_L001_R{}_001.fastq", name, i);
            let file = File::create(&output_file).expect("could not create output file");
            let buf_writer = BufWriter::new(file);
            output_files_for_sample.push(buf_writer);
        }
        output_files.push(output_files_for_sample);
    }

    let arc_mutex_output_files = Arc::new(std::sync::Mutex::new(output_files));

    (1..=lanes).into_par_iter().for_each(|lane| {
        tile_numbers.par_iter().for_each(|&tile| {
            let outputs = process_bcl_files(&path, &run_info, &sample_sheet, lane, tile);

            for (sample_idx, buffers) in outputs.into_iter().enumerate() {
                for (read_idx, buffer) in buffers.into_iter().enumerate() {

                    let mut output_files = arc_mutex_output_files.lock().unwrap();
                    let mut output_file = &mut output_files[sample_idx][read_idx];
                    

                    for record in buffer {
                        output_file.write_all(record.as_bytes()).expect("could not write record");
                    }
                }
            }
        });
    });
    // After all processing, write buffers to files

    let elapsed = std::time::Instant::now().elapsed();
    println!("Total time taken to write fastq files: {:?}", elapsed);
}
