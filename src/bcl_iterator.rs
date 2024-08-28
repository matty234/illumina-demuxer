use std::io::Read;
use std::ops::Range;
use std::{fs::File, io::BufReader};

use crate::run_info::Reads;




pub struct BclIterator {
    cycles: u32,
    path: String,

    open_files: Vec<BufReader<File>>,
    pub cluster_count: Vec<u32>,

    read_position_decode_memory_map: Vec<u8>,
}

impl BclIterator {
    pub fn new(cycles: u32, path: String, reads: &Reads, lane: u32, tile_number: u32) -> Self {
        let mut open_files = Vec::new();
        let basecalls_path: String = format!("{}/Data/Intensities/BaseCalls", path);
        let mut cluster_count = Vec::new();
        for cycle in 1..=cycles {
            let path = format!("{}/L001/C{}.1", basecalls_path, cycle);

            // todo find out what the convention is here:
            let s_prefix = format!("s_{}_{}", lane, tile_number);

            let bcl_path = format!("{}/{}.bcl", path, s_prefix);

            let file = std::fs::File::open(bcl_path).expect("could not open file");
            let mut reader = BufReader::new(file);

            
            
            let mut n_clusters_bytes = [0u8; 4];
            let n_clusters = match &mut reader.read_exact(&mut n_clusters_bytes) {
                Ok(()) => u32::from_le_bytes(n_clusters_bytes),
                Err(_) => panic!("could not read n_clusters"),
            };


            cluster_count.push(n_clusters);
            open_files.push(reader);
        }
        Self {
            cycles,
            path,
            open_files,
            cluster_count: cluster_count,
            read_position_decode_memory_map: reads.create_memory_map_decode(),
        }
    }

    pub fn close(&mut self) {
        for file in self.open_files.drain(..) {
            drop(file);
        }
    }
}

impl Iterator for BclIterator {
    // every item is the collection of cycles of clusters of bases and quals
    // the third item is the number of the read and the fourth is whether it is an index read
    type Item = Vec<(char, u8, u8, bool)>;


    
    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
    
        for (file_i, file) in &mut self.open_files.iter_mut().enumerate() {
         
            let mem_map_position = self.read_position_decode_memory_map[file_i];

            let read_number = mem_map_position >> 4;
            let is_indexed_read = (mem_map_position & 1) == 1;

            let mut byte = [0];
                match file.read_exact(&mut byte) {
                    Ok(()) => {

                        

                        if byte[0] == 0 {
                            buffer.push(('N', 0, read_number, is_indexed_read));
                        } else {
                            let base = match byte[0] & 0b11 {
                                0b00 => 'A',
                                0b01 => 'C',
                                0b10 => 'G',
                                0b11 => 'T',
                                _ => unreachable!(),
                            };
                            let qual = byte[0] >> 2;
                            buffer.push((base, qual, read_number, is_indexed_read));
                        }
                    }
                    Err(_) => return None,
                }
            
        }
        Some(buffer)
    }
}
