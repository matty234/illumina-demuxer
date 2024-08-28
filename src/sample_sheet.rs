use std::str::FromStr;

// Sample sheet row is a vec or r1 indexes, r2 indexes and a sample ID
type SampleSheetRow = Vec<(String, String, String)>;

pub struct SampleSheet {
    r1_indexes: Vec<u16>,
    r2_indexes: Vec<u16>,
    pub names: Vec<String>,
}

impl SampleSheet {
    #[inline(always)]
    fn ascii_to_index(c: char) -> u8 {
        match c {
            'A' => 0,
            'C' => 1,
            'G' => 2,
            'T' => 3,
            _ => panic!("Invalid base"),
        }
    }
    
    #[inline]
    fn index_string_to_vec(s: &str) -> usize {
        s.chars()
            .fold(0, |index, c| (index << 2) | Self::ascii_to_index(c) as usize)
    }



    // atm ignore read 2
    #[inline]
    pub fn get_r1_sample_idx(&self, r1_read: u16) -> u16 {
        self.r1_indexes[r1_read as usize]
    }

    #[inline]
    pub fn get_r2_sample_idx(&self, r2_read: u16) -> u16 {
        self.r2_indexes[r2_read as usize]
    }


}

impl FromStr for SampleSheet {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .from_reader(s.as_bytes());

        let mut r1_indexes = vec![0u16; 65536];
        let mut r2_indexes = vec![0u16; 65536];
        let mut names = Vec::new();
        names.push("Unknown".to_string());

        for result in rdr.records() {
            let record = result.expect("a CSV record");

            // r1, r2, sample_name
            let r1_index_str = record.get(0).expect("r1 index missing");
            let r2_index_str = record.get(1).expect("r2 index missing");
            let sample_name = record.get(2).expect("sample name missing");

            // push the name and get the index
            names.push(sample_name.to_string());
            let index = names.len() - 1;

            let r1_index = Self::index_string_to_vec(r1_index_str);
            let r2_index = Self::index_string_to_vec(r2_index_str);

            r1_indexes[r1_index] = index as u16;
            r2_indexes[r2_index] = index as u16;

            // Create all variations with 1 or 2 mismatches
            for i in 0..8 {
                let mask1 = 3 << (i * 2);
                for j in 0..4 {
                    let r1_variant1 = (r1_index & !mask1) | (j << (i * 2));
                    let r2_variant1 = (r2_index & !mask1) | (j << (i * 2));

                    r1_indexes[r1_variant1] = index as u16;
                    r2_indexes[r2_variant1] = index as u16;

                    // Introduce second mismatch
                    /*for k in (i + 1)..8 {
                        let mask2 = 3 << (k * 2);
                        for l in 0..4 {
                            let r1_variant2 = (r1_variant1 & !mask2) | (l << (k * 2));
                            let r2_variant2 = (r2_variant1 & !mask2) | (l << (k * 2));

                            r1_indexes[r1_variant2] = index as u16;
                            r2_indexes[r2_variant2] = index as u16;
                        }
                    }*/
                }
            }

            println!("{} {} {}", r1_index, r2_index, sample_name);
        }

        Ok(Self {
            r1_indexes,
            r2_indexes,
            names,
        })
    }
}

// 8 base index  = 2 bytes
// 8 ^ 8 = 2^16 = 65536 indexes
// 65536 * 2 = 131072 bytes
// 131072 / 1024 = 128 KB

// so we need a u16 offset to go to u16 index maps
// u16 max = 65535
