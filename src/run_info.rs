

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RunInfo {
    #[serde(rename = "Run")]
    pub run: Run,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Run {
    pub id: String,
    pub number: u32,
    pub flowcell: String,
    pub instrument: String,
    pub date: String,
    pub reads: Reads,
    pub flowcell_layout: FlowcellLayout,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Reads {
    #[serde(rename = "Read")]
    pub read: Vec<Read>,
}

impl Reads {
    #[inline(always)]
    pub fn calculate_total_cycles(&self) -> u32 {
        self.read.iter().map(|r| r.num_cycles).sum()
    }

    
    pub fn create_memory_map_decode(&self) -> Vec<u8> {
        let mut memory_map = Vec::new();
        for (ri, r) in self.read.iter().enumerate() {
            for i in 0..r.num_cycles {
                memory_map.push(r.create_bit_mask_representation(ri as u8));
            }
        }

        memory_map
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Read {
    num_cycles: u32,
    number: u32,

    // N or Y
    is_indexed_read: String,
}

impl Read {

    #[inline(always)]
    pub fn create_bit_mask_representation(&self, index: u8) -> u8 {
        // 1 byte bit mask representation
        // MSB is currently unused

        if index > 0b111 {
            panic!("index must be less than 8");
        }

        if self.number > 0b111 {
            panic!("number must be less than 8");
        }



        let mut mask: u8 = 0;
        mask |= index << 4;
        mask |= (self.number as u8) << 1;
        if self.is_indexed_read == "Y" {
            mask |= 1;
        }

        mask
    }

    pub fn decode_bit_mask_representation(mask: u8) -> (u8, u8, String) {
        let index = mask >> 4;
        let number = (mask >> 1) & 0b111;
        let is_indexed_read = if mask & 1 == 1 {
            "Y"
        } else {
            "N"
        };
        (index, number, is_indexed_read.to_string())
    }

}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FlowcellLayout {
    pub lane_count: u32,
    pub surface_count: u32,
    pub swath_count: u32,
    pub tile_count: u32,
}

