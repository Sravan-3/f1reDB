use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub struct Manifest {
    pub next_sstable_id: u64,
    pub sstables:Vec<PathBuf>
}

impl Manifest {

    pub fn load(path: &str) -> std::io::Result<Self>{

        if !std::path::Path::new(path).exists() {
            return Ok(Self{
                next_sstable_id: 0,
                sstables: Vec::new(),
            });
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut next_id = 0;
        let mut sstables = Vec::new();

        for line in reader.lines() {

            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();

            match parts.as_slice() {

                ["NEXT_SSTABLE_ID", id] => {
                    next_id = id.parse().unwrap();
                }

                ["SSTABLE", path] => {
                    sstables.push(PathBuf::from(path));
                }

                _ => {}
            }
            
        }

        Ok(Self { 
            next_sstable_id: next_id, 
            sstables,
        })

    }

    pub fn save(&self, path: &str) -> std::io::Result<()>{

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        writeln!(file, "NEXT_SSTABLE_ID {}", self.next_sstable_id)?;

        for sstable in &self.sstables{
            writeln!(file, "SSTABLE {}", sstable.display())?;
        }

        file.flush()?;

        Ok(())
    }

    pub fn allocate_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }

    pub fn add_sstable(&mut self, path: PathBuf) {
        self.sstables.push(path);
    }
}