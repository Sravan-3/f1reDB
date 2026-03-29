use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub struct Manifest {
    pub next_sstable_id: u64,
    pub level0: Vec<PathBuf>,
    pub level1: Vec<PathBuf>,
    pub level2: Vec<PathBuf>,
}

impl Manifest {

    pub fn load(path: &str) -> std::io::Result<Self> {

        if !std::path::Path::new(path).exists() {
            return Ok(Self {
                next_sstable_id: 0,
                level0: Vec::new(),
                level1: Vec::new(),
                level2: Vec::new(),
            });
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut next_id = 0;
        let mut level0 = Vec::new();
        let mut level1 = Vec::new();
        let mut level2 = Vec::new();

        for line in reader.lines() {

            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();

            match parts.as_slice() {

                ["NEXT_SSTABLE_ID", id] => {
                    next_id = id.parse().unwrap();
                }

                ["LEVEL", "0", path] => {
                    level0.push(PathBuf::from(path));
                }

                ["LEVEL", "1", path] => {
                    level1.push(PathBuf::from(path));
                }

                ["LEVEL", "2", path] => {
                    level2.push(PathBuf::from(path));
                }

                _ => {}
            }
        }

        Ok(Self {
            next_sstable_id: next_id,
            level0,
            level1,
            level2,
        })
    }

    pub fn persist(&self, path: &str) -> std::io::Result<()> {

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        writeln!(file, "NEXT_SSTABLE_ID {}", self.next_sstable_id)?;

        for p in &self.level0 {
            writeln!(file, "LEVEL 0 {}", p.display())?;
        }

        for p in &self.level1 {
            writeln!(file, "LEVEL 1 {}", p.display())?;
        }

        for p in &self.level2 {
            writeln!(file, "LEVEL 2 {}", p.display())?;
        }

        file.flush()?;

        Ok(())
    }

    pub fn allocate_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }

    pub fn add_sstable(&mut self, level: usize, path: PathBuf) {
        match level {
            0 => self.level0.push(path),
            1 => self.level1.push(path),
            2 => self.level2.push(path),
            _ => {}
        }
    }

    pub fn remove_sstable(&mut self, level: usize, path: &PathBuf) {
        match level {
            0 => self.level0.retain(|p| p != path),
            1 => self.level1.retain(|p| p != path),
            2 => self.level2.retain(|p| p != path),
            _ => {}
        }
    }
}