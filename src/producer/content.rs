use std::{fs::File, io::BufRead, io::BufReader, path::PathBuf};

pub struct CachedMessages(PathBuf);

impl CachedMessages {
    pub fn from_file(file: PathBuf) -> Self {
        Self(file)
    }
}

impl<'a> IntoIterator for &'a CachedMessages {
    type Item = Vec<u8>;
    type IntoIter = Box<dyn Iterator<Item = Vec<u8>>>;

    fn into_iter(self) -> Self::IntoIter {
        let file = File::open(&self.0).unwrap();
        let reader = BufReader::new(file);
        Box::new(
            reader
                .lines()
                .map(|line| base64::decode(line.unwrap()).unwrap()),
        )
    }
}
