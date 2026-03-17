use std::{fs::File, io, path::PathBuf};
use tempfile::NamedTempFile;

pub mod lgz;
pub mod ltar;

pub struct TempFile {
  file: NamedTempFile,
  path: PathBuf,
}

impl std::ops::Deref for TempFile {
  type Target = File;
  fn deref(&self) -> &Self::Target { self.file.as_file() }
}
impl std::ops::DerefMut for TempFile {
  fn deref_mut(&mut self) -> &mut Self::Target { self.file.as_file_mut() }
}

impl TempFile {
  pub fn new(path: PathBuf) -> io::Result<Self> {
    Ok(TempFile { file: NamedTempFile::new_in(path.parent().unwrap_or(&path))?, path })
  }

  pub fn save(self) -> io::Result<PathBuf> {
    self.file.persist(&self.path).map_err(|e| e.error)?;
    Ok(self.path)
  }
}
