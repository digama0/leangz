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

  /// Stage the fully written file for commit, closing the file handle and
  /// keeping only the temp path for the final rename.
  pub fn stage(self) -> StagedFile {
    StagedFile { temp: self.file.into_temp_path(), path: self.path }
  }
}

/// A file fully written under a temporary name, staged for commit. `save`
/// renames it into place; dropping it unsaved deletes it.
pub struct StagedFile {
  temp: tempfile::TempPath,
  path: PathBuf,
}

impl StagedFile {
  pub fn save(self) -> io::Result<PathBuf> {
    self.temp.persist(&self.path).map_err(|e| e.error)?;
    Ok(self.path)
  }
}
