use crate::configuration::{Configuration, ConfigurationError};
use lockfile::Lockfile;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, SystemTimeError};
use std::{fs, io};

#[derive(Debug)]
pub enum CacheError {
    ConfigurationError(ConfigurationError),
    CopyError(io::Error),
    LockError(io::Error),
    MakeSpaceError(io::Error),
    TimestampError(SystemTimeError),
    TouchError(io::Error),
}

pub struct Cache {
    conf: Configuration,
    lock: Lockfile,
}

fn get_current_timestamp_in_s() -> Result<u64, CacheError> {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(CacheError::TimestampError)?;
    Ok(d.as_secs())
}

impl Cache {
    pub fn open() -> Result<Cache, CacheError> {
        let conf =
            Configuration::open().map_err(CacheError::ConfigurationError)?;
        let lock = Lockfile::create(conf.cache_path.join("lock"))
            .map_err(CacheError::LockError)?;
        Ok(Cache { conf, lock })
    }

    pub fn path(&self, md5sum: &str) -> PathBuf {
        self.lock.path().join(md5sum)
    }

    pub fn temporary_path(&self, md5sum: &str) -> PathBuf {
        let name = format!("{}.tmp", md5sum);
        self.lock.path().join(name)
    }

    pub fn contains(&self, md5sum: &str) -> bool {
        self.path(md5sum).exists()
    }

    fn touch(&self, md5sum: &str) -> Result<(), CacheError> {
        let path = self.path(md5sum);
        let (_, mtime) =
            utime::get_file_times(&path).map_err(CacheError::TouchError)?;
        let now = get_current_timestamp_in_s()?;
        utime::set_file_times(&path, now, mtime)
            .map_err(CacheError::TouchError)?;
        Ok(())
    }

    pub fn copy(
        &self,
        md5sum: &str,
        dst_path: &Path,
    ) -> Result<(), CacheError> {
        let src_path = self.path(md5sum);
        self.touch(md5sum)?;
        fs::copy(src_path, dst_path).map_err(CacheError::CopyError)?;
        Ok(())
    }

    pub fn make_space(&self, num_bytes: u64) -> Result<bool, CacheError> {
        // Check if object is bigger than the cache limit
        if num_bytes > self.conf.cache_size_limit_in_bytes {
            return Ok(false);
        }

        let mut map = BTreeMap::new();
        for entry in fs::read_dir(self.lock.path())
            .map_err(CacheError::MakeSpaceError)?
        {
            let entry = entry.map_err(CacheError::MakeSpaceError)?;
            let path = entry.path();
            let (atime, _) = utime::get_file_times(&path)
                .map_err(CacheError::MakeSpaceError)?;
            map.insert(atime, path);
        }

        let mut num_bytes_freed = 0;
        for (_, path) in map.iter() {
            let metadata =
                fs::metadata(path).map_err(CacheError::MakeSpaceError)?;
            let size = metadata.len();
            fs::remove_file(path).map_err(CacheError::MakeSpaceError)?;
            num_bytes_freed += size;
            if num_bytes_freed >= num_bytes {
                return Ok(true);
            }
        }

        return Ok(false);
    }
}
