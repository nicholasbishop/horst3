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
    ScanError(io::Error),
    TimestampError(SystemTimeError),
    TouchError(io::Error),
}

pub struct Cache {
    conf: Configuration,
    #[allow(dead_code)]
    lock: Lockfile,
}

fn get_current_timestamp_in_s() -> Result<u64, CacheError> {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(CacheError::TimestampError)?;
    Ok(d.as_secs())
}

/// Set a file's atime without changing its mtime
fn set_file_atime(path: &Path, atime: u64) -> Result<(), CacheError> {
    let (_, mtime) =
        utime::get_file_times(path).map_err(CacheError::TouchError)?;
    utime::set_file_times(path, atime, mtime)
        .map_err(CacheError::TouchError)?;
    Ok(())
}

impl Cache {
    pub fn open() -> Result<Cache, CacheError> {
        let conf =
            Configuration::open().map_err(CacheError::ConfigurationError)?;
        Cache::open_with_configuration(conf)
    }

    fn open_with_configuration(conf: Configuration) -> Result<Cache, CacheError> {
        let lock = Lockfile::create(conf.cache_path.join("lock"))
            .map_err(CacheError::LockError)?;
        Ok(Cache { conf, lock })
    }

    fn root(&self) -> &Path {
        &self.conf.cache_path
    }

    pub fn path(&self, md5sum: &str) -> PathBuf {
        self.root().join(md5sum)
    }

    pub fn temporary_path(&self, md5sum: &str) -> PathBuf {
        let name = format!("{}.tmp", md5sum);
        self.root().join(name)
    }

    pub fn contains(&self, md5sum: &str) -> bool {
        self.path(md5sum).exists()
    }

    fn touch(&self, md5sum: &str) -> Result<(), CacheError> {
        let path = self.path(md5sum);
        let now = get_current_timestamp_in_s()?;
        set_file_atime(&path, now)
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

    fn get_least_recently_used(&self) -> Result<BTreeMap<u64, PathBuf>, CacheError> {
        let mut map = BTreeMap::new();
        for entry in fs::read_dir(self.root())
            .map_err(CacheError::ScanError)?
        {
            let entry = entry.map_err(CacheError::ScanError)?;
            if entry.file_name() == "lock" {
                continue;
            }
            let path = entry.path();
            let (atime, _) = utime::get_file_times(&path)
                .map_err(CacheError::ScanError)?;
            map.insert(atime, path);
        }
        Ok(map)
    }

    pub fn make_space(&self, num_bytes: u64) -> Result<bool, CacheError> {
        // Check if object is bigger than the cache limit
        if num_bytes > self.conf.cache_size_limit_in_bytes {
            return Ok(false);
        }

        let map = self.get_least_recently_used()?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache() {
        let dir = tempfile::tempdir().unwrap();
        let conf = Configuration {
            cache_size_limit_in_bytes: 2,
            cache_path: dir.path().to_path_buf(),
        };
        let cache = Cache::open_with_configuration(conf).unwrap();
        let mut map = BTreeMap::new();
        assert_eq!(cache.get_least_recently_used().unwrap(), map);

        let file1 = dir.path().join("test1");
        fs::write(&file1, "a").unwrap();
        set_file_atime(&file1, 1).unwrap();
        map.insert(1, file1);
        assert_eq!(cache.get_least_recently_used().unwrap(), map);

        let file2 = dir.path().join("test2");
        fs::write(&file2, "a").unwrap();
        set_file_atime(&file2, 2).unwrap();
        map.insert(2, file2);
        assert_eq!(cache.get_least_recently_used().unwrap(), map);

        // Can't make space for a file that's bigger than the cache
        assert_eq!(cache.make_space(3).unwrap(), false);

        // This should delete file1
        assert_eq!(cache.make_space(1).unwrap(), true);
        map.remove(&1);
        assert_eq!(cache.get_least_recently_used().unwrap(), map);
    }
}
