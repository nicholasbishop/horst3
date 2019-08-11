use crate::cache::{Cache, CacheError};
use log::error;
use serde::Deserialize;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::{fs, io};

#[derive(Debug, Deserialize)]
struct HeadObjectMetadata {
    md5sum: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HeadObject {
    last_modified: String,
    content_length: u64,
    metadata: HeadObjectMetadata,
}

#[derive(Debug)]
pub struct S3Url {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug)]
pub enum S3Error {
    CacheError(CacheError),
    CommandFailed(ExitStatus),
    IoError(io::Error),
    JsonError(serde_json::Error),
    MoveError(io::Error),
    NonUtf8Path,
}

impl S3Url {
    /// Create an S3Url
    pub fn new(bucket: String, key: String) -> S3Url {
        S3Url { bucket, key }
    }

    /// Format as s3://<bucket>/<key>
    pub fn to_string(&self) -> String {
        format!("s3://{}/{}", &self.bucket, &self.key)
    }

    /// Request the object's metadata
    fn head_object(&self) -> Result<HeadObject, S3Error> {
        let output = Command::new("aws")
            .args(&[
                "s3api",
                "head-object",
                "--bucket",
                &self.bucket,
                "--key",
                &self.key,
            ])
            .output()
            .map_err(S3Error::IoError)?;
        if !output.status.success() {
            return Err(S3Error::CommandFailed(output.status));
        }
        serde_json::from_slice(&output.stdout).map_err(S3Error::JsonError)
    }

    /// Download the object directly (bypassing the cache)
    pub fn download_direct(&self, path: &Path) -> Result<(), S3Error> {
        let path_str = path.to_str().ok_or(S3Error::NonUtf8Path)?;
        let status = Command::new("aws")
            .args(&["s3", "cp", &self.to_string(), path_str])
            .status()
            .map_err(S3Error::IoError)?;
        if !status.success() {
            return Err(S3Error::CommandFailed(status));
        }
        Ok(())
    }

    pub fn download(&self, path: &Path) -> Result<(), S3Error> {
        let head = self.head_object()?;

        // If the object doesn't have an md5sum then we can't look it
        // up in the cache
        let md5sum;
        if let Some(m) = head.metadata.md5sum.as_ref() {
            md5sum = m;
        } else {
            return self.download_direct(path);
        }

        let cache = Cache::open().map_err(S3Error::CacheError)?;
        if cache.contains(md5sum) {
            cache.copy(md5sum, path).map_err(S3Error::CacheError)
        } else {
            match cache.make_space(head.content_length) {
                Ok(true) => {
                    // Download the object into the cache
                    let tmp_path = cache.temporary_path(md5sum);
                    if let Err(err) = self.download_direct(&tmp_path) {
                        if let Err(err) = fs::remove_file(&tmp_path) {
                            error!(
                                "failed to delete {}: {}",
                                tmp_path.display(),
                                err
                            );
                        }
                        Err(err)
                    } else {
                        let final_path = cache.path(md5sum);
                        fs::rename(tmp_path, final_path)
                            .map_err(S3Error::MoveError)
                    }
                }
                Ok(false) => self.download_direct(path),
                Err(err) => Err(S3Error::CacheError(err)),
            }
        }
    }
}
