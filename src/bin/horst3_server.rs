use actix_files::NamedFile;
use actix_web::error::ErrorBadRequest;
use actix_web::{web, App, HttpServer};
use horst3::cache::Cache;
use serde::Deserialize;

// This server caches files from S3. The idea is that this server is
// run on a machine in your LAN, which hopefully allows faster file
// transfer than directly downloading from S3.

#[derive(Debug, Deserialize)]
struct LookupInput {
    bucket: String,
    key: String,
    md5sum: String,
}

/// Check if `s` is a valid md5sum (32 hex digits)
fn is_valid_md5sum(s: &str) -> bool {
    if s.len() != 32 {
        return false;
    }

    for c in s.chars() {
        if !c.is_ascii_hexdigit() {
            return false;
        }
    }

    return true;
}

fn download(inputs: web::Json<LookupInput>) -> actix_web::Result<NamedFile> {
    if !is_valid_md5sum(&inputs.md5sum) {
        return Err(ErrorBadRequest("invalid md5sum"))
    }

    let path;
    {
        let cache = Cache::open().map_err(ErrorBadRequest)?;
        cache.touch(&inputs.md5sum).map_err(ErrorBadRequest)?;
        path = cache.path(&inputs.md5sum);
    }

    Ok(NamedFile::open(path)?)
}

fn main() {
    HttpServer::new(|| App::new().route("/", web::get().to(download)))
        .bind("0.0.0.0:47205")
        .unwrap()
        .run()
        .unwrap();
}
