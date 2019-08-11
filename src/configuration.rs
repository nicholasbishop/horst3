use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::{fs, io};

#[derive(Debug)]
pub enum ConfigurationError {
    HomeDirNotFound,
    DefaultConfigError(io::Error),
    ParseFailed,
    ReadFailed(io::Error),
}

pub struct Configuration {
    pub cache_size_limit_in_bytes: u64,
    pub cache_path: PathBuf,
}

/// Parse the contents of a configuration file
///
/// Lines where the first non-whitespace character is a '#' are
/// ignored. Lines containing an '=' are parsed as <key> = <value>
/// pairs and returned in a HashMap.
fn parse_config(s: &str) -> HashMap<&str, &str> {
    let mut map = HashMap::new();
    for line in s.lines() {
        let line = line.trim();
        if !line.starts_with('#') {
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() == 2 {
                let key = parts[0].trim();
                let val = parts[1].trim();
                map.insert(key, val);
            }
        }
    }
    map
}

const CACHE_PATH: &str = "cache_path";
const CACHE_PATH_DEFAULT: &str = "~/.cache/horst3";
const CACHE_SIZE_LIMIT: &str = "cache_size_limit";
const CACHE_SIZE_LIMIT_DEFAULT: &str = "16GiB";
const CACHE_SIZE_LIMIT_DEFAULT_IN_BYTES: u64 = 16 * 1024 * 1024 * 1024;

fn write_default_config(path: &Path) -> Result<(), ConfigurationError> {
    let contents = format!(
        "{} = {}\n{} = {}\n",
        CACHE_PATH,
        CACHE_PATH_DEFAULT,
        CACHE_SIZE_LIMIT,
        CACHE_SIZE_LIMIT_DEFAULT
    );
    fs::write(path, contents)
        .map_err(ConfigurationError::DefaultConfigError)?;
    Ok(())
}

fn parse_size_as_bytes(s: &str) -> Option<u64> {
    let mut units = HashMap::new();
    units.insert("TiB", 1024u64 * 1024 * 1024 * 1024);
    units.insert("TB", 1000 * 1000 * 1000 * 1000);
    units.insert("GiB", 1024 * 1024 * 1024);
    units.insert("GB", 1000 * 1000 * 1000);
    units.insert("MiB", 1024 * 1024);
    units.insert("MB", 1000 * 1000);
    units.insert("KiB", 1024);
    units.insert("KB", 1000);
    units.insert("B", 1);
    let num_str;
    let unit;
    if let Some(unit_start) = s.find(|c: char| !c.is_ascii_digit() && c != '.')
    {
        num_str = s[..unit_start].trim();
        unit = s[unit_start..].trim();
    } else {
        num_str = s;
        unit = "B";
    }
    if let Ok(num) = num_str.parse::<f64>() {
        if let Some(multiplier) = units.get(unit) {
            Some((num * (*multiplier as f64)) as u64)
        } else {
            None
        }
    } else {
        None
    }
}

impl Configuration {
    pub fn open() -> Result<Configuration, ConfigurationError> {
        let home =
            dirs::home_dir().ok_or(ConfigurationError::HomeDirNotFound)?;
        let conf_path = home.join(".config/horst3.conf");
        if !conf_path.exists() {
            write_default_config(&conf_path)?;
        }
        let contents = fs::read_to_string(conf_path)
            .map_err(ConfigurationError::ReadFailed)?;
        let map = parse_config(&contents);
        let cache_path = map.get(CACHE_PATH).unwrap_or(&CACHE_PATH_DEFAULT);
        let cache_size_limit = map
            .get(CACHE_SIZE_LIMIT)
            .unwrap_or(&CACHE_SIZE_LIMIT_DEFAULT);
        let cache_size_limit_in_bytes = parse_size_as_bytes(cache_size_limit)
            .unwrap_or(CACHE_SIZE_LIMIT_DEFAULT_IN_BYTES);
        Ok(Configuration {
            cache_size_limit_in_bytes,
            cache_path: Path::new(cache_path).to_path_buf(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let mut map = HashMap::new();
        assert_eq!(parse_config(""), map);
        map.insert("a", "b");
        assert_eq!(parse_config("a=b"), map);
        assert_eq!(parse_config("a = b"), map);
        map.insert("c", "d");
        assert_eq!(parse_config("a = b\nc = d"), map);
        assert_eq!(parse_config("a = b\nc = d\n# comment"), map);
    }

    #[test]
    fn test_parse_size_as_bytes() {
        assert_eq!(parse_size_as_bytes("16GiB"), Some(16 * 1024 * 1024 * 1024));
        assert_eq!(
            parse_size_as_bytes("16 GiB"),
            Some(16 * 1024 * 1024 * 1024)
        );
    }
}
