use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub databases: Vec<DatabaseConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub backend: Backend,
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Fs,
    S3,
    Memory,
    Url,
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let default = Self {
            databases: vec![DatabaseConfig {
                name: "default".to_string(),
                backend: Backend::Fs,
                path: "./.silo/default".to_string(),
            }],
        };

        match path {
            Some(p) => {
                let data = fs::read_to_string(p)?;
                let cfg: Self = toml::from_str(&data)?;
                Ok(cfg)
            }
            None => Ok(default),
        }
    }
}


