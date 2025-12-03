use std::{
    fs, io,
    path::{Path, PathBuf},
};

use minio::s3::{
    self,
    creds::StaticProvider,
    http::BaseUrl,
    segmented_bytes::SegmentedBytes,
    types::{S3Api, ToStream},
};
use serde::Deserialize;
use tokio_util::bytes::Bytes;
use tracing::{debug, error, info, warn};

use anyhow::{Result, anyhow};
use tracing_subscriber::field::debug;
use url::Url;

#[derive(Deserialize, Debug)]
#[serde(tag = "htype")]
enum DetectorHeader {
    #[serde(rename = "dheader-1.0")]
    Header {
        header_detail: String,
        series: usize,
    },
    #[serde(rename = "dimage-1.0")]
    Image {
        frame: usize,
        hash: String,
        series: usize,
    },
    #[serde(rename = "dseries_end-1.0")]
    SeriesEnd { series: usize },
}

impl DetectorHeader {
    fn series(&self) -> usize {
        match self {
            DetectorHeader::Header { series, .. } => *series,
            DetectorHeader::Image { series, .. } => *series,
            DetectorHeader::SeriesEnd { series } => *series,
        }
    }
}

pub trait AcquisitionWriter {
    fn handle_start(&mut self, series: usize, messages: Vec<Vec<u8>>) -> Result<()>;
    fn handle_end(&mut self, series: usize) -> io::Result<()>;
    fn handle_image(&self, series: usize, image: usize, messages: Vec<Vec<u8>>) -> io::Result<()>;
}

/// Writes a copy of a stream of data to a folder.
///
/// Matches behaviour of ODIN /dev/shm writer
pub struct FolderWriter {
    /// The base folder location to write to
    base: PathBuf,
    /// The current series-destination path
    current_path: Option<PathBuf>,
}

impl FolderWriter {
    pub fn new(base_path: &Path) -> Self {
        Self {
            base: base_path.to_path_buf(),
            current_path: None,
        }
    }
}
impl AcquisitionWriter for FolderWriter {
    fn handle_start(&mut self, series: usize, messages: Vec<Vec<u8>>) -> Result<()> {
        let mut attempts = 0usize;
        let mut series_path = self.base.join(format!("{series}"));
        // This should rarely happen, but handle cases where this path already exists
        while series_path.exists() {
            attempts += 1;
            series_path = self.base.join(format!("{series}_{attempts}"))
        }
        std::fs::create_dir_all(&series_path)?;
        info!(
            "Writing new acquisition {series} to {}",
            series_path.display()
        );

        for (i, message) in messages.iter().enumerate() {
            let filename = series_path.join(format!("start_{i}"));
            fs::write(&filename, &message)?;
        }
        self.current_path = Some(series_path);
        Ok(())
    }
    fn handle_end(&mut self, series: usize) -> io::Result<()> {
        info!("Ending acquisition {series}");

        fs::write(
            self.current_path.as_ref().unwrap().join("end"),
            format!(r#"{{"htype":"dseries_end-1.0","series":{series}}}"#).as_bytes(),
        )?;
        self.current_path = None;
        Ok(())
    }
    fn handle_image(&self, _series: usize, image: usize, messages: Vec<Vec<u8>>) -> io::Result<()> {
        for (i, message) in messages.iter().enumerate() {
            let image_path = self
                .current_path
                .as_ref()
                .unwrap()
                .join(format!("image_{image:05}_{i}"));
            fs::write(image_path, &message)?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Credentials {
    access_key: String,
    secret_key: String,
}
pub struct S3Writer {
    client: minio::s3::Client,
    bucket: String,
    /// Bucket sub-path, if present. Guaranteed to not end with '/'
    bucket_path: String,
}

/// Given an absolute URL path (starting with '/'), work out the bucket and bucket path
///
/// The bucket is the first item in the path. The subpath within the bucket is
/// any remaining path.
fn get_bucket_and_path(path: &str) -> Option<(String, Option<String>)> {
    let stripped = path.strip_prefix("/")?;

    match stripped.split_once("/") {
        None => {
            if stripped.is_empty() {
                None
            } else {
                Some((stripped.to_string(), None))
            }
        }
        Some((prefix, suffix)) => {
            if prefix.is_empty() {
                None
            } else {
                Some((
                    prefix.to_string(),
                    if suffix.is_empty() {
                        None
                    } else {
                        Some(suffix.to_string())
                    },
                ))
            }
        }
    }
}

impl S3Writer {
    pub async fn new(endpoint: &str, credentials: Option<&Path>) -> Result<Self> {
        let url = Url::parse(endpoint)?;

        // Get the host contact details
        let host = url.host().unwrap();
        let port = url
            .port_or_known_default()
            .ok_or(anyhow!("Have not specified port"))?;
        let server: BaseUrl = format!("http://{host}:{port}").parse()?;

        // Work out credentials
        let provider = if let Some(path) = credentials {
            let creds: Credentials = toml::from_slice(&fs::read(path)?)?;
            StaticProvider::new(&creds.access_key, &creds.secret_key, None)
        } else {
            warn!("No credentials specified, falling back to anonymous access");
            StaticProvider::new("", "", None)
        };

        // Work out the bucket name from the url path
        let Some((bucket, bucket_path)) = get_bucket_and_path(url.path()) else {
            return Err(anyhow!(
                "Unspecified or invalid bucket specified in {url}. Please pass s3://server:port/bucket"
            ));
        };
        let client = s3::Client::new(server, Some(Box::new(provider)), None, None)?;

        if !client.bucket_exists(&bucket).send().await?.exists {
            return Err(anyhow!("Bucket {bucket} does not exist!"));
        }
        debug!("Successfully connected to {url} and confirmed bucket exists");
        Ok(Self {
            client,
            bucket,
            bucket_path: bucket_path
                .map(|s| s.strip_suffix("/").unwrap_or(&s).to_string())
                .unwrap_or(String::new()),
        })
    }
    fn upload_file(&self, path: &str, data: Vec<u8>) {
        let client = self.client.clone();
        let path = path.to_string();
        let bucket = self.bucket.clone();

        tokio::task::spawn(async move {
            match client
                .put_object(bucket, &path, SegmentedBytes::from(Bytes::from(data)))
                .send()
                .await
            {
                Ok(_) => info!("Successfully uploaded {path}"),
                Err(e) => warn!("Failed to asynchronously upload data to {path}: {e}"),
            }
        });
    }
}

impl AcquisitionWriter for S3Writer {
    fn handle_start(&mut self, series: usize, messages: Vec<Vec<u8>>) -> Result<()> {
        let path = format!("{}/{}", self.bucket_path, series);
        info!("Writing new acquisition {series} to {}", path);
        for (i, message) in messages.into_iter().enumerate() {
            self.upload_file(&format!("{path}/start_{i}"), message);
        }
        Ok(())
    }

    fn handle_end(&mut self, series: usize) -> io::Result<()> {
        //     fs::write(
        //     self.current_path.as_ref().unwrap().join("end"),
        //     format!(r#"{{"htype":"dseries_end-1.0","series":{series}}}"#).as_bytes(),
        // )?;
        self.upload_file(
            &format!("{}/{series}/end", self.bucket_path),
            format!(r#"{{"htype":"dseries_end-1.0","series":{series}}}"#)
                .as_bytes()
                .to_vec(),
        );
        Ok(())
    }

    fn handle_image(&self, series: usize, image: usize, messages: Vec<Vec<u8>>) -> io::Result<()> {
        let prefix = format!("{}/{}/image_{image:05}", self.bucket_path, series);
        // info!("Writing new acquisition {series} to {}", path);
        for (i, message) in messages.into_iter().enumerate() {
            self.upload_file(&format!("{prefix}_{i}"), message);
        }
        Ok(())
    }
}

/// Keep track of the current writer state
#[derive(Copy, Clone)]
enum AcquisitionState {
    /// Waiting for the header packet of a new capture series
    Waiting,
    /// Currently processing a specific series
    InAcquisition(usize),
    /// We are currently skipping anything matching a specific series
    Skipping(usize),
}

pub struct AcquisitionLifecycle {
    state: AcquisitionState,
    writer: Box<dyn AcquisitionWriter>,
}

impl AcquisitionLifecycle {
    pub fn new(writer: Box<dyn AcquisitionWriter>) -> Self {
        Self {
            state: AcquisitionState::Waiting,
            writer,
        }
    }

    fn handle_messages(
        &mut self,
        header: &DetectorHeader,
        messages: Vec<Vec<u8>>,
    ) -> Result<AcquisitionState> {
        match (&self.state, header) {
            (AcquisitionState::Waiting, DetectorHeader::Header { series, .. }) => {
                // We are starting a new acquisition!
                self.writer.handle_start(*series, messages)?;
                Ok(AcquisitionState::InAcquisition(*series))
            }
            (AcquisitionState::Waiting, _) => {
                // We got something other than a start header while waiting. Ignore this series.
                warn!(
                    "Waiting for new collection, got a {header:?} message instead. Skipping series."
                );
                Ok(AcquisitionState::Skipping(header.series()))
            }
            (AcquisitionState::InAcquisition(s), DetectorHeader::Image { series, .. })
                if s != series =>
            {
                // Normal image packet receipt, except the series changed!?!?
                error!(
                    "Detector stream switched series from {s} to {series} without start or end packets! Skipping rest of series."
                );
                Ok(AcquisitionState::Skipping(*series))
            }
            (AcquisitionState::InAcquisition(s), DetectorHeader::Image { frame, .. }) => {
                // Normal receipt of an image packet
                self.writer.handle_image(*s, *frame, messages)?;
                Ok(AcquisitionState::InAcquisition(*s))
            }
            (AcquisitionState::InAcquisition(s), DetectorHeader::SeriesEnd { .. }) => {
                // Normal ending of an image series
                self.writer.handle_end(*s)?;
                Ok(AcquisitionState::Waiting)
            }
            (AcquisitionState::InAcquisition(old_s), DetectorHeader::Header { series, .. }) => {
                // "Premature" end of an acquisition. This could indicate that
                // the end packet was lost, so we should warn but continue with
                // the new acquisition.
                warn!("Header for new series {series} recieved before end packet for {old_s}");
                self.writer.handle_end(*old_s)?;
                self.writer.handle_start(*series, messages)?;
                Ok(AcquisitionState::InAcquisition(*series))
            }
            (AcquisitionState::Skipping(_), DetectorHeader::Header { series, .. }) => {
                // We were ignoring a collection, now we have a new one
                self.writer.handle_start(*series, messages)?;
                Ok(AcquisitionState::InAcquisition(*series))
            }
            (AcquisitionState::Skipping(series), _) => {
                // Anything else except the new header we keep skipping
                Ok(AcquisitionState::Skipping(*series))
            }
        }
    }

    pub fn handle(&mut self, messages: Vec<Vec<u8>>) -> Result<()> {
        debug!(
            "Received: {} messages, sizes: [{}]",
            messages.len(),
            messages
                .iter()
                .map(|m| m.len().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        // Parse the header message... if this isn't valid then nothing is
        let header_0: DetectorHeader =
            serde_json::from_slice(&messages.first().expect("Got empty message set in Writer"))?;

        self.state = match self.handle_messages(&header_0, messages) {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to handle message headed by {header_0:?}: {e}");
                self.state
            }
        };

        Ok(())
    }
    /// Called when the "mirror" state is changed
    ///
    /// Can be used to clean up any opened resources
    pub fn toggle(&self, _to: bool) {}
}
