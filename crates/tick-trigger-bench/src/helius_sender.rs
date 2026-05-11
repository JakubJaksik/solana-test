use std::time::Duration;

pub struct HeliusSender {
    client: reqwest::Client,
    endpoint: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("http status {0}: {1}")]
    HttpStatus(u16, String),
    #[error("network: {0}")]
    Network(#[from] reqwest::Error),
}

impl HeliusSender {
    pub fn new(endpoint: String) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        Ok(Self { client, endpoint })
    }

    pub async fn send_raw(&self, body: Vec<u8>) -> Result<(), SendError> {
        let resp = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SendError::HttpStatus(status.as_u16(), text));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_client() {
        let s = HeliusSender::new("http://localhost:0/fast".into()).unwrap();
        assert_eq!(s.endpoint, "http://localhost:0/fast");
    }
}
