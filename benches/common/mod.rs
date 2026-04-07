use axum::{Router, serve};
use reqwest::Client;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use url::Url;

pub async fn listen_and_serve(
    set: &mut JoinSet<anyhow::Result<()>>,
    token: CancellationToken,
    service: Router,
) -> anyhow::Result<(Client, Url)> {
    listen_and_serve_with_addr(set, token, |_| service).await
}

pub async fn listen_and_serve_with_addr<F>(
    set: &mut JoinSet<anyhow::Result<()>>,
    token: CancellationToken,
    // https://huonw.github.io/blog/2015/05/finding-closure-in-rust/
    f: F,
) -> anyhow::Result<(Client, Url)>
where
    F: FnOnce(&Url) -> Router,
{
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;

    let url = Url::parse(&format!("http://{addr}"))?;

    let service = f(&url);
    set.spawn(async move {
        serve(listener, service)
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        anyhow::Ok(())
    });

    Ok((Client::new(), url))
}
