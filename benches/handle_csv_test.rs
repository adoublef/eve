use anyhow::{Ok, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::Path,
    response::Response,
    routing::{get, head},
};
use divan::Bencher;
use eve::{
    net::http::{HttpClient, app},
    order::{Handler, Order},
};
use futures_util::TryStreamExt;
use reqwest::Client;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::{io::StreamReader, sync::CancellationToken};
use url::Url;

fn main() {
    divan::main();
}

#[derive(Debug, Clone)]
struct Arg(usize, usize, usize);

#[divan::bench(args = [Arg(1<<4, 1<<4, 1<<4), Arg(1<<3, 1<<4, 1<<5), Arg(1<<3, 1<<3, 1<<6)])] // 4096
fn handle_csv(b: Bencher, arg: &Arg) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let mut set = JoinSet::new();
    let token = CancellationToken::new();

    let token = token.clone();
    let (client, mut url, api_url) = rt
        .block_on(async {
            let (client, api_url) = api_serve(&mut set, token.clone(), arg.0, arg.1, arg.2).await?;
            let (client, url) = serve(&mut set, token.clone(), client).await?;

            Ok((client, url, api_url))
        })
        .unwrap();

    url.query_pairs_mut()
        .append_pair("base_url", api_url.as_str());

    b.bench(|| {
        rt.block_on(async {
            let response = client.get(url.clone()).send().await?.error_for_status()?;
            // what is the buffer used by reqwest
            // https://docs.rs/http-body-util/latest/http_body_util/struct.BodyStream.html
            let mut reader =
                StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));
            let mut writer = tokio::io::sink();
            assert!(tokio::io::copy(&mut reader, &mut writer).await? > 0); // should i know this?
            Ok(())
        })
        .unwrap();
    });

    // use the clone
    let cancelled = rt
        .block_on({
            async {
                token.cancel();
                for res in set.join_all().await {
                    res?
                }
                Ok(token.is_cancelled())
            }
        })
        .unwrap();
    assert!(cancelled)
}

async fn api_serve(
    set: &mut JoinSet<Result<()>>,
    token: CancellationToken,
    num_regions: usize,
    num_pages: usize,
    num_orders: usize,
) -> Result<(Client, Url)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;

    let client = Client::builder().build()?; // modify the client
    let url = Url::parse(&format!("http://{addr}"))?;

    // a vector of ids (stargeting at 10000)
    // a vector of orders
    let regions = (1..=num_regions).map(|n| 10000 + n).collect::<Vec<_>>();
    let orders = (1..=num_orders)
        .map(|_| Order::default())
        .collect::<Vec<_>>();

    let app = Router::new()
        // GET "/v1/universe/regions"
        .route("/v1/universe/regions", get(async move |()| Json(regions)))
        // HEAD "/v1/markets/{region}/orders"
        .route(
            "/v1/markets/{region}/orders",
            head(async move |Path(_): Path<usize>| {
                Response::builder()
                    .header("x-pages", num_pages)
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        // GET "/v1/markets/{region}/orders?page={page}"
        .route(
            "/v1/markets/{region}/orders",
            get(async move |Path(_): Path<usize>| Json(orders)),
        );

    set.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        Ok(())
    });

    Ok((client, url))
}

async fn serve(
    set: &mut JoinSet<Result<()>>,
    token: CancellationToken,
    api_client: Client,
) -> Result<(Client, Url)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;

    let client = Client::new();
    let url = Url::parse(&format!("http://{addr}"))?;

    set.spawn(async move {
        let handler = Handler::new(HttpClient(api_client));
        axum::serve(listener, app(handler))
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        Ok(())
    });

    Ok((client, url))
}
