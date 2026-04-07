mod common;

use anyhow::{Context as _, Ok, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::Path,
    response::Response,
    routing::{get, head},
};
use csv_async::AsyncReaderBuilder;
use eve::{
    net::http::{HttpClient, app},
    order::{Handler, Order},
};
use futures_util::TryStreamExt as _;
use http::{StatusCode, header};
use reqwest::Client;
use std::io;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_stream::StreamExt as _;
use tokio_util::{io::StreamReader, sync::CancellationToken};
use url::Url;

#[tokio::test]
async fn handle_csv_ok() -> Result<()> {
    let mut set = JoinSet::new();
    let token = CancellationToken::new();

    let num_regions = 1 << 1;
    let num_pages = 1 << 1;
    let num_orders = 1 << 1;

    let has_header = false;

    let (client, api_url) = common::listen_and_serve(
        &mut set,
        token.clone(),
        test_app(num_regions, num_pages, num_orders),
    )
    .await?;
    let (client, mut url) = common::listen_and_serve(
        &mut set,
        token.clone(),
        app(Handler::new(HttpClient(client))),
    )
    .await?;

    url.query_pairs_mut()
        .append_pair("base_url", api_url.as_str());

    let response = client.get(url).send().await?;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .context("Missing content-type header")?;
    assert_eq!(content_type, mime::TEXT_CSV.as_ref());
    // check content-disposition

    // include this info in the headers of the request
    // or the query, so that we can use that in our reader
    let mut rdr = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .buffer_capacity(4 << 10) // not my concern?
        .create_reader(StreamReader::new(
            response.bytes_stream().map_err(io::Error::other),
        ));
    let mut records = rdr.records();
    let mut num_records = 0;
    while let Some(record) = records.next().await {
        let record = record?;
        assert_eq!(record.len(), 12);
        num_records += 1;
    }
    assert_eq!(num_records, num_regions * num_pages * num_orders);

    token.cancel();
    for res in set.join_all().await {
        res?
    }
    assert!(token.is_cancelled());
    Ok(())
}

fn test_app(num_regions: usize, num_pages: usize, num_orders: usize) -> Router {
    let regions = (1..=num_regions).map(|n| 10000 + n).collect::<Vec<_>>();
    let orders = (1..=num_orders)
        .map(|_| Order::default())
        .collect::<Vec<_>>();

    Router::new()
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
        )
}
