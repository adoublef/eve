use crate::order::{Client, Handler, Order};
use anyhow::{Context as _, anyhow};
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{Stream, TryStreamExt as _, future::Either};
use http::{StatusCode, header};
use http_json_stream::{JsonPart, JsonStream};
use json_stream::JsonStream as NdJsonStream;
use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone)]
struct AppState<C> {
    handler: Handler<C>,
}

pub fn app<C>(handler: Handler<C>) -> Router
where
    C: Client,
{
    Router::new()
        .route("/", get(handle_csv))
        .with_state(AppState { handler })
}

#[derive(Deserialize)]
struct CsvParams {
    base_url: Url,
    has_header: Option<bool>,
}

async fn handle_csv<C>(
    State(AppState { handler }): State<AppState<C>>,
    Query(params): Query<CsvParams>,
) -> Result<Response, AppError>
where
    C: Client,
{
    let has_header = params.has_header.unwrap_or_default();
    let stream = handler.order_stream(params.base_url, has_header);
    let response = Response::builder()
        .header(header::CONTENT_TYPE, mime::TEXT_CSV.essence_str())
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"evetech.csv\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))?;
    Ok(response)
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // match on specific error
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpClient(pub reqwest::Client);

#[async_trait]
impl Client for HttpClient {
    async fn regions(&self, url: &Url) -> Result<impl Stream<Item = Result<u32>> + Send> {
        let response = self
            .0
            .get(url.join("/v1/universe/regions")?)
            .send()
            .await?
            .error_for_status()?;

        let stream = match response
            .headers()
            .get(header::CONTENT_TYPE)
            .context("Missing Content-Type header")?
            .to_str()?
        {
            "application/json" => {
                // https://docs.rs/http-json-stream/0.1.2/http_json_stream/enum.Error.html
                let stream = JsonStream::<_, _, u32>::process(response, JsonPart::level(1))
                    .map_err(|e| anyhow!(e.to_string()));
                // .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()));
                Ok(Either::Left(stream))
            }
            "application/x-ndjson" | "application/jsonl" => {
                let stream = NdJsonStream::<u32, _>::new(response.bytes_stream())
                    .map_err(|e| anyhow!(e.to_string()));
                // .map_err(io::Error::other);
                Ok(Either::Right(stream))
            }
            ct => Err(anyhow!("Invalid Content-Type: {ct}")),
        }?;

        Ok(stream) // map the error here
    }

    async fn max_pages(&self, url: &Url, region: u32) -> Result<u32> {
        Ok(self
            .0
            .head(url.join(&format!("/v1/markets/{region}/orders"))?)
            .send()
            .await?
            .error_for_status()?
            .headers()
            .get("x-pages")
            .context("Missing x-pages header")?
            .to_str()?
            .parse::<u32>()?)
    }

    async fn orders(
        &self,
        url: &Url,
        region: u32,
        page: u32,
    ) -> Result<impl Stream<Item = Result<Order>> + Send> {
        let response = self
            .0
            .get(url.join(&format!("/v1/markets/{region}/orders?page={page}"))?)
            .send()
            .await?
            .error_for_status()?;

        let stream = match response
            .headers()
            .get(header::CONTENT_TYPE)
            .context("Missing Content-Type header")?
            .to_str()?
        {
            "application/json" => {
                let stream = JsonStream::<_, _, Order>::process(response, JsonPart::level(1))
                    .map_err(|e| anyhow!(e.to_string()));
                // .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()));
                Ok(Either::Left(stream))
            }
            "application/x-ndjson" | "application/jsonl" => {
                let stream = NdJsonStream::<Order, _>::new(response.bytes_stream())
                    .map_err(|e| anyhow!(e.to_string()));
                // .map_err(io::Error::other);
                Ok(Either::Right(stream))
            }
            ct => Err(anyhow!("Invalid Content-Type: {ct}")),
        }?;

        Ok(stream) // map the error here
    }
}

pub type Result<T, E = anyhow::Error> = core::result::Result<T, E>;

#[allow(non_snake_case)]
#[inline]
pub fn Ok<T, E>(value: T) -> Result<T, E> {
    Result::Ok(value)
}

// Error
// ErrorRepr
