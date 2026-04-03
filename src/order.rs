use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use csv_async::AsyncWriterBuilder;
use futures_util::{Stream, StreamExt as _, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::{io::duplex, sync::mpsc, task::JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tracing::{Instrument, trace_span};
use url::Url;

const DEFAULT_LIMIT: usize = 1;
const DEFAULT_BUF_SIZE: usize = 1;
const DEFAULT_BUF: usize = 4 << 10;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Order {
    duration: i64,
    is_buy_order: bool,
    issued: String,
    location_id: i64,
    min_volume: i64,
    order_id: i64,
    price: f64,
    range: String,
    system_id: i64,
    type_id: i64,
    volume_remain: i64,
    volume_total: i64,
}

impl Order {
    fn to_record(&self) -> [String; 12] {
        [
            self.duration.to_string(),
            self.is_buy_order.to_string(),
            self.issued.clone(),
            self.location_id.to_string(),
            self.min_volume.to_string(),
            self.order_id.to_string(),
            self.price.to_string(),
            self.range.clone(),
            self.system_id.to_string(),
            self.type_id.to_string(),
            self.volume_remain.to_string(),
            self.volume_total.to_string(),
        ]
    }
}

#[async_trait]
pub trait Client: Clone + Send + Sync + 'static {
    async fn regions(&self, url: &Url) -> Result<impl Stream<Item = Result<u32>> + Send + Unpin>;
    async fn max_pages(&self, url: &Url, region: u32) -> Result<u32>;
    async fn orders(
        &self,
        url: &Url,
        region: u32,
        page: u32,
    ) -> Result<impl Stream<Item = Result<Order>> + Send + Unpin>;
}

#[derive(Debug, Clone)]
pub struct Handler<C> {
    client: C,
}

impl<C> Handler<C>
where
    C: Client,
{
    pub fn new(client: C) -> Self {
        Self { client }
    }

    pub fn order_stream(
        &self,
        base_url: Url,
        has_header: bool,
    ) -> impl Stream<Item = Result<Bytes>> + 'static {
        let mut set = JoinSet::new();

        let (tx, regions) = mpsc::channel(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone(); // we form the string here?
            async move {
                let mut stream = client.regions(&base_url).await?;
                while let Some(id) = stream.try_next().await? {
                    tx.send(id).await?;
                }
                Ok(())
            }
            .instrument(trace_span!("regions"))
        });

        let (tx, queries) = mpsc::channel(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                ReceiverStream::new(regions)
                    .map(Ok::<_, anyhow::Error>)
                    .try_for_each_concurrent(DEFAULT_LIMIT, async |region| {
                        async {
                            let last = client.max_pages(&base_url, region).await?;
                            for page in 1..=last {
                                tx.send((region, page)).await?;
                            }
                            Ok(())
                        }
                        .instrument(trace_span!("pages", region))
                        .await
                    })
                    .await
            }
        });

        let (tx, mut records) = mpsc::channel(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                if has_header {
                    tx.send([
                        "duration".to_string(),
                        "is_buy_order".to_string(),
                        "issued".to_string(),
                        "location_id".to_string(),
                        "min_volume".to_string(),
                        "order_id".to_string(),
                        "price".to_string(),
                        "range".to_string(),
                        "system_id".to_string(),
                        "type_id".to_string(),
                        "volume_remain".to_string(),
                        "volume_total".to_string(),
                    ])
                    .await?
                }
                ReceiverStream::new(queries)
                    .map(Ok)
                    .try_for_each_concurrent(DEFAULT_LIMIT, async |(region, page)| {
                        async {
                            let mut stream = client.orders(&base_url, region, page).await?;
                            while let Some(order) = stream.try_next().await? {
                                tx.send(order.to_record()).await?;
                            }
                            Ok(())
                        }
                        .instrument(trace_span!("orders", region, page))
                        .await
                    })
                    .await
            }
        });

        let (rx, tx) = duplex(DEFAULT_BUF);
        set.spawn({
            async move {
                let mut wri = AsyncWriterBuilder::new()
                    .buffer_capacity(DEFAULT_BUF)
                    .create_writer(tx);
                while let Some(order) = records.recv().await {
                    wri.write_record(&order).await?;
                }
                wri.flush().await?;
                Ok(())
            }
            .instrument(trace_span!("csv"))
        });

        //  AsyncStream<Result<Bytes, Error>, impl Future<Output = ()>>
        let stream = try_stream! {
            let mut stream = ReaderStream::new(rx);
            while let Some(msg) = stream.next().await {
                yield msg?
            }
            while let Some(res) = set.join_next().await {
                res??;
            }
        };
        stream
    }
}

// DefaultClient

pub type Result<T, E = anyhow::Error> = core::result::Result<T, E>;

#[allow(non_snake_case)]
#[inline]
pub fn Ok<T, E>(value: T) -> Result<T, E> {
    Result::Ok(value)
}
