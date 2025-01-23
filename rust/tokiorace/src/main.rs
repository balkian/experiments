use anyhow::Result;
use anyhow::{bail, Context};
use std::collections::HashMap;
use std::future::Future;
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task,
    time::{sleep, timeout_at, Duration, Instant},
};
use tracing::{debug, error, warn, info, instrument};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter::LevelFilter, fmt, EnvFilter};

/// Simplified version of a maelstrom event
#[derive(Debug)]
struct Event {
    id: usize,
    in_reply_to: Option<usize>,
}

/// Very simplified transport that receives from a channel and sends to another channel. In practice,
/// this would be replaced with a transport that reads/writes events from/to files or a connection.
struct Transport {
    erx: Receiver<anyhow::Error>,
    etx: Sender<anyhow::Error>,
    msg_id: usize,
    outbox: Sender<Event>,
    inbox: Receiver<Option<Event>>,
    callbacks: HashMap<usize, (oneshot::Sender<Option<Event>>, Instant)>,
}

impl Transport {
    fn new() -> (Self, Sender<Option<Event>>, Receiver<Event>) {
        let (itx, irx) = channel::<Option<Event>>(1);
        let (otx, orx) = channel::<Event>(1);
        let (etx, erx) = channel(1);
        (
            Self {
                erx,
                etx,
                msg_id: 0,
                outbox: otx,
                inbox: irx,
                callbacks: Default::default(),
            },
            itx,
            orx,
        )
    }

    #[instrument(level = "debug", skip(self))]
    async fn close(mut self) -> Result<()> {
        self.inbox.close();
        let i = Instant::now();
        self.callbacks.retain(|_, (_, t)| *t >= i);
        drop(self.etx);
        debug!("Waiting for errors to propagate");
        for (c, _t) in self.callbacks.into_values() {
            let _ = c.send(None);
        }
        if let Some(e) = self.erx.recv().await {
            return Err(e);
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    async fn send(&mut self, mut e: Event) -> Result<()> {
        e.id = self.msg_id;
        self.msg_id += 1;
        self.outbox.send(e).await.context("unable to send message")
    }

    #[instrument(level = "debug", skip(self))]
    async fn rpc_chan(
        &mut self,
        e: Event,
        expiration: Instant,
    ) -> Result<oneshot::Receiver<Option<Event>>> {
        let id = e.id;
        self.send(e).await?;
        let (tx, rx) = oneshot::channel();
        self.callbacks.insert(id, (tx, expiration));
        Ok(rx)
    }

    async fn rpc_callback<C, F>(&mut self, e: Event, cb: C, expiration: Instant) -> Result<()>
    where
        C: 'static + Send + FnOnce(oneshot::Receiver<Option<Event>>, Sender<Event>) -> F,
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let rec = self.rpc_chan(e, expiration).await?;
        let sender = self.outbox.clone();
        let tx = self.etx.clone();
        task::spawn(async move {
            if let Ok(Err(e)) = timeout_at(expiration, cb(rec, sender)).await {
                error!("propagating error '{e:?}'");
                tx.send(e)
                    .await
                    .expect("could not propagate error to transport.");
            }
        });
        Ok(())
    }

    /// #Cancellation safety: This method is cancellation safe
    #[instrument(level = "debug", skip(self))]
    async fn recv(&mut self) -> Result<Option<Event>> {
        loop {
            tokio::select! {
                rec = self.inbox.recv() => {
                    let Some(Some(nxt)) = rec else {
                        break;
                    };
                    if let Some(e) = self.process_event(nxt).await? {
                        return Ok(Some(e));
                    }
                },
                e = self.erx.recv() => {
                    error!("OHNOES, we have received an error");
                    if let Some(e) = e {
                        return Err(e);
                    }
                    info!("no more errors");
                    break;
                }
            }
        }
        Ok(None)
    }

    async fn process_event(&mut self, nxt: Event) -> Result<Option<Event>> {
        let Some(original) = nxt.in_reply_to else {
            return Ok(Some(nxt));
        };
        if let Some((c, t)) = self.callbacks.remove(&original) {
            if t >= Instant::now() {
                if let Err(e) = c.send(Some(nxt)) {
                    bail!("could not send event to callback {e:?}")
                }
            }
        }
        Ok(None)
    }
}

///// Simple macro to generate a dyn-safe closure from an async block.
//macro_rules! callback {
//    (|$e:ident, $svc:ident| $blk:block) => {
//        move |$e, $svc| {
//            async move {
//                $blk
//            }
//        }
//    }
//
//}

struct Service {
    t: Transport,
}

/// All-in-one implementation of a service. In practice, this would be a trait where the `serve`
/// part is kind of generic, and the process varies between implementers.
impl Service {
    fn new(t: Transport) -> Self {
        Self { t }
    }

    #[instrument(level = "debug", skip(self))]
    async fn process(&mut self, event: Event) -> Result<()> {
        let eid = event.id;
        info!("Service received event");
        let resp = self
            .t
            .rpc_callback(
                Event {
                    id: event.id,
                    in_reply_to: None,
                },
                move |chan: oneshot::Receiver<Option<Event>>, out: Sender<Event>| {
                    async move {
                        let Some(e) = chan.await? else {
                            warn!("Reply not received for event {eid}");
                            return Ok(());
                        };
                        info!("Received reply {e:?}");
                        out.send(Event {
                            id: 199,
                            in_reply_to: Some(e.id),
                        })
                        .await
                        .context("could not send reply.")?;
                        //bail!("forcing an error here!");
                        Ok(())
                    }
                },
                Instant::now() + Duration::from_millis(100000),
            )
            .await?;
        debug!("{:?}", &resp);
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    async fn serve(mut self) -> Result<()> {
        while let Some(e) = self.t.recv().await? {
            self.process(e).await?;
        }
        self.t.close().await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    info!("Starting example server");

    let (t, tx, mut rx) = Transport::new();

    let serv = Service::new(t);

    let tx1 = tx.clone();
    let s = tokio::spawn(async move {
        for i in 0..5 {
            debug!("Main sending event {i}");
            tx1.send(Some(Event {
                id: i,
                in_reply_to: None,
            }))
            .await
            .context("could not send original message")?;
            debug!("Main sent event {i}");
        }
        debug!("Main done sending events");
        Ok::<(), anyhow::Error>(())
    });
    debug!("Serving");
    let r = tokio::spawn(async move {
        info!("Spawning receptor");
        for _i in 0..3 {
            let Some(msg) = rx.recv().await else {
                break;
            };
            if msg.in_reply_to.is_some() {
                info!("Main received response: {msg:?}");
                continue;
            }
            info!("Main received request: {msg:?}");
            sleep(Duration::from_millis(10)).await;
            let reply = Event {
                id: 99,
                in_reply_to: Some(msg.id),
            };
            debug!("Sending reply: {:?}", &reply);
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(Some(reply)).await.expect("could not send reply");
            });
        }
        info!("Not sending more replies. Logging outgoing messages.");
        drop(tx);
        sleep(Duration::from_millis(1000)).await;
        while let Some(msg) = rx.recv().await {
            info!("Main received: {msg:?}");
        };
        Ok(())
    });
    if let Err(e) = serv.serve().await {
        error!("Have received the error: {e:?}");
        return Ok(());
    };

    info!("Served");
    s.await.context("error joining")??;
    r.await.context("error joining send")?
}
