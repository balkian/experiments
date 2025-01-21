use tokio::{
    task,
    time::{Instant, Duration, sleep},
    sync::{oneshot,
           mpsc::{Receiver, Sender, channel}
    }
};
use std::collections::{HashMap};
use anyhow::Result;
use anyhow::{bail, Context};
use tracing::{info, debug, instrument};
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use std::future::Future;



/// Simplified version of a maelstrom event
#[derive(Debug)]
struct Event {
    id: usize,
    in_reply_to: Option<usize>,
}


type BoxFuture<'a, T> = Box<dyn Future<Output = T> + 'a>;
type CallbackDyn = dyn for<'a> Fn(Event, &'a mut Transport) -> BoxFuture<'a, Result<()>>;

enum Handler {
    Callback(Box<CallbackDyn>),
    Channel(oneshot::Sender<Event>)
}

/// Very simplified transport that receives from a channel and sends to another channel. In practice,
/// this would be replaced with a transport that reads/writes events from/to files or a connection.
struct Transport {
    msg_id: usize,
    outbox: Sender<Event>,
    inbox: Receiver<Option<Event>>,
    callbacks: HashMap<usize, (Handler, Instant)>,
}


impl Transport {
    fn new() -> (Self, Sender<Option<Event>>, Receiver<Event>) {
        let (itx, irx) = channel::<Option<Event>>(1);
        let (otx, orx) = channel::<Event>(1);
        (Self {
            msg_id: 0,
            outbox: otx,
            inbox: irx,
            callbacks: Default::default(),
        }, itx, orx)

    }

    #[instrument(level="debug",skip(self),ret)]
    fn close(&mut self) -> Result<()> {
        self.inbox.close();
        let i = Instant::now();
        self.callbacks.retain(|_, (_, t)| *t >= i);
        if self.callbacks.is_empty() {
            Ok(())
        } else {
            bail!("some callbacks failed to launch.")
        }
    }

    #[instrument(level="debug",skip(self),ret)]
    async fn send(&self, e: Event) -> Result<()> {
        self.outbox.send(e).await.context("unable to send message")
    }

    #[instrument(level="debug",skip(self),ret)]
    async fn rpc(&mut self, e: Event) -> Result<Event> {
        let rx = self.rpc_chan(e).await?;
        let resp = rx.await?;
        Ok(resp)
    }

    #[instrument(level="debug",skip(self),ret)]
    async fn rpc_chan(&mut self, e: Event) -> Result<oneshot::Receiver<Event>> {
        let id = e.id;
        self.send(e).await?;
        let (tx, rx) = oneshot::channel();
        self.callbacks.insert(id, (Handler::Channel(tx), Instant::now() + Duration::from_millis(1000)));
        Ok(rx)
    }

    async fn rpc_callback<C>(&mut self, e: Event, cb: C) -> Result<()>
        where
        C: for<'a> Fn(Event, &'a mut Transport) -> BoxFuture<'a, Result<()>> + 'static
    {
        self.callbacks.insert(e.id, (Handler::Callback(Box::new(cb)), (Instant::now() + Duration::from_millis(1000))));
        self.send(e).await
    }

    /// #Cancellation safety: This method is **not** cancellation safe, because it deals with callbacks
    #[instrument(level="debug",skip(self),ret)]
    async fn recv(&mut self) -> Result<Option<Event>> {
        loop {
            let Some(nxt) = self.inbox.recv().await else {
                return Ok(None);
            };
            let Some(nxt) = nxt else {
                return Ok(None);
            };
            let Some(original) = nxt.in_reply_to else {
                return Ok(Some(nxt));
            };
            let sp = match self.callbacks.remove(&original) {
                None => {
                    return Ok(Some(nxt));
                },
                Some((h, t)) => {
                    if t < Instant::now() {
                        continue;
                    }
                    h
                }
            };
            match sp {
                Handler::Callback(cb) => {
                    let f = Box::into_pin(cb(nxt, self));
                    f.await?;
                },
                Handler::Channel(c) => {
                    if let Err(e) = c.send(nxt) {
                        bail!("could not send event to callback {e:?}")
                    }
                }
            }
        }
    }
}

/// Simple macro to generate a dyn-safe closure from an async block.
macro_rules! callback {
    (|$e:ident, $svc:ident| $blk:block) => {
        |$e, $svc| {
            Box::new(async move {
                $blk
            })
        }
    }

}

struct Service {
    t: Transport,
}

/// All-in-one implementation of a service. In practice, this would be a trait where the `serve`
/// part is kind of generic, and the process varies between implementers.
impl Service {
    #[instrument(level="debug", skip(self),ret)]
    async fn process(&mut self, event: Event) -> Result<()>{
        let resp = self.t.rpc(Event{id: event.id, in_reply_to: None}).await?;
        debug!("{:?}", &resp);
        Ok(())
    }

    #[instrument(level="debug", skip(self),ret)]
    async fn process_nonblocking(&mut self, event: Event) -> Result<()>{
        let resp = self.t.rpc_callback(Event{id: event.id, in_reply_to: None},
                                       callback!(|e, svc| {
                                               svc.send(Event{id: 199, in_reply_to: Some(e.id)}).await
                                           })).await?;
        debug!("{:?}", &resp);
        Ok(())
    }

    #[instrument(level="debug", skip(self),ret)]
    async fn serve(mut self) -> Result<()>{
        while let Some(e) = self.t.recv().await? {
            self.process_nonblocking(e).await?;
        }
        self.t.close()
    }

}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
    .with(fmt::layer())
    .with(EnvFilter::from_default_env())
    .init();

    info!("Starting example server");

    let (t, tx, mut rx) = Transport::new();

    let serv = Service{t};

    let tx1 = tx.clone();
    let s = tokio::spawn(async move {
        for i in 0..2 {
            debug!("Sending event {i}");
            tx1.send(Some(Event{id: i, in_reply_to: None})).await.context("could not send original message")?;
        }
        Ok::<(), anyhow::Error>(())
    });
    debug!("Serving");
    let r = tokio::spawn(async move {
        for i in 0..2 {
            let Some(msg) = rx.recv().await else {
                break;
            };
            if msg.in_reply_to.is_some() {
                continue;
            }
            sleep(Duration::from_millis(200)).await;
            if i < 0 {
                let reply = Event{id: 99, in_reply_to: Some(msg.id)};
                debug!("Sending reply: {:?}", &reply);
                tx.send(Some(reply)).await.context("could not send reply")?;
            } else {
                println!("DONE WITH REPLIES");
            }
        }
        sleep(Duration::from_millis(100)).await;
        tx.send(None).await.context("could not send closing message")?;
        Ok(())
    });
    serv.serve().await?;

    info!("Served");
    s.await.context("error joining")??;
    r.await.context("error joining send")?
}
