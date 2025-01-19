use tokio::{
    task,
    time::{Duration, sleep},
    sync::{oneshot,
           mpsc::{Receiver, Sender, channel}
    }
};
use std::collections::{VecDeque, HashMap};
use anyhow::Result;
use anyhow::{bail, Context};
use tracing::{info, debug, instrument, Level};
use tracing_subscriber::{fmt, Registry, EnvFilter};
use tracing_subscriber::prelude::*;
use tracing_subscriber::fmt::format::FmtSpan;
use tokio::runtime::Handle;

struct Service {
    t: Transport,
}

#[derive(Debug)]
struct Event {
    id: usize,
    in_reply_to: Option<usize>,
}

type CB = dyn (FnMut(Event, &mut Transport) -> Result<()>) + Send;

enum Handler {
    Callback(Box<CB>),
    Channel(oneshot::Sender<Event>)
}

struct Transport {
    msg_id: usize,
    outbox: Sender<Event>,
    inbox: Receiver<Option<Event>>,
    callbacks: HashMap<usize, Handler>,
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
        Ok(())
    }

    #[instrument(level="debug",skip(self),ret)]
    async fn send(&mut self, e: Event) -> Result<()> {
        self.outbox.send(e).await.context("unable to send message")
    }
    #[instrument(level="debug",skip(self),ret)]
    async fn rpc(&mut self, e: Event) -> Result<Event> {
        let id = e.id;
        self.send(e).await?;
        let (tx, rx) = oneshot::channel();
        self.callbacks.insert(id, Handler::Channel(tx));
        let resp = rx.await?;
        Ok(resp)
    }

    async fn rpc_callback<C: FnMut(Event, &mut Transport) -> Result<()> + Send + Sized + 'static>(&mut self, e: Event, cb: C) -> Result<()> {
        self.callbacks.insert(e.id, Handler::Callback(Box::new(cb)));
        self.send(e).await
    }

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
            match self.callbacks.remove(&nxt.id) {
                None => {
                    return Ok(Some(nxt));
                },
                Some(Handler::Callback(mut cb)) => {
                    cb(nxt, self)?;
                },
                Some(Handler::Channel(c)) => {
                    if let Err(e) = c.send(nxt) {
                        bail!("could not send event to callback")
                    }
                }
            }
        }
    }
}

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
                                       |e, svc| {
                                           let handle = Handle::current();
                                           task::block_in_place(move || {
                                               handle.block_on(
                                                   async move {
                                                       svc.send(Event{id: 199, in_reply_to: Some(e.id)}).await
                                                   })
                                           })
                                       }
                                       ).await?;
        debug!("{:?}", &resp);
        Ok(())
    }

    #[instrument(level="debug", skip(self),ret)]
    async fn serve(mut self) -> Result<()>{
        while let Some(e) = self.t.recv().await? {
            self.process_nonblocking(e).await?;
        }
        Ok::<(), anyhow::Error>(())
    }

}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
    .with(fmt::layer())
    .with(EnvFilter::from_default_env())
    .init();

    let (t, tx, mut rx) = Transport::new();

    let mut serv = Service{t};

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
        for i in 0..5 {
            let Some(msg) = rx.recv().await else {
                break;
            };
            if msg.in_reply_to.is_some() {
                continue;
            }
            sleep(Duration::from_millis(1000)).await;
            let reply = Event{id: 99, in_reply_to: Some(msg.id)};
            debug!("Sending reply: {:?}", &reply);
            tx.send(Some(reply)).await.context("could not send reply")?;
        }
        println!("DONE WITH REPLIES");
        sleep(Duration::from_millis(5000)).await;
        println!("REALLY DONE WITH REPLIES");
        tx.send(None).await.context("could not send closing message")?;
        Ok(())
    });
    serv.serve().await?;

    debug!("Served");
    //serv.close();
    s.await.context("error joining")??;
    r.await.context("error joining send")?
}
