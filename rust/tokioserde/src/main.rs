/// A prototype design for the implementation of Maelstrom services.
/// It consists of the following main elements: 
/// * Msg<P> - For messages with generic payload P
/// * Transport Trait - A transport that can send and receive Msg<P>. The basic implementation is
/// NewlineTransport.
/// * MsgSender Trait - capable of sending Msg<P>. Transport should generate new senders through
/// `clone_sender`.
///
use tokio::{
    time::{sleep, Duration},
    task::JoinHandle, 
    sync::{oneshot, mpsc::{channel, Sender}}
};
use serde::{Serialize,Deserialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt::Debug;
use tokio::io::{self, AsyncRead, BufReader, AsyncWrite, AsyncWriteExt, AsyncBufReadExt, Lines, BufWriter};
use anyhow::{bail, Context, Result};
use std::future::Future;
use std::pin::{pin};
use tracing::{instrument, info, debug, warn};
use tracing_subscriber::{filter::EnvFilter};


#[derive(Debug,Deserialize,Serialize)]
struct Message {
    body: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Msg<P> {
    src: u64,
    dest: u64,
    body: Body<P>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Body<P>{
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    payload: P
}

impl<P> Msg<P>
where P: Serialize
{
    fn to_generic(&self) -> Result<Msg<Value>> {
        Ok(Msg {
            src: self.src.clone(),
            dest: self.dest.clone(),
            body: Body {
                msg_id: self.body.msg_id.clone(),
                in_reply_to: self.body.in_reply_to.clone(),
                payload: serde_json::to_value(&self.body.payload)?
            }
        })

    }

}

struct NewlineTransport<R> {
    input: Lines<BufReader<R>>,
    jh: JoinHandle<Result<()>>,
    output: AckSender,
}

impl NewlineTransport<io::Stdin>
{
    fn new() -> Self {
        Self::from_rw(io::stdin(), io::stdout())
    }
}

type AckSender = Sender<(Msg<Value>, oneshot::Sender<Result<usize>>)>;

impl<R> NewlineTransport<R>
where 
R: AsyncRead + Unpin,
{
    fn from_rw<W: AsyncWrite + Send + 'static>(r: R, w: W) -> Self {
        let buf = io::BufReader::new(r);
        let (output, mut rx) = channel::<(Msg<Value>, oneshot::Sender<Result<usize>>)>(1);
        let jh = tokio::spawn(async move {
            let mut out = pin!(BufWriter::new(w));
            let mut msg_id = 0; // This msg_id could be added to the Transport with an Arc or
                                // similar.
            while let Some((mut gen, back)) = rx.recv().await {
                gen.body.msg_id = Some(msg_id);
                msg_id += 1;
                let s = serde_json::to_string(&gen).context("failed to serialize message")?;
                let res = out.write(s.as_bytes()).await.context("failed to write");
                out.flush().await?;
                if let Err(res) = back.send(res) {
                    bail!("failed to send result of sending: {res:?}");
                }
            }
            Ok(())
        });
        Self {
            input: buf.lines(),
            jh,
            output,
        }
    }

}

trait Transport {
    fn clone_sender(&self) -> impl MsgSender + 'static;

    fn close(self) -> impl Future<Output=Result<()>>;

    fn recv<P: Debug + DeserializeOwned>(&mut self) -> impl Future<Output=Result<Option<Msg<P>>>>;
}

impl<R> Transport for NewlineTransport<R>
where 
R: AsyncRead + Unpin,
{
    fn clone_sender(&self) -> impl MsgSender + 'static {
        self.output.clone()
    }

    async fn close(self) -> Result<()>{
        drop(self.output);
        self.jh.await?
    }

    #[instrument(skip(self),ret)]
    async fn recv<P: Debug + DeserializeOwned>(&mut self) -> Result<Option<Msg<P>>> {
        let Some(line) = self.input.next_line().await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_str::<Msg<P>>(&line)?))
    }
}

trait MsgSender: Send {
    fn send_msg<P: Serialize + Debug + Sync>(&mut self, m: &Msg<P>) -> impl Future<Output=Result<usize>> + Send;
}


impl MsgSender for AckSender {
    #[instrument(skip(self),ret)]
    async fn send_msg<P: Serialize + Debug + Sync>(&mut self, m: &Msg<P>) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        let generic = m.to_generic()?;
        Sender::send(self, (generic, tx)).await.context("could not send message to sender")?;
        rx.await.context("could not send ack")?
    }
}

trait Service {
    type P: Debug + DeserializeOwned + Serialize;
    fn process(&mut self, m: Msg<Self::P>, s: impl MsgSender + 'static) -> impl Future<Output=Result<()>>;
    fn serve<T: Transport>(mut self, mut t: T) -> impl Future<Output=Result<()>> 
    where Self: Sized{
        async move {
            while let Some(m) = t.recv::<Self::P>().await? {
                let mut s = t.clone_sender();
                self.process(m, s).await?;
            }
            info!("Closing transport");
            t.close().await.expect("should not panic");
            Ok(())
        }
    }
}
struct EchoService {}

#[derive(Debug,Deserialize,Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo {
        echo: String
    },
}

impl Service for EchoService {
    type P = EchoPayload;
    async fn process(&mut self, m: Msg<Self::P>, mut s: impl MsgSender + 'static) -> Result<()> {
        tracing::info!("Received: {m:?}");
        tokio::spawn(async move {
            sleep(Duration::from_millis(5000)).await;
            s.send_msg(&Msg{src: 1, dest: 1, body: { Body { msg_id: Some(1), in_reply_to: None, payload: EchoPayload::Echo{ echo: String::from("Hello") }}}}).await.expect("sending failed");
        });
        Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<()>{
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let mut t = NewlineTransport::new();
    let s = EchoService{};
    s.serve(t).await
}
