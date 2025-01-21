//use futures::future::BoxFuture;
use std::future::Future;
use std::pin::Pin;


struct Data {
    num: usize
}

async fn bar(data: &Data) -> usize {
    data.num + 666
}

async fn lorem(data: &Data) -> usize {
    data.num + 777
}

type BoxFuture<'a, T> = Box<dyn Future<Output = T> + 'a>;
type CallbackDyn = dyn for<'a> Fn(&'a Data) -> BoxFuture<'a, usize>;

struct Foo {
    one: Box<CallbackDyn>,
    data: Data,
}

impl Foo {
    fn set<F>(&mut self, f: F) 
        where F: for<'a> Fn(&'a Data) -> BoxFuture<'a, usize> + 'static {
        self.one = Box::new(f);
    }

    async fn process(mut self) {
        let func = &self.one;
        println!("Result: {}", Box::into_pin(func(&self.data)).await);
    }
}

#[tokio::main]
async fn main() {    
    let mut foo = Foo {
        one: Box::new(|i| Box::new(bar(i))),
        data: Data{num: 3},
    };
    //let mut foo = Foo::new(bar);
    let offset = 5;
    foo.set(move |d| {
        Box::new(async move {
            let i = lorem(d).await;
            i + offset
        })
    });

    foo.process().await;
    
}
