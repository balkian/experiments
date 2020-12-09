use std::fmt::Debug;

#[derive(Debug)]
struct Prueba1<'a>(&'a str);

#[derive(Debug)]
struct Prueba2(usize);

trait GreetTrait: Debug {
    fn greet(&self);
}

impl<T> GreetTrait for T
where T: Debug {
    fn greet(&self){
        println!("{:?}", self);
    }
}

fn main(){
    let p1 = Prueba1("hello");
    let p2 = Prueba2(5);

    p1.greet();
    p2.greet();
}
