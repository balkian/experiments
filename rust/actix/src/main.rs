use futures::future::Future;
use actix_service::{Service};
use actix_web::{web, App, HttpRequest, HttpServer, Responder};
use actix_web::http::{header::CONTENT_TYPE, HeaderValue};


fn greet(req: HttpRequest) -> impl Responder {
    let name = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", &name)

}

fn p404(req: HttpRequest) -> impl Responder {
    println!("{:?}", req);
    actix_web::HttpResponse::NotFound()
       .content_type("text/plain")
       .body("Thou shall not pass")

}


fn main() {
    HttpServer::new(|| {
        App::new()
            .route("/", web::get().to(greet))
            .route("/{name}", web::get().to(greet))
            .default_service(
                web::route().to(p404))
            .wrap_fn(|req, srv| {
                println!("{:?}", req);
                srv.call(req).map(|mut res| {
                    res.headers_mut().insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("0001"),
                        );
                    res
                })
            })
    })
        .bind("127.0.0.1:8000")
        .expect("Can not bind to port 8000")
        .run()
        .unwrap();

}
