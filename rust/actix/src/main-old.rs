use futures::future::Future;
use actix_service::Service;
use actix_web::{web, App};
use actix_web::http::{header::CONTENT_TYPE, HeaderValue};


fn main() {
    let app = App::new()
        .wrap_fn(|req, srv|
                 srv.call(req).map(|mut res| {
                     res.headers_mut().insert(
                         CONTENT_TYPE, HeaderValue::from_static("text/plain"),
                     );
                     res
                 }))
        .route(
            "/index.html",
            web::get().to(|| "Hello, middleware!"),
        );
}
