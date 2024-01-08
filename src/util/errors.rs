use std::fmt::Debug;

pub trait AsAnyhow: Sized {
    type Ok;
    #[track_caller]
    fn anyhow(self) -> anyhow::Result<Self::Ok>;

    #[track_caller]
    fn anyhow_as<M: Into<String>>(self, msg: M) -> anyhow::Result<Self::Ok>;


    #[track_caller]
    fn track_err(self) -> anyhow::Result<Self::Ok> { self.anyhow_as("") }
}
impl<T> AsAnyhow for Option<T> {
    type Ok = T;
    fn anyhow(self) -> anyhow::Result<T> { 
        match self {
            Some(x) => Ok(x),
            None => {
                let loc = std::panic::Location::caller();
                Err(anyhow::anyhow!("[{}:{}] {}", loc.file(), loc.line().to_string(), "No value"))
            },
        }
    }
    fn anyhow_as<M: Into<String>>(self, msg: M) -> anyhow::Result<T>{
        match self {
            Some(x) => Ok(x),
            None => {
                let loc = std::panic::Location::caller();
                Err(anyhow::anyhow!("[{}:{}] {}", loc.file(), loc.line().to_string(), Into::<String>::into(msg)))
            },
        }
    }
}

impl<T,E: Debug> AsAnyhow for std::result::Result<T,E> {
    type Ok = T;
    fn anyhow(self) -> anyhow::Result<T> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                let loc = std::panic::Location::caller();
                Err(anyhow::anyhow!("[{}:{}] {}\n- {:?}", loc.file(), loc.line().to_string(), "error", x))
            },
        }
    }

    fn anyhow_as<M: Into<String>>(self, msg: M) -> anyhow::Result<T> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                let loc = std::panic::Location::caller();
                Err(anyhow::anyhow!("[{}:{}] {}\n- {:?}", loc.file(), loc.line().to_string(), Into::<String>::into(msg), x))
            },
        }
    }
}

pub trait CatchProcessor<T> {
    type Out;
    fn wrap_ok(v: T) -> Self::Out;
    fn catch_err<Err: Debug>(v: Err, msg: &str) -> Self::Out;
}
pub struct DefaultCatchProcessor;
impl<T> CatchProcessor<T> for DefaultCatchProcessor {
    type Out = Option<T>;
    fn wrap_ok(v: T) -> Self::Out { Some(v) }
    #[track_caller]
    fn catch_err<Err: Debug>(v: Err, msg: &str) -> Self::Out {
        let loc = std::panic::Location::caller();
        error!("{}\n- [{}:{}] {}\n- {:?}", "", loc.file(), loc.line().to_string(), msg, v);
        None
    }
}

pub trait UnwrapPrint<T, E: Debug>: Sized {
    #[track_caller]
    fn catch(self, msg: &str) -> Option<T> { self.catch_with::<DefaultCatchProcessor>(msg) }
    #[track_caller]
    fn catch_map<R>(self, msg: &str, v: fn(E)->R) -> Result<T,R>;
    #[track_caller]
    fn catch_with<P: CatchProcessor<T>>(self, msg: &str) -> P::Out;
}
impl<T, E:Debug> UnwrapPrint<T, E> for Result<T,E> {
    #[track_caller]
    fn catch_with<P: CatchProcessor<T>>(self, msg: &str) -> P::Out {
        match self {
            Ok(v) => P::wrap_ok(v),
            Err(v) => P::catch_err(v, msg),
        }
    }
    #[track_caller]
    fn catch_map<R>(self, msg: &str, f: fn(E)->R) -> Result<T,R> {
        match self {
            Ok(v) => Ok(v),
            Err(v) => {
                let loc = std::panic::Location::caller();
                error!("\n- [{}:{}] {}\n- {v:?}", loc.file(), loc.line().to_string(), msg);
                Err((f)(v))
            },
        }
    }
}

impl<T> UnwrapPrint<T, ()> for Option<T> {
    fn catch_with<P: CatchProcessor<T>>(self, msg: &str) -> P::Out {
        match self {
            Some(v) => P::wrap_ok(v),
            None => P::catch_err(anyhow::anyhow!("\n- null"), msg),
        }
    }
    fn catch_map<R>(self, msg: &str, f: fn(())->R) -> Result<T,R> {
        match self {
            Some(v) => Ok(v),
            None => {
                error!("\n- {msg}");
                Err((f)(()))
            },
        }
    }
}

impl<S: AsRef<str> + Debug + std::fmt::Display> UnwrapPrint<S, S> for S {
    fn catch_with<P: CatchProcessor<S>>(self, msg: &str) -> P::Out {
        P::catch_err(anyhow::anyhow!("\n- {self}"), msg)
    }
    fn catch_map<R>(self, msg: &str, f: fn(S)->R) -> Result<S,R> {
        error!("\n- {msg}: {self}");
        Err((f)(self))
    }
}