use std::io::Error as IOError;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use bytes::Buf;
use futures::{SinkExt, StreamExt, future::poll_fn};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfoHolder, ClientInfo, PgWireConnectionState};
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::error::{PgWireResult, PgWireError, ErrorInfo};
use pgwire::messages::response::{READY_STATUS_IDLE, ReadyForQuery};
use pgwire::messages::startup::SslRequest;
use pgwire::messages::{PgWireFrontendMessage, PgWireBackendMessage};
use pgwire::tokio::PgWireMessageServerCodec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::auth::PgLiteAuthenticator;
use crate::backend::PgLitebackendFactory;
use crate::query_handler::PgQueryProcessor;

const GSSENC_REQUEST_MAGIC_NUMBER: i32 = 80877104;

pub struct PgLiteConnection<F, A>  {
    pub connection_id: Uuid,
    #[allow(unused)]
    pub socket_addr:SocketAddr,
    #[allow(unused)]
    pub is_tls: bool, 
    #[allow(unused)]
    authenticated: bool, 
    db_factory: Arc<Mutex<F>>,
    authenticator: Arc<A>,
    portal_store: Arc<MemPortalStore<String>>,
    query_parser: Arc<NoopQueryParser>,
}

impl <F, A> PgLiteConnection<F, A> 
where F:PgLitebackendFactory, A: PgLiteAuthenticator {
    pub fn create(db_factory: Arc<Mutex<F>>, authenticator: Arc<A>) -> Self {
        let connection_id: Uuid = Uuid::new_v4();

        PgLiteConnection {
            connection_id,
            socket_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            is_tls: false,
            authenticated: false,
            db_factory, 
            authenticator,
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    pub async fn handle(&mut self, mut stream: TcpStream, socket_addr:SocketAddr) -> Result<(), IOError> {
        // Configure Socket
        stream.set_nodelay(true)?;

        // First peek for GSSENC - and always reply NO if requested
        self.peek_for_gssenc_request(&mut stream).await?;   

        // Check for a TLS connection
        let tls_acceptor:Option<TlsAcceptor> = None; // TODO: Handle TLS...
        self.is_tls = self.peek_for_tls_request(&mut stream, tls_acceptor.is_some()).await?;
        
        // Build Client Info
        let client_info: ClientInfoHolder = ClientInfoHolder::new(socket_addr, self.is_tls);

        trace!("[{}] Is SSL: {}", &self.connection_id, &self.is_tls);

        if self.is_tls {
            self.process_tls(stream, tls_acceptor.unwrap(), client_info).await?;
        } else {
            self.process(stream, client_info).await?;
        }

        Ok(())
    }

    async fn process(&mut self, stream: TcpStream, client_info: ClientInfoHolder) -> Result<(), IOError> {
        let mut socket = Framed::new(stream, PgWireMessageServerCodec::new(client_info));
        loop {
            if let Some(msg_opt) = socket.next().await {
                match msg_opt {
                    Ok(msg) => {
                        if let Err(e) = self.process_message(msg, &mut socket).await {
                            if e.to_string().contains("{TERMINATE}") {
                                break;
                            } else {
                                self.send_error_to_client(&mut socket, e).await?;
                            }
                        }
                    },
                    Err(err) => {
                        if err.to_string().contains("Connection reset by peer") {
                            debug!("[{}] Connection was closed by peer", self.connection_id);
                            break;
                        } else {
                            debug!("[{}] Unexpected connection Error: {:#?}", self.connection_id, err);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    async fn process_tls(&mut self, stream: TcpStream, tls_acceptor:TlsAcceptor, client_info: ClientInfoHolder) -> Result<(), IOError> {
        let ssl_socket = tls_acceptor.accept(stream).await?;
        let mut socket = Framed::new(ssl_socket, PgWireMessageServerCodec::new(client_info));
        // todo: No need to repeat this loop from the non-tls version... :p
        loop {
            if let Some(msg_opt) = socket.next().await {
                match msg_opt {
                    Ok(msg) => {
                        if let Err(e) = self.process_message(msg, &mut socket).await {
                            if e.to_string().contains("{TERMINATE}") {
                                break;
                            } else {
                                self.send_error_to_client(&mut socket, e).await?;
                            }
                        }
                    },
                    Err(err) => {
                        if err.to_string().contains("Connection reset by peer") {
                            debug!("[{}] Connection was closed by peer", self.connection_id);
                            break;
                        } else {
                            debug!("[{}] Unexpected connection Error: {:#?}", self.connection_id, err);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_message<S>(&mut self, message: PgWireFrontendMessage, socket: &mut Framed<S, PgWireMessageServerCodec>) -> PgWireResult<()> 
    where S: AsyncRead + AsyncWrite + Unpin + Send + Sync, {
        match socket.state() {
            PgWireConnectionState::AwaitingStartup
            | PgWireConnectionState::AuthenticationInProgress => {
                // Handle Authentication phase .... 
                self.authenticator.on_startup(socket, message).await?;
            }
            _ => {
                // Reload the backend - in case it's been disconnected and needs to be re-opened since the last query was done...
                let backend = { self.db_factory.lock().unwrap().create_backend(socket.metadata())? };
                let portal = self.portal_store.clone();
                let parser = self.query_parser.clone();
                let query_handler = PgQueryProcessor::create(backend, portal, parser);
                // Process Query Message
                trace!("Handling Message: {:#?}", message);
                match message {
                    PgWireFrontendMessage::Query(query) => {
                        query_handler.on_query(socket, query).await?;
                    }
                    PgWireFrontendMessage::Parse(parse) => {
                        query_handler.on_parse(socket, parse).await?;
                    }
                    PgWireFrontendMessage::Bind(bind) => {
                        query_handler.on_bind(socket, bind).await?;
                    }
                    PgWireFrontendMessage::Execute(execute) => {
                        query_handler.on_execute(socket, execute).await?;
                    }
                    PgWireFrontendMessage::Describe(describe) => {
                        query_handler.on_describe(socket, describe).await?;
                    }
                    PgWireFrontendMessage::Sync(sync) => {
                        query_handler.on_sync(socket, sync).await?;
                    }
                    PgWireFrontendMessage::Close(close) => {
                        query_handler.on_close(socket, close).await?;
                    }
                    PgWireFrontendMessage::Terminate(_) => {
                        return Err(PgWireError::ApiError("{TERMINATE}".into()));
                    }
                    // todo: should we be handling the flush message?
                    _ => { } 
                }
            }
        }
        Ok(())
    }

    async fn send_error_to_client<S>(&mut self, socket: &mut Framed<S, PgWireMessageServerCodec>, error: PgWireError) -> Result<(), IOError>
    where S: AsyncRead + AsyncWrite + Unpin + Send + Sync {
        match error {
            PgWireError::UserError(error_info) => {
                socket.feed(PgWireBackendMessage::ErrorResponse((*error_info).into())).await?;
                socket.feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(READY_STATUS_IDLE))).await?;
                socket.flush().await?;
            }
            PgWireError::ApiError(e) => {
                let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
                socket.feed(PgWireBackendMessage::ErrorResponse(error_info.into())).await?;
                socket.feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(READY_STATUS_IDLE))).await?;
                socket.flush().await?;
            }
            _ => {
                let error_info = ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string());
                socket.send(PgWireBackendMessage::ErrorResponse(error_info.into())).await?;
                socket.close().await?;
            }
        }

        Ok(())
    }

    async fn peek_for_tls_request(&self, tcp_socket: &mut TcpStream, tls_supported: bool) -> Result<bool, IOError> {
        let found = self.peek_for_magic(tcp_socket, SslRequest::BODY_MAGIC_NUMBER, true).await?;
        if found {
            if tls_supported {
                tcp_socket.write_all(b"S").await?;
                return Ok(true);
            } else {
                tcp_socket.write_all(b"N").await?;
                return Ok(false);
            }
        }
        Ok(false)
    }

    async fn peek_for_gssenc_request(&self, tcp_socket: &mut TcpStream) -> Result<bool, IOError> {
        let found = self.peek_for_magic(tcp_socket, GSSENC_REQUEST_MAGIC_NUMBER, true).await?;
        if found {
            tcp_socket.write_all(b"N").await?;  // Always NO - we don't support!    
        }
        Ok(false)
    }


    async fn peek_for_magic(&self, tcp_socket: &mut TcpStream, magic_number:i32, consume_bytes_if_found:bool) -> Result<bool, IOError> {
        let mut buf: [u8; 8] = [0u8; SslRequest::BODY_SIZE];
        let mut buf = ReadBuf::new(&mut buf);
        loop {
            let size = poll_fn(|cx| tcp_socket.poll_peek(cx, &mut buf)).await?;
            if size == 0 {
                // the tcp_stream has ended
                return Ok(false);
            }

            if size == SslRequest::BODY_SIZE {
                let mut buf_ref = buf.filled();

                buf_ref.get_i32(); // skip first 4 bytes (it's the length)
                if buf_ref.get_i32() == magic_number {
                    if consume_bytes_if_found {
                        // Consume the bytes
                        tcp_socket.read_exact(&mut [0u8; SslRequest::BODY_SIZE]).await?;
                    }
                    return Ok(true);
                } else { 
                    return Ok(false);
                }
            }
        }
    }
}

