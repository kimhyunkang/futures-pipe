extern crate futures;
extern crate futures_pipe;
extern crate rand;
extern crate tokio_core;

use std::{cmp, io};

use futures::{Async, Future, Poll};
use futures_pipe::pipe;
use rand::{StdRng, Rng};
use tokio_core::reactor::Core;

struct AsyncRandomWriter<R, W> {
    rng: R,
    writer: W,
    source: Vec<u8>,
    position: usize,
    max_packet_size: usize,
}

impl <W> AsyncRandomWriter<StdRng, W>
{
    fn new(writer: W, source: Vec<u8>, max_packet_size: usize) -> io::Result<Self> {
        Ok(AsyncRandomWriter {
            rng: StdRng::new()?,
            writer,
            source,
            position: 0,
            max_packet_size,
        })
    }
}

impl <R, W> Future for AsyncRandomWriter<R, W>
    where R: Rng, W: io::Write
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        while self.position < self.source.len() {
            let len = self.rng.gen_range(1, self.max_packet_size + 1);
            let end = cmp::min(self.source.len(), self.position + len);
            match self.writer.write(&self.source[self.position .. end]) {
                Ok(0) => {
                    return Err(io::ErrorKind::BrokenPipe.into());
                },
                Ok(n) => {
                    self.position += n;
                },
                Err(e) =>
                    if let io::ErrorKind::WouldBlock = e.kind() {
                        return Ok(Async::NotReady);
                    } else {
                        return Err(e);
                    }
            }
        }

        Ok(Async::Ready(()))
    }
}

struct CumulativeRead<R, RNG> {
    reader: R,
    rng: RNG,
    buf: Vec<u8>,
    result: Vec<u8>
}

impl <R> CumulativeRead<R, StdRng> {
    fn new(reader: R, max_packet_size: usize) -> io::Result<Self> {
        Ok(CumulativeRead {
            reader,
            rng: StdRng::new()?,
            buf: vec![0; max_packet_size],
            result: Vec::new()
        })
    }
}

impl <R, RNG> Future for CumulativeRead<R, RNG>
    where R: io::Read, RNG: Rng
{
    type Item = Vec<u8>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Vec<u8>, io::Error> {
        loop {
            let len = self.rng.gen_range(1, self.buf.len() + 1);
            match self.reader.read(&mut self.buf[.. len]) {
                Ok(0) => {
                    return Ok(Async::Ready(self.result.clone()));
                },
                Ok(n) => self.result.extend_from_slice(&self.buf[.. n]),
                Err(e) =>
                    if let io::ErrorKind::WouldBlock = e.kind() {
                        return Ok(Async::NotReady)
                    } else {
                        return Err(e)
                    }
            }
        }
    }
}

#[test]
fn test() {
    let mut core = Core::new().expect("Failed to initialize reactor");
    let (writer, reader) = pipe(32);

    let mut source = vec![0; 100000];
    let mut rng = StdRng::new().expect("Failed to open RNG");
    rng.fill_bytes(&mut source);
    let sender = AsyncRandomWriter::new(writer, source.clone(), 100).expect("Failed to open RNG");
    let receiver = CumulativeRead::new(reader, 100).expect("Failed to open RNG");

    let handle = core.handle();
    handle.spawn_fn(|| {
        sender.map_err(|_| ())
    });

    let result = core.run(receiver).expect("Receiver returned error");

    assert_eq!(result, source);
}
