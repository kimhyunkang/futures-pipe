//! A single-producer, single-consumer, futures-aware byte channel.
//!
//! This library provides a byte buffer which works like a UNIX pipe, but intended to work with
//! [futures](https://docs.rs/futures).
//!
//! You can use the pipe to send byte stream between two tasks. If the buffer is empty,
//! `PipeReader::read` will return `WouldBlock` error and the task will be notified when there is
//! new data. If the buffer is full, the `PipeWriter::write` will return `WouldBlock` error and the
//! task will be notified when there is new buffer space available.
//!
//! # AsyncRead and AsyncWrite
//!
//! `PipeReader` also implements `AsyncReader` and `PipeWriter` implements `AsyncWriter` from
//! [tokio-io](https://docs.rs/tokio-io) crate. If you don't need this dependency, turn off
//! `use_tokio_io` feature in your `Cargo.toml` file.
//!
//! # Closing the pipe
//!
//! When the `PipeReader` is closed or dropped, the `PipeWriter` on the other side will be notified
//! and the subsequent call to `PipeWriter::write` will return `Ok(0)`, indicating no more data will
//! be read from the reader. The data written so far, but not read by the reader, will be lost.
//!
//! When the `PipeWriter` is closed or dropped, the `PipeReader` on the other side will be notified,
//! but still can read remaining data in the buffer. When the pipe is closed and the buffer is
//! empty, the subsequent call to `PipeReader::read` will return `Ok(0)`, indicating there is no
//! more data to be read.

#[cfg(feature = "use_tokio_io")]
extern crate bytes;
extern crate futures;
#[cfg(feature = "use_tokio_io")]
extern crate tokio_io;

use std::{io, mem, slice};
use std::cmp::{self, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering as MemoryOrdering;

#[cfg(feature = "use_tokio_io")]
use bytes::BufMut;
#[cfg(feature = "use_tokio_io")]
use futures::{Async, Poll};
use futures::task::{self, Task};
#[cfg(feature = "use_tokio_io")]
use tokio_io::{AsyncRead, AsyncWrite};

const SIZE_MASK: usize = ::std::usize::MAX >> 1;
const PARITY_MASK: usize = ::std::usize::MAX ^ SIZE_MASK;

struct Inner {
    // Pointer to the original Box<[u8]>. This will be wrapped again into the Box<[u8]> when this
    // struct is dropped
    buf: *mut [u8],
    capacity: usize,

    rd_state: State,
    wr_state: State,

    closed: AtomicBool
}

struct State {
    // Next position to read/write
    // The MSB of pos stores parity bit. This bit is flipped on and off every time the value is
    // wrapped around. This helps us distinguish if the buffer is full or empty even when
    // `rd_state.pos` and `wr_state.pos` are same. When rest of the bits are the same, the buffer is
    // empty if the parity bit is the same, and is full if the parity bit is not the same
    pos: AtomicUsize,
    // Parked task
    task: Mutex<Option<Task>>,
    // false if there is no parked task
    // This is an optimization to avoid locking the mutex per every read/write
    maybe_blocked: AtomicBool
}

impl State {
    fn new() -> Self {
        State {
            pos: AtomicUsize::new(0),
            task: Mutex::new(None),
            maybe_blocked: AtomicBool::new(false)
        }
    }

    fn signal(&self) {
        if self.maybe_blocked.load(MemoryOrdering::SeqCst) {
            let mut lock = self.task.lock().expect("Lock is poisoned");
            if let Some(ref parked_task) = lock.take() {
                parked_task.notify()
            }
        }
    }
}

impl Inner {
    #[inline]
    fn wrapping_add(&self, parity: usize, pos: usize, len: usize) -> usize {
        if pos + len < self.capacity {
            (pos + len) | parity
        } else {
            (pos + len - self.capacity) | (PARITY_MASK & !parity)
        }
    }

    fn len(&self) -> usize {
        let wr_pos = self.wr_state.pos.load(MemoryOrdering::Acquire);
        let rd_pos = self.rd_state.pos.load(MemoryOrdering::Acquire);
        let end: usize = wr_pos & SIZE_MASK;
        let start: usize = rd_pos & SIZE_MASK;

        match start.cmp(&end) {
            Ordering::Less => end - start,
            Ordering::Equal => {
                let wr_parity = wr_pos & PARITY_MASK;
                let rd_parity = rd_pos & PARITY_MASK;
                if rd_parity == wr_parity {
                    0
                } else {
                    self.capacity
                }
            },
            Ordering::Greater => self.capacity - start + end
        }
    }

    fn is_empty(&self) -> bool {
        let wr_pos = self.wr_state.pos.load(MemoryOrdering::Acquire);
        let rd_pos = self.rd_state.pos.load(MemoryOrdering::Acquire);
        wr_pos == rd_pos
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let original_box = unsafe {
            Box::from_raw(self.buf)
        };
        drop(original_box);
    }
}

/// Create a new pipe with given buffer.
///
/// This function creates a pair of writer and reader, which implements `std::io::Write` and
/// `std::io::Read` respectively. The writer and reader works like a UNIX pipe, communicating with
/// each other using `capacity`-sized buffer. The pipe is futures-aware, which means they will
/// return `WouldBlock` error when they cannot continue the function, and the task will be notified
/// when the operation can be continued.
///
/// # Panics
///
/// This function panics when the buffer is empty, or the buffer size exceeds 1/2 of maximum of
/// usize
pub fn pipe_with_buffer(buf: Box<[u8]>) -> (PipeWriter, PipeReader) {
    let capacity = buf.len();
    if capacity == 0 {
        panic!("buffer is empty");
    }

    if capacity & PARITY_MASK != 0 {
        panic!("buffer capacity too large");
    }

    let inner = Arc::new(Inner {
        buf: Box::into_raw(buf),
        capacity: capacity,
        rd_state: State::new(),
        wr_state: State::new(),
        closed: AtomicBool::new(false)
    });

    let writer = PipeWriter { inner: inner.clone() };
    let reader = PipeReader { inner: inner };

    (writer, reader)
}

/// Create a new pipe with a buffer of given capacity.
///
/// This function creates a pair of writer and reader, which implements `std::io::Write` and
/// `std::io::Read` respectively. The writer and reader works like a UNIX pipe, communicating with
/// each other using `capacity`-sized buffer. The pipe is futures-aware, which means they will
/// return `WouldBlock` error when they cannot continue the function, and the task will be notified
/// when the operation can be continued.
///
/// Use `pipe_with_buffer` if you want to provide your own buffer for the pipe.
///
/// # Panics
///
/// This function panics when `capacity` is 0, or the `capacity` exceeds 1/2 of maximum of usize
pub fn pipe(capacity: usize) -> (PipeWriter, PipeReader) {
    pipe_with_buffer(vec![0; capacity].into())
}

/// The reading end of the pipe
///
/// `PipeReader` is returned from `pipe` or `pipe_with_buffer` function.
///
/// `PipeReader` implements `tokio_io::AsyncRead` by default. If you don't want dependency to
/// tokio_io, turn off `use_tokio_io` feature.
pub struct PipeReader {
    inner: Arc<Inner>
}

unsafe impl Send for PipeReader {}

impl PipeReader {
    /// Read from the pipe and work with the data.
    ///
    /// The provided closure will be called with two byte slices which contain, in order, available
    /// data. The closure must return the number of bytes it processed, or return error.
    ///
    /// When the writer on the other end is closed or dropped, and the buffer is empty, this method
    /// will return `Ok(0)` without calling the provided closure.
    ///
    /// # Errors
    ///
    /// This method will return `WouldBlock` error when there is no available data right now. When
    /// this error is returned, and the task calling this function is not finished, the task will be
    /// notified when the new data is available.
    ///
    /// Otherwise, the function will return error when the provided closure returns error.
    ///
    /// # Panics
    ///
    /// This method will panic if the number of bytes returned by the provided closure exceeds the
    /// number of available data.
    pub fn read_nb<F>(&mut self, f: F) -> io::Result<usize>
        where F: FnOnce(&[u8], &[u8]) -> io::Result<usize>
    {
        let wr_pos = self.inner.wr_state.pos.load(MemoryOrdering::Acquire);
        let rd_pos = self.inner.rd_state.pos.load(MemoryOrdering::Acquire);
        let wr_parity = wr_pos & PARITY_MASK;
        let rd_parity = rd_pos & PARITY_MASK;
        let end: usize = wr_pos & SIZE_MASK;
        let start: usize = rd_pos & SIZE_MASK;

        match start.cmp(&end) {
            Ordering::Less => {
                self.inner.rd_state.maybe_blocked.store(false, MemoryOrdering::SeqCst);

                assert!(wr_parity == rd_parity);

                let data = unsafe {
                    let buf: &[u8] = mem::transmute(self.inner.buf);
                    let ptr = buf.as_ptr();
                    slice::from_raw_parts(ptr.offset(start as isize), end - start)
                };
                let read_size = f(data, Default::default())?;
                if read_size > end - start {
                    panic!("read_size exceeds the size of available data");
                }

                let rd_pos = self.inner.wrapping_add(rd_parity, start, read_size);
                self.inner.rd_state.pos.store(rd_pos, MemoryOrdering::Release);

                if read_size > 0 {
                    self.inner.wr_state.signal();
                }

                Ok(read_size)
            },
            Ordering::Equal if wr_parity == rd_parity => {
                if self.inner.closed.load(MemoryOrdering::SeqCst) {
                    Ok(0)
                } else {
                    self.inner.rd_state.maybe_blocked.store(true, MemoryOrdering::SeqCst);

                    let parked = {
                        let mut lock = self.inner.rd_state.task.lock().expect("Lock is poisoned");

                        // Check the status again in case there was an event between status check
                        // and lock acquire
                        if self.inner.wr_state.pos.load(MemoryOrdering::SeqCst) != wr_pos
                            || self.inner.closed.load(MemoryOrdering::SeqCst)
                        {
                            false
                        } else {
                            *lock = Some(task::current());
                            true
                        }

                        // Unlike conditional variable pattern, it is OK to release lock before
                        // parking the task, because task::notify guarantees this will be polled
                        // again even if it's notified while this task is still alive
                    };

                    if parked {
                        Err(io::Error::new(io::ErrorKind::WouldBlock, "Buffer is empty"))
                    } else {
                        self.read_nb(f)
                    }
                }
            },
            _ => {
                self.inner.rd_state.maybe_blocked.store(false, MemoryOrdering::SeqCst);

                assert!(wr_parity != rd_parity);

                let capacity = self.inner.capacity;
                let slices = unsafe {
                    let buf: &[u8] = mem::transmute(self.inner.buf);
                    let ptr = buf.as_ptr();
                    (
                        slice::from_raw_parts(ptr.offset(start as isize), capacity - start),
                        slice::from_raw_parts(ptr, end)
                    )
                };
                let read_size = f(slices.0, slices.1)?;
                if read_size > capacity + end - start {
                    panic!("read_size exceeds the size of available data");
                }

                let new_rd_pos = self.inner.wrapping_add(rd_parity, start, read_size);
                self.inner.rd_state.pos.store(new_rd_pos, MemoryOrdering::Release);

                if read_size > 0 {
                    self.inner.wr_state.signal();
                }

                Ok(read_size)
            }
        }
    }

    /// Returns true if this channel does not have data
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns amount of bytes this channel has
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns max amount of bytes this channel can buffer
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Close this channel
    ///
    /// The `PipeWriter` at the other end won't be able to write any more data, and it will be
    /// unparked if it was parked
    #[inline]
    pub fn close(&mut self) {
        self.inner.closed.store(true, MemoryOrdering::SeqCst);
        self.inner.wr_state.signal();
    }
}

impl io::Read for PipeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.read_nb(|slice0, slice1| {
            if buf.len() <= slice0.len() {
                let len = buf.len();
                buf.copy_from_slice(&slice0[.. len]);
                Ok(len)
            } else {
                let len = cmp::min(slice0.len() + slice1.len(), buf.len());
                buf[.. slice0.len()].copy_from_slice(slice0);
                buf[slice0.len() .. len].copy_from_slice(&slice1[.. len - slice0.len()]);
                Ok(len)
            }
        })
    }
}

impl Drop for PipeReader {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(feature = "use_tokio_io")]
impl AsyncRead for PipeReader {
    #[inline]
    unsafe fn prepare_uninitialized_buffer(&self, _buf: &mut [u8]) -> bool {
        // We don't have to zero the buffer because AsyncRead::read does not read from the buffer
        true
    }
}

/// The writing end of the pipe
///
/// `PipeWriter` is returned from `pipe` or `pipe_with_buffer` function.
///
/// `PipeWriter` implements `tokio_io::AsyncWrite` by default. If you don't want dependency to
/// tokio_io, turn off `use_tokio_io` feature.
pub struct PipeWriter {
    inner: Arc<Inner>
}

unsafe impl Send for PipeWriter {}

impl PipeWriter {
    /// Write to the pipe with data provided by given closure.
    ///
    /// The provided closure will be called with two byte slices which contain, in order, available
    /// buffer. The closure must return the number of bytes it has written, or return error.
    ///
    /// When the writer on the other end is closed or dropped, this method will return `Ok(0)`
    /// without calling the provided closure.
    ///
    /// # Errors
    ///
    /// This method will return `WouldBlock` error when there is no available buffer space. When
    /// this error is returned, and the task calling this function is not finished, the task will be
    /// notified when the buffer is available.
    ///
    /// Otherwise, the function will return error when the provided closure returns error.
    ///
    /// # Panics
    ///
    /// This method will panic if the number of bytes returned by the provided closure exceeds the
    /// number of available buffer.
    pub fn write_nb<F>(&mut self, f: F) -> io::Result<usize>
        where F: FnOnce(&mut [u8], &mut [u8]) -> io::Result<usize>
    {
        if self.inner.closed.load(MemoryOrdering::SeqCst) {
            return Ok(0);
        }

        let rd_pos = self.inner.rd_state.pos.load(MemoryOrdering::Acquire);
        let wr_pos = self.inner.wr_state.pos.load(MemoryOrdering::Acquire);
        let rd_parity = rd_pos & PARITY_MASK;
        let wr_parity = wr_pos & PARITY_MASK;
        let end = rd_pos & SIZE_MASK;
        let start = wr_pos & SIZE_MASK;

        match start.cmp(&end) {
            Ordering::Less => {
                self.inner.wr_state.maybe_blocked.store(false, MemoryOrdering::SeqCst);

                assert!(wr_parity != rd_parity);

                let data = unsafe {
                    let buf: &mut [u8] = mem::transmute(self.inner.buf);
                    let ptr = buf.as_mut_ptr();
                    slice::from_raw_parts_mut(ptr.offset(start as isize), end - start)
                };
                let write_size = f(data, Default::default())?;
                if write_size > end - start {
                    panic!("write_size exceeds the size of available buffer");
                }

                let wr_pos = self.inner.wrapping_add(wr_parity, start, write_size);
                self.inner.wr_state.pos.store(wr_pos, MemoryOrdering::Release);

                if write_size > 0 {
                    self.inner.rd_state.signal();
                }

                Ok(write_size)
            },
            Ordering::Equal if wr_parity != rd_parity => {
                self.inner.wr_state.maybe_blocked.store(true, MemoryOrdering::SeqCst);

                let parked = {
                    let mut lock = self.inner.wr_state.task.lock().expect("Lock is poisoned");

                    // Check the status again in case there was an event between status check and
                    // lock acquire
                    if self.inner.rd_state.pos.load(MemoryOrdering::SeqCst) != rd_pos
                        || self.inner.closed.load(MemoryOrdering::SeqCst)
                    {
                        false
                    } else {
                        *lock = Some(task::current());
                        true
                    }

                    // Unlike conditional variable pattern, it is OK to release lock before parking
                    // the task, because task::notify guarantees this will be polled again even if
                    // it's notified while this task is still alive
                };

                if parked {
                    Err(io::Error::new(io::ErrorKind::WouldBlock, "Buffer is full"))
                } else {
                    self.write_nb(f)
                }
            },
            _ => {
                self.inner.wr_state.maybe_blocked.store(false, MemoryOrdering::SeqCst);

                assert!(wr_parity == rd_parity);

                let capacity = self.inner.capacity;
                let slices = unsafe {
                    let buf: &mut [u8] = mem::transmute(self.inner.buf);
                    let ptr = buf.as_mut_ptr();
                    (
                        slice::from_raw_parts_mut(ptr.offset(start as isize), capacity - start),
                        slice::from_raw_parts_mut(ptr, end)
                    )
                };
                let write_size = f(slices.0, slices.1)?;
                if write_size > capacity + end - start {
                    panic!("write_size exceeds the size of available buffer");
                }

                let wr_pos = self.inner.wrapping_add(wr_parity, start, write_size);
                self.inner.wr_state.pos.store(wr_pos, MemoryOrdering::Release);

                if write_size > 0 {
                    self.inner.rd_state.signal();
                }

                Ok(write_size)
            }
        }
    }

    /// Returns true if this channel does not have data
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns amount of bytes this channel has
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns max amount of byte this channel can buffer
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Close this channel
    ///
    /// The `PipeReader` at the other end will be able to read remaining data in the buffer, and
    /// after that, it will return `Ok(0)` when read. If the reader task was parked, it will be
    /// unparked
    #[inline]
    pub fn close(&mut self) {
        self.inner.closed.store(true, MemoryOrdering::SeqCst);
        self.inner.rd_state.signal();
    }
}

impl Drop for PipeWriter {
    fn drop(&mut self) {
        self.close();
    }
}

impl io::Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_nb(|slice0, slice1| {
            if buf.len() <= slice0.len() {
                slice0[.. buf.len()].copy_from_slice(buf);
                Ok(buf.len())
            } else {
                let len0 = slice0.len();
                let len = cmp::min(len0 + slice1.len(), buf.len());
                slice0.copy_from_slice(&buf[.. len0]);
                slice1[.. len - len0].copy_from_slice(&buf[len0 .. len]);
                Ok(len)
            }
        })
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "use_tokio_io")]
impl AsyncWrite for PipeWriter {
    #[inline]
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.close();
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod test {
    use super::{PARITY_MASK, SIZE_MASK};

    #[test]
    fn test_mask() {
        assert_eq!(0, PARITY_MASK & SIZE_MASK);
        assert_eq!(::std::usize::MAX, PARITY_MASK | SIZE_MASK);

        assert_eq!(0, PARITY_MASK.leading_zeros());
        assert!(PARITY_MASK.is_power_of_two());

        assert_eq!(1, SIZE_MASK.leading_zeros());
    }
}
