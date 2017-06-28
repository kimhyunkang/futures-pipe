extern crate futures;
#[cfg(feature = "tokio")]
extern crate tokio_io;

use std::{io, mem, slice};
use std::cmp::{self, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering as MemoryOrdering;

#[cfg(feature = "tokio")]
use futures::{Async, Poll};
use futures::task::{self, Task};
#[cfg(feature = "tokio")]
use tokio_io::{AsyncRead, AsyncWrite};

const SIZE_MASK: usize = ::std::usize::MAX >> 1;
const PARITY_MASK: usize = ::std::usize::MAX ^ SIZE_MASK;

struct Inner {
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
}

impl Drop for Inner {
    fn drop(&mut self) {
        let original_box = unsafe {
            Box::from_raw(self.buf)
        };
        drop(original_box);
    }
}

pub fn pipe(buf: Box<[u8]>) -> (PipeWriter, PipeReader) {
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

pub fn pipe_with_capacity(capacity: usize) -> (PipeWriter, PipeReader) {
    pipe(vec![0; capacity].into())
}

pub struct PipeReader {
    inner: Arc<Inner>
}

unsafe impl Send for PipeReader {}

impl PipeReader {
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

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

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

#[cfg(feature = "tokio")]
impl AsyncRead for PipeReader {}

pub struct PipeWriter {
    inner: Arc<Inner>
}

unsafe impl Send for PipeWriter {}

impl PipeWriter {
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

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

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

#[cfg(feature = "tokio")]
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
