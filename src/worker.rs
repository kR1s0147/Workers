use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{Arc, atomic::AtomicUsize},
    thread,
};

use crate::{
    async_chan::{RecvFut, SendFut},
    syncwaker::SyncWaker,
    wakers::Wakers,
};

#[derive(Debug)]
pub enum Error<T> {
    RecvError,
    SendError(T),
    Closed,
    Full(T),
    Empty,
}

pub trait WakeSignal {
    /// Wakesup the signal
    fn wake(&self) -> bool;
}

struct Data<T> {
    stamp: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

pub struct Channel<T> {
    head: AtomicUsize,
    tail: AtomicUsize,
    slots: Box<[Data<T>]>,
    capacity: usize,
    send_wakers: Wakers,
    recv_wakers: Wakers,
}

unsafe impl<T: Send> Send for Channel<T> {}
unsafe impl<T: Send> Sync for Channel<T> {}

impl<T> Channel<T> {
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|i| Data {
                stamp: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Channel {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            slots,
            capacity,
            send_wakers: Wakers::new(),
            recv_wakers: Wakers::new(),
        }
    }

    pub fn register_sender_waker(&self, waker: Box<dyn WakeSignal>) {
        let guard = self.send_wakers.lock();
        let senders = unsafe { &mut *guard.wakers.wakers.get() };
        senders.push(waker);
    }

    pub fn register_receiver_waker(&self, waker: Box<dyn WakeSignal>) {
        let guard = self.recv_wakers.lock();
        let receivers = unsafe { &mut *guard.wakers.wakers.get() };
        receivers.push(waker);
    }

    fn wake_sender(&self) {
        let guard = self.send_wakers.lock();
        let senders = unsafe { &mut *guard.wakers.wakers.get() };
        for waker in senders.drain(..) {
            if waker.wake() {
                return;
            }
        }
    }

    fn wake_receiver(&self) {
        let guard = self.recv_wakers.lock();
        let receivers = unsafe { &mut *guard.wakers.wakers.get() };
        for waker in receivers.drain(..) {
            if waker.wake() {
                return;
            }
        }
    }

    pub fn try_send(&self, item: T) -> Result<(), Error<T>> {
        if self.capacity == 0 {
            return Err(Error::Full(item));
        }

        let head = self.head.load(std::sync::atomic::Ordering::Acquire);
        let tail = self.tail.load(std::sync::atomic::Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.capacity {
            return Err(Error::Full(item));
        }

        let pos = tail;
        let index = pos % self.capacity;
        let slot = &self.slots[index];
        let slot_seq = slot.stamp.load(std::sync::atomic::Ordering::Acquire);
        if slot_seq == pos {
            if self
                .tail
                .compare_exchange_weak(
                    pos,
                    pos + 1,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                unsafe {
                    (*slot.data.get()).write(item);
                }

                slot.stamp
                    .store(pos + 1, std::sync::atomic::Ordering::Release);
                self.wake_receiver();
                return Ok(());
            }
        }

        return Err(Error::SendError(item));
    }

    pub fn try_recv(&self) -> Result<T, Error<T>> {
        let pos = self.head.load(std::sync::atomic::Ordering::Acquire);
        let index = pos % self.capacity;
        let slot = &self.slots[index];
        let slot_seq = slot.stamp.load(std::sync::atomic::Ordering::Acquire);
        if slot_seq == pos + 1 {
            if self
                .head
                .compare_exchange_weak(
                    pos,
                    pos + 1,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                let val = unsafe { (*slot.data.get()).assume_init_read() };

                slot.stamp
                    .store(pos + self.capacity, std::sync::atomic::Ordering::Release);
                self.wake_sender();
                return Ok(val);
            }
        } else {
            return Err(Error::Empty);
        }
        return Err(Error::RecvError);
    }
}

pub struct Sender<T> {
    pub chan: Arc<Channel<T>>,
}
impl<T> Sender<T> {
    pub fn send(&self, item: T) -> Result<(), Error<T>> {
        let mut val = item;
        loop {
            match self.chan.try_send(val) {
                Ok(_) => return Ok(()),
                Err(Error::Full(t)) => {
                    val = t;
                    let waker = Box::new(SyncWaker::new());
                    self.chan.register_sender_waker(waker);
                    thread::park();
                }
                Err(Error::SendError(t)) => {
                    val = t;
                }
                res => return res,
            }
        }
    }

    pub fn send_async(&self, item: T) -> SendFut<'_, T> {
        SendFut::new(self, Some(item))
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            chan: self.chan.clone(),
        }
    }
}

pub struct Receiver<T> {
    pub chan: Arc<Channel<T>>,
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, Error<T>> {
        loop {
            match self.chan.try_recv() {
                Ok(val) => return Ok(val),
                Err(Error::Empty) => {
                    let waker = Box::new(SyncWaker::new());
                    self.chan.register_receiver_waker(waker);
                    thread::park();
                }
                Err(Error::RecvError) => {}
                res => return res,
            }
        }
    }

    pub fn recv_async(&self) -> RecvFut<'_, T> {
        RecvFut::new(self)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            chan: self.chan.clone(),
        }
    }
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Channel::new(capacity));
    (
        Sender { chan: chan.clone() },
        Receiver { chan: chan.clone() },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn try_send_and_try_recv_round_trip() {
        let (sender, receiver) = channel(2);

        assert!(sender.send(7).is_ok());
        assert_eq!(receiver.recv().unwrap(), 7);
        assert!(sender.send(42).is_ok());
        assert!(matches!(receiver.recv().unwrap(), 42));
    }

    #[test]
    fn sender_send_and_recv_work_across_threads() {
        let (sender, receiver) = channel(1);
        let sender_clone = sender.clone();
        let receiver_clone = receiver.clone();

        let handle = thread::spawn(move || sender_clone.send(42));

        let received = receiver_clone.recv();
        let send_result = handle.join().unwrap();

        assert_eq!(received.unwrap(), 42);
        assert!(send_result.is_ok());
    }

    #[test]
    fn multiple_threads_can_send_and_receive_items() {
        let (sender, receiver) = channel(3);
        let mut send_handles = Vec::new();
        let mut recv_handles = Vec::new();

        for value in 0..4usize {
            let sender_clone = sender.clone();
            send_handles.push(thread::spawn(move || {
                sender_clone.send(value).unwrap();
                println!("val send");
                value
            }));
        }

        for _ in 0..4 {
            let receiver_clone = receiver.clone();
            recv_handles.push(thread::spawn(move || {
                let val = receiver_clone.recv().unwrap();
                println!("val recv");
                val
            }));
        }

        let mut received_values = Vec::new();
        for handle in recv_handles {
            received_values.push(handle.join().unwrap());
        }

        let mut sent_values = Vec::new();
        for handle in send_handles {
            sent_values.push(handle.join().unwrap());
        }

        assert_eq!(received_values.len(), sent_values.len());
    }
}
