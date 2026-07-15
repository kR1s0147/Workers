use std::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
};

struct Data<T> {
    send_buffer: Vec<T>,
    stand_by: Option<Vec<T>>,
    recv_buffer: Option<Vec<T>>,
}

pub struct Channel<T> {
    lock: AtomicBool,
    data: UnsafeCell<Data<T>>,
    senders: AtomicUsize,
    receivers: AtomicUsize,
}

unsafe impl<T: Send> Send for Channel<T> {}
unsafe impl<T: Send> Sync for Channel<T> {}


impl<T> Channel<T> {
    pub fn new() -> Self {
        Channel {
            lock: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            data: UnsafeCell::new(Data {
                send_buffer: Vec::new(),
                stand_by: Some(Vec::new()),
                recv_buffer: None,
            }),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Channel {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(Data {
                send_buffer: Vec::with_capacity(capacity),
                stand_by: Some(Vec::with_capacity(capacity)),
                recv_buffer: None,
            }),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
        }
    }
    fn acquire_lock(&self) {
        loop {
            for _ in 0..100 {
                if self
                    .lock
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::Acquire,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }

                std::hint::spin_loop();
            }
            std::thread::yield_now();
        }
    }

    fn release_lock(&self) {
        self.lock.store(false, std::sync::atomic::Ordering::Release);
    }

    fn send(&self, item: T) {
        self.acquire_lock();
        unsafe {
            let data = self.data.get();
            (*data).send_buffer.push(item);
            if let Some(mut stand_by_buffer) = (*data).stand_by.take() {
                std::mem::swap(&mut (*data).send_buffer, &mut stand_by_buffer);
                (*data).recv_buffer = Some(stand_by_buffer);
            }
        }
        self.release_lock();
    }

    fn recv(&self) -> Option<Vec<T>> {
        self.acquire_lock();
        let recv_buffer = unsafe {
            let data = self.data.get();
            (*data).recv_buffer.take()
        };
        self.release_lock();
        recv_buffer
    }

    fn ack(&self, mut buf: Vec<T>) {
        self.acquire_lock();
        unsafe {
            let data = self.data.get();
            if !(*data).send_buffer.is_empty() {
                std::mem::swap(&mut (*data).send_buffer, &mut buf);
                (*data).recv_buffer = Some(buf);
            } else {
                (*data).stand_by = Some(buf);
            }
        }
        self.release_lock();
    }
}

#[derive(Clone)]
pub struct Sender<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Sender<T> {
    pub fn send(&self, item: T) {
        self.channel.send(item);
    }

    pub fn clone(&self) -> Self {
        Sender {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.channel
            .senders
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Clone)]
pub struct Receiver<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Option<Vec<T>> {
        self.channel.recv()
    }

    pub fn ack(&self, buf: Vec<T>) {
        self.channel.ack(buf);
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.channel
            .receivers
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
    }
}

pub fn new_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel::with_capacity(capacity));
    (
        Sender {
            channel: channel.clone(),
        },
        Receiver { channel },
    )
}

pub fn new_unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel::new());
    (
        Sender {
            channel: channel.clone(),
        },
        Receiver { channel },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel() {
        let (sender, receiver) = new_channel::<i32>(10);
        sender.send(1);
        sender.send(2);
        let recv = receiver.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver.ack(vec![]);
        let recv = receiver.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver.ack(vec![]);
        let recv = receiver.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_multiple_senders() {
        let (sender1, receiver) = new_channel::<i32>(10);
        let sender2 = sender1.clone();
        sender1.send(1);
        sender2.send(2);
        let recv = receiver.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver.ack(vec![]);
        let recv = receiver.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver.ack(vec![]);
        let recv = receiver.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_multiple_receivers() {
        let (sender, receiver1) = new_channel::<i32>(10);
        let receiver2 = receiver1.clone();
        sender.send(1);
        sender.send(2);
        let recv = receiver1.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver1.ack(vec![]);
        let recv = receiver2.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver2.ack(vec![]);
        let recv = receiver1.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_multiple_senders_and_receivers() {
        let (sender1, receiver1) = new_channel::<i32>(10);
        let sender2 = sender1.clone();
        let receiver2 = receiver1.clone();
        sender1.send(1);
        sender2.send(2);
        let recv = receiver1.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver1.ack(vec![]);
        let recv = receiver2.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver2.ack(vec![]);
        let recv = receiver1.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_multiple_senders_and_receivers_with_buffer() {
        let (sender1, receiver1) = new_channel::<i32>(10);
        let sender2 = sender1.clone();
        let receiver2 = receiver1.clone();
        sender1.send(1);
        sender2.send(2);
        let recv = receiver1.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver1.ack(vec![]);
        let recv = receiver2.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver2.ack(vec![]);
        let recv = receiver1.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_stress() {
        let (sender1, receiver1) = new_channel::<i32>(10);
        let sender2 = sender1.clone();
        let receiver2 = receiver1.clone();
        sender1.send(1);
        sender2.send(2);
        let recv = receiver1.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver1.ack(vec![]);
        let recv = receiver2.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver2.ack(vec![]);
        let recv = receiver1.recv();
        assert_eq!(recv, None);
    }

    #[test]
    fn test_channel_stress_with_ack() {
        let (sender1, receiver1) = new_channel::<i32>(10);
        let sender2 = sender1.clone();
        let receiver2 = receiver1.clone();
        sender1.send(1);
        sender2.send(2);
        let recv = receiver1.recv();
        assert_eq!(recv, Some(vec![1]));
        receiver1.ack(vec![]);
        let recv = receiver2.recv();
        assert_eq!(recv, Some(vec![2]));
        receiver2.ack(vec![]);
        let recv = receiver1.recv();
        assert_eq!(recv, None);
    }
}
