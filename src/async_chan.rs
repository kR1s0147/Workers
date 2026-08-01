use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    async_wakers::AsyncWaker,
    worker::{Error, Receiver, Sender},
};

pub struct SendFut<'a, T> {
    sender: &'a Sender<T>,
    async_waker: Option<AsyncWaker>,
    item: Option<T>,
}

impl<'a, T> SendFut<'a, T> {
    pub fn new(sender: &'a Sender<T>, item: Option<T>) -> Self {
        Self {
            sender,
            async_waker: None,
            item,
        }
    }
}

impl<T: Unpin> Future for SendFut<'_, T> {
    type Output = Result<(), Error<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.item.is_none() {
            return Poll::Ready(Ok(()));
        }
        let item = this.item.take().unwrap();
        match this.sender.chan.try_send(item) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(Error::Full(t)) | Err(Error::SendError(t)) => {
                this.item = Some(t);
                if this.async_waker.is_none() {
                    let async_waker = AsyncWaker::new(cx.waker().clone());
                    this.sender
                        .chan
                        .register_sender_waker(Box::new(async_waker.clone()));
                    this.async_waker = Some(async_waker);
                }
                this.async_waker
                    .as_mut()
                    .unwrap()
                    .update_waker(cx.waker().clone());
                Poll::Pending
            }
            Err(Error::Closed) => Poll::Ready(Err(Error::Closed)),
            _ => unreachable!(),
        }
    }
}

pub struct RecvFut<'a, T> {
    receiver: &'a Receiver<T>,
    async_waker: Option<AsyncWaker>,
}

impl<'a, T> RecvFut<'a, T> {
    pub fn new(receiver: &'a Receiver<T>) -> Self {
        Self {
            receiver,
            async_waker: None,
        }
    }
}

impl<T: Unpin> Future for RecvFut<'_, T> {
    type Output = Result<T, Error<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.receiver.chan.try_recv() {
            Ok(val) => Poll::Ready(Ok(val)),
            Err(Error::Empty) | Err(Error::RecvError) => {
                if this.async_waker.is_none() {
                    let async_waker = AsyncWaker::new(cx.waker().clone());
                    this.receiver
                        .chan
                        .register_receiver_waker(Box::new(async_waker.clone()));
                    this.async_waker = Some(async_waker);
                }
                this.async_waker
                    .as_mut()
                    .unwrap()
                    .update_waker(cx.waker().clone());
                Poll::Pending
            }
            Err(Error::Closed) => Poll::Ready(Err(Error::Closed)),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    struct TestWake {
        wake_count: Arc<AtomicUsize>,
    }

    impl Wake for TestWake {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_once<F: Future + Unpin>(
        future: &mut F,
        wake_count: &Arc<AtomicUsize>,
    ) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(TestWake {
            wake_count: wake_count.clone(),
        }));
        let mut cx = Context::from_waker(&waker);
        Pin::new(future).poll(&mut cx)
    }

    #[test]
    fn send_async_waits_for_receiver() {
        let (sender, receiver) = crate::worker::channel(1);
        sender.send(7).unwrap();

        let wake_count = Arc::new(AtomicUsize::new(0));
        let mut send_fut = sender.send_async(8);

        assert!(matches!(
            poll_once(&mut send_fut, &wake_count),
            Poll::Pending
        ));
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);

        assert_eq!(receiver.recv().unwrap(), 7);
        assert!(wake_count.load(Ordering::SeqCst) > 0);

        assert!(matches!(
            poll_once(&mut send_fut, &wake_count),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn recv_async_waits_for_sender() {
        let (sender, receiver) = crate::worker::channel(1);

        let wake_count = Arc::new(AtomicUsize::new(0));
        let mut recv_fut = receiver.recv_async();

        assert!(matches!(
            poll_once(&mut recv_fut, &wake_count),
            Poll::Pending
        ));
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);

        assert!(sender.send(9).is_ok());
        assert!(wake_count.load(Ordering::SeqCst) > 0);

        assert!(matches!(
            poll_once(&mut recv_fut, &wake_count),
            Poll::Ready(Ok(9))
        ));
    }
}
