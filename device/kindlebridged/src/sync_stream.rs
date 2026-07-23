//! One `sync.v1` stream from opening metadata through its terminal close.

use std::sync::mpsc::{self, TryRecvError, TrySendError};
use std::sync::Arc;
use std::thread;

use kindlebridge_schema::device_protocol::{SyncReply, SyncRequest, DEFAULT_STREAM_WINDOW};
use kindlebridge_schema::{RpcError, MAX_SYNC_BLOCK_SIZE};
use kindlebridge_transport::actor::{IncomingStream as ActorIncomingStream, Stream as ActorStream};
use kindlebridge_transport::TrafficClass;
use kindlebridge_wire::{Command, Frame, FLAG_END_STREAM};
use serde::Serialize;

use crate::server::ServerError;
use crate::sync::{PullTransfer, PushTransfer, StoreError, SyncStore};

const SYNC_PIPELINE_QUEUE_DEPTH: usize = 3;

#[derive(Debug)]
enum PipelineFailure<E> {
    Stage(E),
    WorkerStopped,
    WorkerPanicked,
}

struct PipelineOutcome<A, E> {
    result: Result<(), PipelineFailure<E>>,
    last_written: Option<A>,
}

fn run_bounded_pipeline<C, T, A, E, Read, Write, Stored, MustDrain>(
    context: &mut C,
    mut read: Read,
    mut write: Write,
    mut stored: Stored,
    mut must_drain: MustDrain,
) -> PipelineOutcome<A, E>
where
    Read: FnMut(&mut C) -> Result<Option<T>, E>,
    Write: FnMut(T) -> Result<A, E> + Send,
    Stored: FnMut(&mut C, A) -> Result<(), E>,
    MustDrain: FnMut(&C) -> bool,
    T: Send,
    A: Clone + Send,
    E: Send,
{
    let (work_tx, work_rx) = mpsc::sync_channel::<T>(SYNC_PIPELINE_QUEUE_DEPTH);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<Result<A, E>>(SYNC_PIPELINE_QUEUE_DEPTH);

    thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let mut last_written = None;
            while let Ok(item) = work_rx.recv() {
                match write(item) {
                    Ok(acknowledgement) => {
                        last_written = Some(acknowledgement.clone());
                        if ack_tx.send(Ok(acknowledgement)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = ack_tx.send(Err(error));
                        break;
                    }
                }
            }
            last_written
        });

        let mut result = 'produce: loop {
            loop {
                match ack_rx.try_recv() {
                    Ok(Ok(acknowledgement)) => {
                        if let Err(error) = stored(context, acknowledgement) {
                            break 'produce Err(PipelineFailure::Stage(error));
                        }
                    }
                    Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        break 'produce Err(PipelineFailure::WorkerStopped)
                    }
                }
            }

            let mut pending = match read(context) {
                Ok(Some(item)) => item,
                Ok(None) => break Ok(()),
                Err(error) => break Err(PipelineFailure::Stage(error)),
            };
            loop {
                match work_tx.try_send(pending) {
                    Ok(()) => {
                        while must_drain(context) {
                            match ack_rx.recv() {
                                Ok(Ok(acknowledgement)) => {
                                    if let Err(error) = stored(context, acknowledgement) {
                                        break 'produce Err(PipelineFailure::Stage(error));
                                    }
                                }
                                Ok(Err(error)) => {
                                    break 'produce Err(PipelineFailure::Stage(error))
                                }
                                Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                            }
                        }
                        break;
                    }
                    Err(TrySendError::Full(item)) => {
                        pending = item;
                        match ack_rx.recv() {
                            Ok(Ok(acknowledgement)) => {
                                if let Err(error) = stored(context, acknowledgement) {
                                    break 'produce Err(PipelineFailure::Stage(error));
                                }
                            }
                            Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                            Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => loop {
                        match ack_rx.recv() {
                            Ok(Ok(acknowledgement)) => {
                                if let Err(error) = stored(context, acknowledgement) {
                                    break 'produce Err(PipelineFailure::Stage(error));
                                }
                            }
                            Ok(Err(error)) => break 'produce Err(PipelineFailure::Stage(error)),
                            Err(_) => break 'produce Err(PipelineFailure::WorkerStopped),
                        }
                    },
                }
            }
        };

        drop(work_tx);
        if result.is_ok() {
            loop {
                match ack_rx.recv() {
                    Ok(Ok(acknowledgement)) => {
                        if let Err(error) = stored(context, acknowledgement) {
                            result = Err(PipelineFailure::Stage(error));
                            break;
                        }
                    }
                    Ok(Err(error)) => {
                        result = Err(PipelineFailure::Stage(error));
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        drop(ack_rx);

        match writer.join() {
            Ok(last_written) => PipelineOutcome {
                result,
                last_written,
            },
            Err(_) => PipelineOutcome {
                result: Err(PipelineFailure::WorkerPanicked),
                last_written: None,
            },
        }
    })
}

pub(crate) fn serve(
    incoming: ActorIncomingStream,
    sync_store: &SyncStore,
) -> Result<(), ServerError> {
    let mut stream = incoming.accept(DEFAULT_STREAM_WINDOW, TrafficClass::Bulk)?;
    let request_frame = actor_data(&mut stream)?;
    let request: SyncRequest = decode(&request_frame.payload, "sync request")?;
    let block_size = match &request {
        SyncRequest::Push { block_size, .. } | SyncRequest::Pull { block_size, .. } => *block_size,
    };
    if block_size == 0 || block_size > MAX_SYNC_BLOCK_SIZE {
        return fail(
            &stream,
            RpcError::invalid_params("block_size must be between 1 and 1048576"),
        );
    }
    match request {
        SyncRequest::Push {
            transfer_id,
            remote_path,
            total_size,
            file_hash,
            block_size,
        } => {
            if request_frame.header.flags & FLAG_END_STREAM != 0 {
                return fail(
                    &stream,
                    RpcError::invalid_params("push metadata must not end the stream"),
                );
            }
            let transfer = sync_store
                .begin_push(transfer_id.as_deref(), &remote_path, total_size, &file_hash)
                .map_err(StoreError::into_rpc);
            match transfer {
                Ok(transfer) => push(stream, block_size, transfer),
                Err(error) => fail(&stream, error),
            }
        }
        SyncRequest::Pull {
            transfer_id,
            remote_path,
            offset,
            block_size,
        } => {
            if request_frame.header.flags & FLAG_END_STREAM == 0 {
                return fail(
                    &stream,
                    RpcError::invalid_params("pull metadata must end the stream"),
                );
            }
            let transfer = sync_store
                .begin_pull(transfer_id.as_deref(), &remote_path, offset)
                .map_err(StoreError::into_rpc);
            match transfer {
                Ok(transfer) => pull(stream, block_size, transfer),
                Err(error) => fail(&stream, error),
            }
        }
    }
}

fn push(
    mut stream: ActorStream,
    block_size: u32,
    mut transfer: PushTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    stream.send_data(encode(&ready)?, false)?;
    let block_size = block_size as usize;
    let digest = if transfer.is_complete() {
        loop {
            let data = actor_data(&mut stream)?;
            if data.payload.len() > block_size {
                stream.reset("sync DATA exceeds negotiated block size")?;
                return Ok(());
            }
            if !data.payload.is_empty() {
                stream.reset("completed sync push accepts only an empty final DATA")?;
                return Ok(());
            }
            if data.header.flags & FLAG_END_STREAM != 0 {
                break;
            }
        }
        None
    } else {
        struct ReceiveContext<'a> {
            stream: &'a mut ActorStream,
            block_size: usize,
            end_stream_received: bool,
        }

        let hash_state = transfer.hash_state();
        let mut context = ReceiveContext {
            stream: &mut stream,
            block_size,
            end_stream_received: false,
        };
        // A small bounded queue overlaps USB receive, BLAKE3 and backing-store
        // writes. Payloads are shared with the hash worker, so the pipeline
        // retains at most three blocks without making another full-size copy.
        let (pipeline, digest_result) = thread::scope(|scope| {
            let (hash_tx, hash_rx) = mpsc::sync_channel::<Arc<[u8]>>(0);
            let hash_worker = scope.spawn(move || {
                let mut hasher = hash_state;
                while let Ok(payload) = hash_rx.recv() {
                    hasher.update(&payload);
                }
                hasher.finalize()
            });
            let pipeline = run_bounded_pipeline(
                &mut context,
                |context| {
                    if context.end_stream_received {
                        return Ok(None);
                    }
                    let data = actor_data(context.stream)?;
                    if data.payload.len() > context.block_size {
                        return Err(ServerError::UnexpectedFrame(
                            "sync DATA exceeds negotiated block size",
                        ));
                    }
                    let is_last = data.header.flags & FLAG_END_STREAM != 0;
                    context.end_stream_received = is_last;
                    Ok(Some(Arc::<[u8]>::from(data.payload)))
                },
                |payload| {
                    hash_tx
                        .send(Arc::clone(&payload))
                        .map_err(|_| ServerError::UnexpectedFrame("sync hash worker stopped"))?;
                    let frame_start = transfer.offset();
                    transfer.write_chunk_without_hash(&payload)?;
                    Ok(frame_start)
                },
                |_context, _stored| Ok(()),
                |_context| false,
            );
            drop(hash_tx);
            let digest = hash_worker
                .join()
                .map_err(|_| ServerError::UnexpectedFrame("sync hash worker panicked"));
            (pipeline, digest)
        });
        let last_frame_start = pipeline.last_written;
        let receive_result = match pipeline.result {
            Ok(()) => Ok(()),
            Err(PipelineFailure::Stage(error)) => Err(error),
            Err(PipelineFailure::WorkerStopped) => Err(ServerError::UnexpectedFrame(
                "sync storage worker stopped before the transfer completed",
            )),
            Err(PipelineFailure::WorkerPanicked) => {
                Err(ServerError::UnexpectedFrame("sync storage worker panicked"))
            }
        };
        match receive_result.and(digest_result) {
            Ok(digest) => Some(digest),
            Err(error) => {
                if let Some(offset) = last_frame_start {
                    transfer.rollback_for_resume(offset)?;
                } else {
                    transfer.checkpoint()?;
                }
                return Err(error);
            }
        }
    };
    let status = match digest {
        Some(digest) => transfer.finish_with_digest(digest),
        None => transfer.finish(),
    };
    let status = match status {
        Ok(status) => status,
        Err(error) => return fail(&stream, error.into_rpc()),
    };
    let complete = SyncReply::Complete {
        transfer_id: status.transfer_id,
        next_offset: status.next_offset,
        total_size: status.total_size,
    };
    stream.send_data(encode(&complete)?, true)?;
    stream.close()?;
    Ok(())
}

fn pull(
    stream: ActorStream,
    block_size: u32,
    mut transfer: PullTransfer,
) -> Result<(), ServerError> {
    let ready = SyncReply::Ready {
        transfer_id: transfer.transfer_id().to_owned(),
        offset: transfer.offset(),
        total_size: transfer.total_size(),
        file_hash: transfer.file_hash().to_owned(),
    };
    stream.send_data(encode(&ready)?, false)?;
    if transfer.offset() == transfer.total_size() {
        transfer.finish()?;
        stream.send_data(Vec::new(), true)?;
    } else {
        struct PullChunk {
            payload: Vec<u8>,
            is_last: bool,
        }

        let (send_result, reader_result) = thread::scope(|scope| {
            let (chunk_tx, chunk_rx) =
                mpsc::sync_channel::<Result<PullChunk, ServerError>>(SYNC_PIPELINE_QUEUE_DEPTH);
            let reader = scope.spawn(move || {
                let result = (|| -> Result<(), ServerError> {
                    let mut buffer = vec![0_u8; block_size as usize];
                    loop {
                        let count = transfer.read_chunk(&mut buffer)?;
                        if count == 0 {
                            return Err(ServerError::UnexpectedFrame(
                                "sync source ended before declared size",
                            ));
                        }
                        let is_last = transfer.offset() == transfer.total_size();
                        if is_last {
                            transfer.finish()?;
                        } else {
                            transfer.checkpoint_if_due()?;
                        }
                        let chunk = PullChunk {
                            payload: buffer[..count].to_vec(),
                            is_last,
                        };
                        if chunk_tx.send(Ok(chunk)).is_err() {
                            return Ok(());
                        }
                        if is_last {
                            return Ok(());
                        }
                    }
                })();
                if let Err(error) = result {
                    let _ = chunk_tx.send(Err(error));
                }
            });

            let send_result = loop {
                match chunk_rx.recv() {
                    Ok(Ok(chunk)) => {
                        if let Err(error) = stream.send_data(chunk.payload, chunk.is_last) {
                            break Err(ServerError::Connection(error));
                        }
                        if chunk.is_last {
                            break Ok(());
                        }
                    }
                    Ok(Err(error)) => break Err(error),
                    Err(_) => {
                        break Err(ServerError::UnexpectedFrame(
                            "sync storage reader stopped before the transfer completed",
                        ));
                    }
                }
            };
            // If USB sending failed, wake a reader blocked on the bounded queue
            // before joining it. This keeps disconnect cleanup deterministic.
            drop(chunk_rx);
            let reader_result = reader
                .join()
                .map_err(|_| ServerError::UnexpectedFrame("sync storage reader panicked"));
            (send_result, reader_result)
        });
        send_result?;
        reader_result?;
    }
    stream.close()?;
    Ok(())
}

fn fail(stream: &ActorStream, error: RpcError) -> Result<(), ServerError> {
    stream.send_data(encode(&SyncReply::Failure { error })?, true)?;
    stream.close()?;
    Ok(())
}

fn actor_data(stream: &mut ActorStream) -> Result<Frame, ServerError> {
    let frame = stream.recv()?;
    if frame.header.command != Command::Data {
        return Err(ServerError::UnexpectedFrame(
            "expected DATA on actor service stream",
        ));
    }
    Ok(frame)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, ServerError> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    label: &'static str,
) -> Result<T, ServerError> {
    serde_json::from_slice(payload).map_err(|source| ServerError::InvalidPayload { label, source })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use super::*;

    struct TrackedChunk {
        index: usize,
        live: Arc<AtomicUsize>,
    }

    impl TrackedChunk {
        fn new(index: usize, live: Arc<AtomicUsize>, maximum: &AtomicUsize) -> Self {
            let current = live.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(current, Ordering::SeqCst);
            Self { index, live }
        }
    }

    impl Drop for TrackedChunk {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn bounded_pipeline_overlaps_reads_with_slow_storage_without_unbounded_payloads() {
        const CHUNK_COUNT: usize = 12;
        let live = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let overlapped = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicUsize::new(0));
        let write_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (writer_started_tx, writer_started_rx) = mpsc::sync_channel(1);
        let mut next = 0;
        let mut stored_count = 0;

        let outcome: PipelineOutcome<usize, &'static str> = run_bounded_pipeline(
            &mut stored_count,
            {
                let live = Arc::clone(&live);
                let maximum = Arc::clone(&maximum);
                let write_gate = Arc::clone(&write_gate);
                move |_| {
                    if next == 1 {
                        writer_started_rx
                            .recv_timeout(Duration::from_secs(1))
                            .map_err(|_| "writer did not start")?;
                        let (ready, condition) = &*write_gate;
                        *ready.lock().map_err(|_| "write gate poisoned")? = true;
                        condition.notify_one();
                    }
                    if next == CHUNK_COUNT {
                        return Ok(None);
                    }
                    let chunk = TrackedChunk::new(next, Arc::clone(&live), &maximum);
                    next += 1;
                    Ok(Some(chunk))
                }
            },
            {
                let overlapped = Arc::clone(&overlapped);
                let write_gate = Arc::clone(&write_gate);
                let written = Arc::clone(&written);
                move |chunk: TrackedChunk| {
                    if chunk.index == 0 {
                        writer_started_tx.send(()).map_err(|_| "reader stopped")?;
                        let (ready, condition) = &*write_gate;
                        let mut guard = ready.lock().map_err(|_| "write gate poisoned")?;
                        if !*guard {
                            (guard, _) = condition
                                .wait_timeout(guard, Duration::from_millis(250))
                                .map_err(|_| "write gate poisoned")?;
                        }
                        overlapped.store(*guard, Ordering::SeqCst);
                    }
                    thread::sleep(Duration::from_millis(5));
                    written.fetch_add(1, Ordering::SeqCst);
                    Ok(chunk.index)
                }
            },
            {
                let written = Arc::clone(&written);
                move |stored_count, _| {
                    assert!(
                        *stored_count < written.load(Ordering::SeqCst),
                        "storage acknowledgement ran before the write completed"
                    );
                    *stored_count += 1;
                    Ok(())
                }
            },
            |_| false,
        );

        assert!(
            outcome.result.is_ok(),
            "pipeline failed: {:?}",
            outcome.result
        );
        assert_eq!(stored_count, CHUNK_COUNT);
        assert!(
            overlapped.load(Ordering::SeqCst),
            "the next USB read did not run while storage was blocked"
        );
        assert!(
            maximum.load(Ordering::SeqCst) <= SYNC_PIPELINE_QUEUE_DEPTH + 2,
            "payload ownership exceeded the queue, active writer, and producer slots"
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn bounded_pipeline_stops_at_storage_error_and_discards_queued_payloads() {
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let mut next = 0_usize;
        let mut stored = Vec::new();
        let outcome: PipelineOutcome<usize, &'static str> = run_bounded_pipeline(
            &mut stored,
            |_| {
                if next == 12 {
                    Ok(None)
                } else {
                    let item = next;
                    next += 1;
                    Ok(Some(item))
                }
            },
            {
                let attempted = Arc::clone(&attempted);
                move |item| {
                    attempted.lock().unwrap().push(item);
                    if item == 2 {
                        Err("simulated storage failure")
                    } else {
                        Ok(item)
                    }
                }
            },
            |stored, item| {
                stored.push(item);
                Ok(())
            },
            |_| false,
        );

        assert!(matches!(
            outcome.result,
            Err(PipelineFailure::Stage("simulated storage failure"))
        ));
        assert_eq!(outcome.last_written, Some(1));
        assert_eq!(*attempted.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(stored, vec![0, 1]);
    }
}
