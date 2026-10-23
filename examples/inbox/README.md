# Transactional inbox examples

`process_deposits.py` is the path you can run without a broker. It opens a session, writes a batch of three deposits (two of them share a message id), commits, and closes. A worker loop then opens its own session and reads `get_many(batch_size=2)` until the batch is empty. The handler and the inbox repository share that session for one batch, and the loop commits it. The ledger ends with two rows: `acc-1` and `acc-2`. A second drain reads an empty batch because both rows are `processed`.

```bash
pip install -e ".[examples]"
python examples/inbox/process_deposits.py
```

That script is the subscriber's job and the worker's job in one process, so you can see both. In an application they are different processes. The subscriber receives an `InboxRepository` from a generator dependency that opens a session and commits it. The worker opens its own session.

| Piece | What it does |
| --- | --- |
| `fastapi_accept.py` | One HTTP request, one command or one event. `Depends(inbox_repository)` injects the repository. The route calls `add` with a list of one element and returns 202. The generator commits after the route returns. |
| `faststream_accept.py` | FastStream 0.5 `batch=True` collects a Kafka batch. `faststream.Depends(inbox_repository)` injects the repository. The subscriber calls `add` with that batch. FastStream acks after the generator commits. |
| `process_deposits.py` | Accept, then drain. The handler does not know about the inbox and does not commit. |
| `presentation_scope.py` | The extra case, not the default. The presentation must write the inbox row and another row in the same transaction, so `open_scope` and `bind_scope` keep both writes on one `AsyncSession`. |

`fastapi_accept.py` listens on `http://127.0.0.1:8000`. Repeat the same `Idempotency-Key` and the response is still 202; `add` returns `[False]` and does not insert another row.

`faststream_accept.py` needs a Kafka broker at `localhost:9092`. The subscriber does not call a mediator. A duplicate inside the batch is still acknowledged, because that delivery is already in the inbox. If `add` or the commit raises, FastStream does not ack and Kafka sends the batch again.

The worker that calls `get_many` and `InboxProcessor.process` is a separate process. It opens its own session for each batch, builds the repository from that session, and passes that repository to `InboxProcessor`. It does not join the session that accepted the message.
