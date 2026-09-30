Transactional outbox examples.

| File | When to open |
|------|----------------|
| `save_events_into_outbox.py` | Persist notification/ECST events in the outbox |
| `kafka_outboxed_event_producing.py` | Drain outbox → Kafka broker |
| `fastapi_outbox.py` | FastAPI + one-tx write + publisher stub (needs examples extra) |
| `protobuf_outbox.py` | Opt-in Protobuf produce + consume (isolated map, no Kafka) |

Docs: https://mkdocs.python-cqrs.dev/outbox/
