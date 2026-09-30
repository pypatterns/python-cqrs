Protobuf assets for outbox / Kafka examples.

| File | When to open |
|------|----------------|
| `user_joined.proto` | Source schema for UserJoined events |
| `user_joined_pb2.py` | Generated Python stubs (do not edit by hand) |

Used by documentation that shows protobuf payloads with the transactional outbox.

Runnable produce + consume (isolated `OutboxedEventMap`, no Kafka):

```bash
pip install -e ".[examples]"
python examples/outbox/protobuf_outbox.py
```
