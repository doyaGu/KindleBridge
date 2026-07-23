# KindleBridge

KindleBridge provides a persistent development link between a host and a Kindle. Its language distinguishes the logical work carried over KBP from the transport that carries it.

## Language

**Sync Stream**:
A single `sync.v1` KBP stream that owns exactly one push or pull from its opening metadata through its terminal reply and close or reset.
_Avoid_: Sync connection, sync session

**Host Sync Client**:
The host-side owner of Sync Stream operations and their local-file hashing, staging, resume, cancellation, and durability rules for one connected Kindle.
_Avoid_: Sync manager, sync session

## Example dialogue

> Developer: Does reconnecting USB resume the same Sync Stream?
>
> Domain expert: No. The interrupted Sync Stream ends; the Host Sync Client opens a new Sync Stream that resumes the same transfer from its persisted offset.
