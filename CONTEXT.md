# KindleBridge

KindleBridge provides a persistent development link between a host and a Kindle. Its language distinguishes the logical work carried over KBP from the transport that carries it.

## Language

**Sync Stream**:
A single `sync.v1` KBP stream that owns exactly one push or pull from its opening metadata through its terminal reply and close or reset.
_Avoid_: Sync connection, sync session

**Host Sync Client**:
The host-side owner of Sync Stream operations and their local-file hashing, staging, resume, cancellation, and durability rules for one connected Kindle.
_Avoid_: Sync manager, sync session

**Shell Stream**:
A single `shell.v2` KBP stream that owns exactly one shell process from its opening metadata through exit and close or reset. A disconnected Shell Stream is destroyed and cannot be resumed.
_Avoid_: Shell connection, resumable shell session

## Example dialogue

> Developer: Does reconnecting USB resume the same Sync Stream?
>
> Domain expert: No. The interrupted Sync Stream ends; the Host Sync Client opens a new Sync Stream that resumes the same transfer from its persisted offset.
>
> Developer: Does the same apply to an interactive shell?
>
> Domain expert: A Shell Stream is not resumable. Disconnecting it terminates its shell process; the host must open a new Shell Stream.
