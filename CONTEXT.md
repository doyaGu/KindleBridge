# KindleBridge

KindleBridge provides a persistent development link between a host and a Kindle. Its language distinguishes the logical work carried over KBP from the transport that carries it.

## Language

**Host RPC**:
A bounded typed request and reply carried over the current user's local JSON-RPC link. It performs one host control or device-provider operation and owns no Shell Stream or sync job after its reply.
_Avoid_: Device RPC, host stream

**Device RPC**:
A bounded typed request and reply carried over one `rpc.v1` KBP stream. It performs one device operation and owns no work after its reply stream closes.
_Avoid_: Generic device command, streaming operation

**Sync Stream**:
A single `sync.v1` KBP stream that owns exactly one push or pull from its opening metadata through its terminal reply and close or reset.
_Avoid_: Sync connection, sync session

**Host Sync Client**:
The host-side owner of Sync Stream operations and their local-file hashing, staging, resume, cancellation, and durability rules for one connected Kindle.
_Avoid_: Sync manager, sync session

**Host Sync Operation**:
A host-server push or pull opened by one streaming request on the local JSON-RPC link. It owns its operation ID, progress, and cancellation through the terminal response, and may drive at most one Sync Stream. Losing the local client cancels it.
_Avoid_: Sync job, Sync Stream

**Logical Sync Path**:
A non-empty, Unicode NFC, forward-slash relative path naming one location below the KindleBridge sync root. Host absolute-root aliases and backslashes are developer input forms, not Logical Sync Paths.
_Avoid_: Remote filesystem path, host input path

**Sync Tree**:
A bounded hierarchy rooted at one Logical Sync Path. The Host preflights the whole hierarchy, then realizes it with ordered directory operations and one Sync Stream per file. A pulled Sync Tree is accepted only when its final names, kinds, and sizes still match the preflight manifest.
_Avoid_: Recursive Sync Stream, atomic directory snapshot

**Shell Stream**:
A single `shell.v2` KBP stream that owns exactly one shell process from its opening metadata through exit and close or reset. A disconnected Shell Stream is destroyed and cannot be resumed.
_Avoid_: Shell connection, resumable shell session

## Example dialogue

> Developer: Is a file push a Device RPC?
>
> Domain expert: No. Opening and transferring the file is a Sync Stream. Bounded operations such as sync status, directory list, and directory creation are Device RPCs.
>
> Developer: Is `v1.shell.open` a Host RPC because it uses JSON-RPC?
>
> Domain expert: No. Its reply hands ownership to the local Shell Stream lifecycle, so it is a stream-opening request. A Host RPC owns no work after its reply.
>
> Developer: Does every Host RPC become a Device RPC?
>
> Domain expert: No. Server status and stop are local host controls, while a legacy bounded sync call can drive a Sync Stream. Host RPC describes the local call's ownership, not the device-side protocol it selects.
>
> Developer: Is a Host Sync Operation the same thing as its Sync Stream?
>
> Domain expert: No. The Host Sync Operation owns local progress, cancellation, and the terminal response. It may open one Sync Stream, while a fake device can complete it without opening any device stream.
>
> Developer: Does a Host Sync Operation continue after its local client disconnects?
>
> Domain expert: No. Disconnecting the local client cancels every Host Sync Operation it owns.
>
> Developer: Does reconnecting USB resume the same Sync Stream?
>
> Domain expert: No. The interrupted Sync Stream ends; the Host Sync Client opens a new Sync Stream that resumes the same transfer from its persisted offset.
>
> Developer: Is `/mnt/us/kindlebridge-data/books/a.epub` a Logical Sync Path?
>
> Domain expert: No. The host input Adapter converts that alias to the Logical Sync Path `books/a.epub` before opening a Sync Stream.
>
> Developer: May a directory push create valid earlier entries before discovering an invalid later Logical Sync Path?
>
> Domain expert: No. The Host Sync Client validates the complete local tree before the first device mutation, so a path failure leaves no partial remote tree.
>
> Developer: What if two source paths differ only by ASCII letter case?
>
> Domain expert: The complete-tree validation rejects the push before mutation and reports both colliding paths.
>
> Developer: May a recursive pull begin writing locally while it is still discovering the device tree?
>
> Domain expert: No. It first builds and validates a complete manifest. Each file is verified independently with BLAKE3, and the Host Sync Client compares names, kinds, and sizes with a final manifest before accepting the result. A mismatch removes the partial result and asks the developer to retry. This does not claim that every file came from one atomic device snapshot.
>
> Developer: Is a recursive directory push one Sync Stream?
>
> Domain expert: No. It is one Sync Tree realized through ordered directory operations and one independent Sync Stream for each file.
>
> Developer: Does the same apply to an interactive shell?
>
> Domain expert: A Shell Stream is not resumable. Disconnecting it terminates its shell process; the host must open a new Shell Stream.
