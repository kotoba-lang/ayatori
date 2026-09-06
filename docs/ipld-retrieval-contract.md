# IPLD retrieval contract

Status: Proposed extension of the existing CARv2 reader, not a Selector-engine
implementation. See [Kotobase ADR-2609060000](https://github.com/kotoba-lang/kotobase/blob/178711d107154db5cf00a3bf7c24f79ed854f08a/docs/adr/2609060000-ipld-adl-selector-car-boundaries.md).

## Existing implementation

`ayatori.pack/open-pack` reads a CARv2 header and index through a caller-owned
range function. `read-block` delegates frame validation to `io-ipld-car` and
checks that the returned CID equals the requested CID. `pack-fetcher` adapts
this to `ayatori.remote/provider-block-getter`. Discovery locates providers;
the CAR index locates candidate frames. Neither establishes graph truth.

The current index-derived `:frame-length` is a read bound, not universally an
exact frame size. It is exact only when every frame boundary is represented
once and frames are contiguous. Sparse indexes (including omitted identity
sections) can over-fetch. Repeated offsets now share the next distinct boundary.
Header arithmetic and index offsets are checked before block reads. Candidate
lookup matches both hash code and digest; `read-block` tries distinct candidate
offsets until the full CID matches, preserving verification of each parsed frame.
Even an overlong provider response cannot extend a frame past its read bound.

This does not claim arbitrary-CAR support: the codec remains CIDv1 and
MultihashIndexSorted only, with its existing supported hash algorithms. Index
size/work limits and verification of CARv1 header structure during `open-pack`
remain separate work. Sparse reads remain bounds rather than exact lengths.

## Proposed selective hydration

Input: immutable root CID, standard Selector, explicitly supported ADL versions,
and budgets for bytes, blocks, depth, and traversal work. Output: independently
verified loaded blocks, selected values, and explicit completion/failure state.
CAR export consumes loaded traversal dependencies, including the root path and
ADL substrate blocks. Merely listing Matcher results is insufficient.

Replay traversal locally; reject missing dependencies and unsupported forms.
Deduplicate fetched bytes by CID while retaining traversal state by path and
Selector state. Reaching a work limit is incomplete retrieval, not success.
Selectors do not choose Datalog indexes or express ordered-map key intervals:
`ExploreRange` is a list-position interval. Initially, database range cursors
can record visited CIDs as a custom artifact with no standard-Selector claim.

CAR roots bind the starting graph only when checked against the caller's root.
Hash verification and Selector replay do not establish database-query completeness
or authorize access. Logical selection may load shared blocks containing other
rows. Apply authorization before serving bytes, and define authenticated range
proofs separately if needed.

## Transport and rollout

Retain `io-ipld-car` as the only encoder/parser. CARv1 is a streaming option;
indexed CARv2 is an object/cache option. Arbitrary Selector-over-HTTP needs a
custom negotiated protocol and must not be advertised as standard gateway
support merely because the response is a CAR.

Keep existing write-local packs. Evaluate traversal-order exports separately
with request-count, byte-count, memory, and latency evidence. Coalescing must
preserve per-frame full-CID verification. `pack-bounds-test` covers
sparse/duplicate indexes, hash-code candidate filtering, codec aliases, padded
payload/index offsets, truncated/corrupt frames, and payload-end bounds. These
tests do not add support for new hash algorithms. Selector work then
needs missing-block, shared-DAG, unsupported-ADL, and budget-exhaustion fixtures.

References: [CARv2](https://ipld.io/specs/transport/car/carv2/) and
[Selectors](https://ipld.io/specs/selectors/).
