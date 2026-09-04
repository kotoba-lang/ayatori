# ayatori

[![CI](https://github.com/kotoba-lang/ayatori/actions/workflows/ci.yml/badge.svg)](https://github.com/kotoba-lang/ayatori/actions/workflows/ci.yml)

**The query plane woven from IPLD graphs and IPNI provider discovery.**

Ayatori bridges [`kotobase`](https://github.com/kotoba-lang/kotobase)'s flat document
store to real cross-record Datalog queries via
[`arrangement`](https://github.com/kotoba-lang/arrangement) — built as a shared
prerequisite (ADR-2607172300 in `com-junkawasaki/root`) for four sibling repos
(`datomic-client-shim`, `org-postgresql-wire`, `org-opencypher-cypher`,
`org-w3-sparql-protocol`) that each need real queries over kotobase-backed
data and should not each reimplement this bridge.

The name reflects the boundary: IPLD owns immutable values, links, traversal,
and verification; IPNI tells Ayatori where a CID may be available; Ayatori
weaves those inputs into a queryable projection. These are separate effects:
an IPNI provider result is not proof that content was retrieved or CID-verified,
and neither is proof that a query was evaluated.

Current implemented surfaces:

- `ayatori.discovery` — injected-transport IPNI/Delegated Routing provider lookup;
- `ayatori.remote` — verified provider block reads and range-pruned queries over
  persistent arrangement snapshots;
- `ayatori.query` — materialization, visibility-carrying access paths, and Datalog;
- `ayatori.agent` — pure query generation and validation helpers.

`kotobase.query.bridge` and `kotobase.query.agent` remain compatibility
namespaces. New callers should use `ayatori.*`.

## `ayatori.remote` — IPNI → verified IPLD → persistent query

`ayatori.remote/open-snapshot` connects the distributed read path without
turning any one step into evidence for the next one:

```clojure
(require '[ayatori.remote :as remote])

(def opened
  (remote/open-snapshot
   {:snapshot-cid arrangement-snapshot-cid
    ;; Or inject :discover-fn directly. http-fn is the IPNI JSON transport.
    :http-fn ipni-http
    ;; Turns advertised HTTPS multiaddrs into trustless-gateway block GETs.
    ;; A custom (fn [provider cid] -> bytes) may be injected instead.
    :fetch-fn (remote/gateway-fetcher block-http)
    :blind-fn blind
    :decrypt-fn decrypt
    ;; The same declared range partitions used by arrangement.core/commit!.
    :partitions [{:attr "score"
                  :boundaries [25 50 75]
                  :budget-bits 2}]}))

(remote/q opened
          '{:find [?entity ?score]
            :where [[?entity "score" ?score]
                    [(>= ?score 25)]
                    [(< ?score 50)]]}
          visible?)

(remote/stats opened)
;; {:discoveries ... :fetch-attempts ... :verified-blocks ...
;;  :verified-bytes ... :memo-hits ... :in-flight-hits ...}
```

The snapshot and every Prolly-tree block are rehashed before use. A bad or
missing provider is skipped, but unverified bytes are never returned or
memoised. `arrangement.source/cursor` then reads only the covering-index
prefixes named by the Datalog clauses. A declared range additionally uses the
persistent range tree and reports whether the read was actually pruned, plus
the declared disclosure budget, through `remote/scan-range-report`.

The synchronous API remains available for JVM services. Workers use the
explicit Promise surface: `open-snapshot-async`, `q-async`, `scan-async`, and
`scan-range-report-async`. Its discovery, provider GET, snapshot opening,
blinding, decryption, and prolly-tree descent all remain async; tree children
are fetched with bounded concurrency instead of a synchronous block-miss
trampoline that retries from the root. `q-async` carries the full Datalog
surface—joins, safe negation, disjunction, recursive rules, aggregates,
persisted range cuts, ordering, and limits—across that cursor without
materializing the snapshot. Promise propagation stays explicit; `remote/q`
remains synchronous.

Concurrent scans often descend through the same upper tree blocks. The async
getter therefore shares one in-flight Promise per CID: callers await the same
verified retrieval, failed flights are removed before retry, and only resolved
verified bytes enter the memo. `:in-flight-hits` makes that coalescing visible.

CID verification now preserves the codec declared by the requested CID and
checks its SHA-256 multihash. Persistent Arrangement snapshots remain
DAG-CBOR, while the transport/verification seam can also validate real raw or
DAG-PB provider responses before a higher layer decides how to decode them.

## Public network benchmark

`bin/network_bench.cljs` runs cold end-to-end samples against public
`cid.contact` discovery and Kotobase's advertised HTTP provider. It uses the
original raw CID from the advertisement corpus: IPNI indexes multihashes, so
constructing a DAG-CBOR CID with the same digest would discover the same
provider but name a different object. It also places a bounded timeout or
corrupt response before the real provider to measure failover, and probes that
codec-alias case to ensure it fails closed.

The 2026-08-27 Apple M4 run is recorded in
`bench/results/2026-08-27-public-ipni-provider.edn`: 20/20 normal reads of a
78,054-byte Kotobase block succeeded at p50 538.6 ms / p95 1295.1 ms;
timeout-first fallback was 10/10 at p50 289.3 ms / p95 790.0 ms;
corrupt-first fallback was 10/10 at p50 483.2 ms / p95 1205.0 ms. A DAG-CBOR
CID sharing an advertised raw block's multihash failed closed 5/5, as required:
discovery is multihash-based, while CID verification preserves the codec.
These are one-host public-network observations, not production Arrangement
snapshot or multi-block Datalog latency claims.

### Public persistent Arrangement snapshot

`bin/build_public_snapshot.cljs` deterministically builds the multi-block
fixture without network effects. `bin/public_arrangement_bench.cljs` then
opens its published snapshot cold and makes every block cross the full public
path: cid.contact discovery, the returned Kotobase provider, SHA-256/CID
verification, and the Worker-native Promise cursor. Kotobase's canonical IPLD
route is declared as `/ipld/:cid`; it is not silently confused with the legacy
`/ipfs` namespace.

The Apple M4 run in
`bench/results/2026-08-27-public-arrangement.json` used 24,000 quads in 324
blocks (6,577,346 bytes). A declared `score` range returning 10 rows succeeded
10/10 at p50 866.1 ms / p95 1273.1 ms and verified 7 blocks / 135,577 bytes.
The four-row rare pattern succeeded 10/10 at p50 536.2 ms / p95 772.9 ms and
verified 3 blocks / 17,234 bytes. A full 12,000-row score scan succeeded 3/3 at
p50 2102.8 ms / p95 3996.3 ms and verified 57 blocks / 916,304 bytes. Thus the
persisted range cut reduced this observed full-score read by 8.1x in blocks
and 6.8x in bytes.

These are cold, one-host public-network cursor measurements, not a production
SLA. They predate the Worker-native Datalog `q-async` landing and therefore
measure cursor patterns and range pruning, not multi-clause join latency.

The follow-up live run in
`bench/results/2026-08-27-public-q-async.edn` executes a real four-clause
`q-async`: persisted `score` range 5010–5020 joined by subject to
`kind = common`. It succeeded 10/10 with 10 rows at p50 3356.5 ms / p95
5004.5 ms, verifying 9 blocks / 181,873 bytes per cold sample with 18 memo
hits. This is likewise a one-host public-network observation, not an SLA.

The bounded-parallel follow-up in
`bench/results/2026-08-27-public-q-async-parallel.edn` also succeeded 10/10
with 10 rows. It kept the read at 9 blocks / 181,873 bytes by coalescing 14
in-flight CID reads per sample. Across separate cold runs, p50 was 3666.6 ms
(9.2% slower) and p95 was 4454.4 ms (11.0% faster) than the sequential
observation. Network variation prevents treating that as a universal speedup;
the durable result is bounded fan-out without duplicate verified bytes.

## `ayatori.agent` — the entry an LLM writes through

`bridge` is how a query reaches the datom plane. `agent` is how a query gets
written and refused before it gets there.

```clojure
(require '[ayatori.agent :as agent])

(def schema {:datasets ["market-intel"]
             :attributes [{:attr "company/lei" :doc "LEI, the join key"}
                          {:attr "company/ticker"}]})

(agent/system-prompt schema)          ;; rules + attribute table + examples
(agent/user-turn "list every ticker")
(agent/validate q schema)             ;; nil, or a map to hand back to the model
(agent/repair-turn refusal)           ;; the turn that closed the gap
(agent/extract-query model-output)    ;; nil when there was no query
```

### The dialect is written as a vector; this engine takes only a map

```clojure
(agent/->engine-query '[:find ?t :where [?e "company/ticker" ?t]])
;; => {:find [?t] :where [[?e "company/ticker" ?t]]}
```

Measured 2026-08-18, and the reason both that function and a guard in `bridge/q`
exist:

```
map     {:find [?t] :where [[?e "company/ticker" ?t]]}  =>  #{["AAA"] ["BBB"]}
vector  [:find ?t :where [?e "company/ticker" ?t]]      =>  #{[]}
```

The vector form — what the dialect is written in, what the prompt teaches, what
`validate` approves — is **not refused** by `arrangement.datalog`. `(:find
<vector>)` is nil, so it runs an empty query and answers with one empty tuple,
which no caller can tell from a query that matched nothing. `bridge/q` now
throws on a non-map query instead of passing it through.

**Pure, and depends on nothing** — not even on `bridge`. The caller owns the
model call and the execution. That is what lets `run-tests-pure.cljs` gate it
without the six-repo classpath the rest of the suite needs.

Measured on 2026-08-18 (ADR-2608189300 in `com-junkawasaki/root`), twenty
questions over two real datasets, graded by executing the generated query:
**45.5% bare → 88.9% with schema, examples, `validate` and up to two
`repair-turn`s.** All five bare failures were fabricated attribute names, and
`validate` caught every one without executing anything.

It refuses **shape, not meaning**: fabricated attributes, keyword attributes,
and predicates written as data patterns (`[>= ?r 1e11]`, `[?ni < 0]` — both
came out of that measurement, and both read as semantic errors while being
syntax). It does not know whether a query answers the question.

## The problem this solves

`kotobase.store`/`kotobase.local` (the `IStore` seam every kotobase-backed
app already uses) is a flat get/put/list(keys-in-one-collection)/append
store — no joins, no predicates across collections, no query language.
`kotoba-lang/arrangement` is a 4-covering triple index (`spo`/`pso`/`pos`/
`ocp` ≡ Datomic's EAVT/AEVT/AVET/VAET) with a real Datomic-shaped Datalog
engine (`arrangement.datalog/q`, `:find`/`:where`, joins, negation,
aggregates, recursive rules) over it — but nothing wired `IStore` documents
INTO that index. This repo is that wiring: a thin bridge, not a
reimplementation. `arrangement.datalog` does all the actual query
evaluation; this repo only materializes `IStore` docs as datoms and hands
them to it.

## Memoising it — `materialize-memo` (ADR-2607310900)

The linear scan below is still what `materialize` does. What changed is that a
caller holding a **content address** for the store's state no longer has to pay
for it twice:

```clojure
(def m (bridge/memo))                                   ; caller owns it
(bridge/materialize-memo m store ["users"] chain-cid)   ; version is REQUIRED
(bridge/memo-stats m)                                   ; {:size :capacity :hits :misses}
```

This is a **memo, not a cache**: the key is a content address, so a changed
graph is a *different key* rather than a dirty entry, and there is no
invalidation path to get wrong.

Two properties make sharing one db across callers safe, and both belong to
`materialize` rather than to the memo:

1. **`materialize` takes no `visible?`.** The predicate is applied by `q`, over
   an already-built db — so one db is correct for every caller regardless of
   what any of them may see. There is a test asserting `materialize`'s arity
   never grows, because if it did, this stops being sound.
2. **The db is a value**, so nothing handed out can be mutated into something
   the next caller sees.

**The answer is never memoised** — only the index. A result memo would need
`visible?` in its key, and keying on a function is how one principal ends up
reading another's rows.

`version` is required and must not be nil: a caller with no content address has
nothing that makes a cached db provably current, and defaulting it would
produce a cache that never invalidates. A revision *counter* is not
automatically sufficient either — a counter is unique only along one line of
writes, and a chain can fork.

## ⚠️ Legacy `IStore` limitation: linear materialization

`materialize` does a **full `-list` + `-get` scan of every requested
collection, on every call**, building a throwaway in-memory `arrangement`
db. This is still the compatibility path for flat document stores. There is:

- **no incremental indexing** — every `materialize` call redoes the full scan,
  unless the caller uses the CID-keyed `materialize-memo`/`db-for` seam;
- **no answer caching** — the memo retains a CID-keyed snapshot projection,
  never a principal-specific query result;
- **no persistence** — `arrangement.core/commit!` (the CID-addressed
  snapshot machinery) is never called by this bridge; materialized data
  lives only in memory for the duration of one `materialize`/`q` call.

`ayatori.remote` does not take this path: it reads an already-persistent
arrangement snapshot by CID and follows only the required index ranges.

This is an accepted, explicitly-documented v0.1 scope decision
(ADR-2607172300), not an oversight: fine for the small/test-scale query
volume this bridge is built for right now. Real incremental indexing via
`arrangement`'s own index-maintenance primitives is a natural follow-up
once there is real query-volume evidence — not a v0.1 requirement.

## Doc → datoms mapping

For a document `doc` at `(kotobase.store/-get store coll k)`:

- the **entity** (`:s`) is `(keyword (str coll) (str k))` — e.g. collection
  `"users"` key `"u1"` becomes the entity `:users/u1`. Globally unique
  across every materialized collection.
- every entity always gets two synthetic attributes: **`:kotobase/coll`**
  (`(str coll)`) and **`:kotobase/key`** (`(str k)`) — these carry the
  original collection/key identity through materialization and are the
  join handle a doc in another collection uses to reference this one (see
  the worked example below).
- if `doc` is a map: every top-level `[attr v]` pair becomes a datom
  `{:s entity :p attr :o v}`. `attr` is used **as-is** (whatever key shape
  the document used — typically a keyword). If `v` is itself a non-map
  collection (vector/set/list), it is treated as a **Datomic-style
  cardinality-many attribute**: one datom per element, not one datom whose
  `:o` is the whole collection.
- if `doc` is not a map (a bare scalar, or `nil`): one fallback datom
  `{:s entity :p :kotobase/value :o doc}`.
- **nested maps as attribute values are stored as an opaque `:o` value**,
  not recursively flattened into their own entity — out of scope for v0.1,
  a documented limitation, not silently mishandled.

## API

```clojure
(require '[kotobase.local :as local]
         '[kotobase.store :as st]
         '[ayatori.query :as bridge])

;; materialize: IStore + collection keys -> one combined arrangement db
(bridge/materialize store coll-keys)

;; entity-id: the [coll k] -> entity convention materialize uses
(bridge/entity-id "users" "u1")            ;=> :users/u1

;; access paths: the four covering indexes, each carrying visible?
(bridge/entity-attrs db s visible?)        ; spo / EAVT  -> {p #{o ...}}
(bridge/by-predicate db p visible?)        ; pso / AEVT  -> {s #{o ...}}
(bridge/by-predicate-value db p o visible?); pos / AVET  -> #{s ...}
(bridge/refs-to db o visible?)             ; ocp / VAET  -> {p #{s ...}}
(bridge/datoms db visible?)                ; the whole plane, lazily -> ({:s :p :o} ...)

;; q: run a Datomic-shaped :find/:where query over an already-materialized db
;; visible? is REQUIRED (see below) — arity 3 or arity 4 with :in inputs
(bridge/q db query visible?)
(bridge/q db query visible? inputs)

;; query: one-shot convenience — materialize + q in a single call
(bridge/query store coll-keys query visible?)
(bridge/query store coll-keys query visible? inputs)
```

`visible?` is **required, not defaulted**, on every query fn here — the
same discipline `arrangement.query`/`arrangement.datalog`/the retired `kqe`
already enforce (ADR-2607050500, "Query as first-class effect" in
`com-junkawasaki/root` — no permissive default to silently fall back on).
Pass `(constantly true)` to see everything materialized; that is a
caller's explicit choice, never this bridge's default.

### The supported contract is `materialize` + the access paths, not `q`

**ADR-2608039970** (`com-junkawasaki/root`). Datalog is *one frontend* over
the materialized datom plane, not the IR every other query language has to
be translated into. What the surfaces share is the plane (triple + content
addressing); `q`'s `:find`/`:where` grammar is not part of that.

This is not a prediction — it is already how the surfaces are built.
[`org-w3-sparql-protocol`](https://github.com/kotoba-lang/org-w3-sparql-protocol)
uses `materialize` and never calls `q`, and says why in its own README: it
has a complete SPARQL algebra already, so routing SPARQL through Datalog
would translate one algebra into another and back for no benefit. **That
repo is on the supported path, not off it.** The same applies to any surface
whose language does not sit on Datalog's set semantics — SQL's bag semantics
and `ORDER BY`, Cypher's variable-length paths.

| Use | When |
|---|---|
| `materialize` / `materialize-memo` / `db-for` | Always. Every surface goes through the shared plane. |
| `entity-attrs` / `by-predicate` / `by-predicate-value` / `refs-to` | Your language has its own algebra — plan joins over the four indexes yourself. |
| `datoms` | Your engine wants the whole plane once, in its own shape (`org-w3-sparql-protocol` turns every triple into an RDF quad). |
| `q` / `query` | Your language *is* Datalog-shaped (`datomic-client-shim`). |

What a surface must **not** do is give up on the datom plane and keep its
own physical representation of the same documents. Sharing the plane and
sharing the language are different decisions, and ADR-2608039970 makes
opposite calls on them.

**Why re-export rather than point at `arrangement.core`?** Those four exist
there already (they are `datalog.index`'s), but they take no `visible?` —
they are raw index reads. Re-exporting them here is what puts the
ADR-2607050500 discipline on the supported path. Reaching past this
namespace into `arrangement.core/entity-attrs` is not faster, it is a read
with no visibility decision in it. This repo's own tests did exactly that
before ADR-2608039970, which is how it was noticed.

The access paths also **prune**: an attribute whose every value is invisible
is dropped from the returned map rather than returned as an empty set. An
empty set under a key answers *"this exists, but you may see none of it"* —
more than the caller is allowed to know.

**`refs-to` returns `{}` on a typical materialized db, and that is a
property of the data, not a stub.** `:vaet` covers only objects satisfying
`materialize`'s `ref?` (`ipld.core/link?`), and documents carry plain EDN.
A foreign key here is a *value* (`:dept-key "d1"`), so the reverse lookup
you want is `(bridge/by-predicate-value db :dept-key "d1" visible?)`.
Making `ref?` injectable is a real follow-up and not free: `materialize-memo`'s
key would have to cover it, and a function is not a memo key — a declarative
`:ref-attrs #{...}` set would be.

### Worked example (equality filter + cross-collection join)

```clojure
(def store (local/local-store))
(st/-put store "users" "u1" {:name "Alice" :role "admin" :dept-key "d1"})
(st/-put store "users" "u2" {:name "Bob" :role "user" :dept-key "d2"})
(st/-put store "departments" "d1" {:name "Engineering"})
(st/-put store "departments" "d2" {:name "Sales"})

(bridge/query store ["users" "departments"]
              '{:find [?uname ?dname]
                :where [[?u :role "admin"]
                        [?u :name ?uname]
                        [?u :dept-key ?dk]
                        [?d :kotobase/key ?dk]
                        [?d :name ?dname]]}
              (constantly true))
;=> #{["Alice" "Engineering"]}
```

`?u`'s `:dept-key` value ("d1") joins against `?d`'s `:kotobase/key`
attribute — a real cross-collection join, evaluated by
`arrangement.datalog`'s nested-loop join, not reimplemented here.

## A note on `kqe`

ADR-2607172300's dependency table names `kotoba-lang/kqe` alongside
`kotoba-lang/arrangement`. As of this repo's landing, **`kqe` is retired
upstream** — its content (`[s p o]` pattern routing) was merged into
`arrangement` as `arrangement.query`, and the Datomic-shaped `:find`/`:where`
Datalog engine used here (`arrangement.datalog/q`) lives in `arrangement`
and never existed in `kqe` at all. This repo therefore depends on
`kotoba-lang/arrangement` only — see `kqe`'s README ("this repo is
retired") and `arrangement`'s own README / ADR-2607050700 for the merge.

## Dependencies

- [`kotoba-lang/kotobase`](https://github.com/kotoba-lang/kotobase) —
  `kotobase.store`/`kotobase.local` (`IStore`, `LocalStore`), the seam this
  bridge reads from.
- [`kotoba-lang/arrangement`](https://github.com/kotoba-lang/arrangement) —
  the persistent 4-covering index, prefix/range cursor, and Datalog engine.
  `ayatori.remote` reads its CID-addressed snapshots directly.
- [`kotoba-lang/io-ipld`](https://github.com/kotoba-lang/io-ipld) —
  CID verification for every block returned by a provider. It pulls in
  `org-ietf-cbor` and `dev-protobuf`.
- [`kotoba-lang/io-multiformats`](https://github.com/kotoba-lang/io-multiformats) —
  strict parsing of advertised HTTP-gateway multiaddrs. `deps.edn`'s direct
  git dependencies resolve the complete chain automatically for
  the JVM `:test` alias via `tools.deps`; the nbb primary test path has no
  dependency resolver, so `bin/run_tests.cljs`/CI clone every transitive
  dep by hand — see Develop/test below.
- [`kotoba-lang/io-ipni-specs`](https://github.com/kotoba-lang/io-ipni-specs) —
  provider discovery through Delegated Routing V1 and IPNI-native `/cid`.
  Transport and JSON parsing are caller-injected; discovery never rewrites
  the content CID and does not imply successful IPLD retrieval.
## Develop / test

First-class runtime is **nbb/cljs** (repo-wide runtime priority):

```bash
git clone https://github.com/kotoba-lang/kotobase .deps/kotobase
git clone https://github.com/kotoba-lang/arrangement .deps/arrangement
git clone https://github.com/kotoba-lang/prolly-tree .deps/prolly-tree
git clone https://github.com/kotoba-lang/io-ipld .deps/io-ipld
git clone https://github.com/kotoba-lang/io-multiformats .deps/io-multiformats
git clone https://github.com/kotoba-lang/org-ietf-cbor .deps/org-ietf-cbor
git clone https://github.com/kotoba-lang/dev-protobuf .deps/dev-protobuf
git clone https://github.com/kotoba-lang/datom-source .deps/datom-source
git clone https://github.com/kotoba-lang/datalog .deps/datalog
git clone https://github.com/kotoba-lang/io-ipni-specs .deps/io-ipni-specs
git clone https://github.com/kotoba-lang/org-nist-sha2 .deps/org-nist-sha2
nbb --classpath "src:test:.deps/kotobase/src:.deps/arrangement/src:.deps/prolly-tree/src:.deps/io-ipld/src:.deps/io-multiformats/src:.deps/org-ietf-cbor/src:.deps/dev-protobuf/src:.deps/datom-source/src:.deps/datalog/src:.deps/io-ipni-specs/src:.deps/org-nist-sha2/src" bin/run_tests.cljs
```

Each `.deps/<name>` should be checked out at the SHA pinned in `deps.edn`
(`kotobase`, `arrangement`, `io-ipld`, `io-multiformats`, `io-ipni-specs`) or in the dependency
repos' own `deps.edn` transitively (`prolly-tree`,
`org-ietf-cbor`, `dev-protobuf`, `datom-source`, `datalog`, and `org-nist-sha2`
— which io-multiformats requires as `sha2.core`, and whose absence made every
documented reproduction of this suite die before the first test until
2026-09-04) — CI pins every one of them, see
`.github/workflows/ci.yml`.

The `:test` alias in `deps.edn` is the JVM **compat** suite only (`clojure
-M:test`, via `tools.deps` transitive git-dep resolution — no manual
`.deps/` cloning needed for this path) — not the primary execution path.

## License

Apache-2.0
