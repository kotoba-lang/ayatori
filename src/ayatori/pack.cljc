(ns ayatori.pack
  "CARv2 packs as a block source for Ayatori.

  Ayatori's existing block path fetches one block per request: discover
  providers for a CID, ask one for its bytes, rehash. That is correct and it
  is one network round trip per block, which is what makes hydrating a
  persistent arrangement over IPLD expensive -- the novelty chain is a cons
  chain of width 1, so the round trips are strictly sequential and cannot be
  prefetched away (ADR-2608021000).

  A CARv2 pack removes the network's part of that. Blocks written together
  live together in one archive, so one HTTP Range read can serve a block that
  would otherwise have been its own request, and a commit packed as a unit
  becomes one open plus one read per block instead of one lookup plus one
  fetch per block. The chain stays logically sequential; only the transport
  stops being.

  ## The layout has to be declared, and there is no default

  ADR-2608160100 separates a block's identity (its CID) from its location
  (which pack, at what offset, for how many bytes) and requires a backend to
  say which of the two layouts it has:

      :block-per-object   one CID, one object. The existing path.
      :packed-blocks      many CIDs inside one object, addressed by range.

  `:packed-blocks` is refused unless the object plane also declares
  `:range-read`, and that refusal is the point of this namespace rather than
  a detail of it. Without Range, a `:packed-blocks` reader still WORKS: it
  GETs the whole archive and picks one frame out of it. Round trips go down,
  every test passes, and transfer explodes -- a success that is a failure.
  Nothing downstream can tell the difference, so it has to be refused here,
  at the only place that knows both halves.

  There is deliberately no default layout. A guess is silently wrong on a
  store that cannot serve ranges.

  ## What this namespace does not do

  It does not write CARv2 -- `kotoba-lang/io-ipld-car` owns the codec, and a
  second encoder is how offset conventions drift (`:file-offset` versus the
  index's `:payload-offset` is the one bug this format reliably produces).
  It does not decide which blocks belong in a pack; that is a write-side
  policy (one commit, one pack). And it does not verify CIDs itself:
  `ipld.car.v2/read-frame` does, on every frame it parses, so a pack that
  names a block it does not contain is rejected by the reader rather than by
  a check this namespace could forget to run."
  (:require [ipld.car :as car]
            [ipld.car.bytes :as b]
            [ipld.car.index :as idx]
            [ipld.car.v2 :as car2]))

(def block-layouts
  "The two layouts a backend may declare. There is no third, and no default."
  #{:block-per-object :packed-blocks})

(def ^:const header-bytes
  "Pragma (11) + header (40). The first range read of any pack."
  51)

(def ^:const default-read-ahead
  "Bytes requested per block read.

  The CARv2 index stores where a frame starts and not how long it is, so a
  reader holding only the index must over-fetch and let `read-frame` use the
  frame it finds. 64 KiB is the string-leaf ceiling named in ADR-2608160100,
  so one read covers any single block that ceiling admits. Callers holding
  real `:frame-length` values from `pack` should pass them instead and fetch
  exactly the frame."
  65536)

(defn block-profile!
  "Validate a backend's declared block layout and return it.

  `profile` is `{:blocks <layout> :object #{capabilities}}`. Throws when
  `:blocks` is missing or unknown, and when `:packed-blocks` is declared
  without `:range-read` on the object plane."
  [{:keys [blocks object] :as profile}]
  (when-not (contains? block-layouts blocks)
    (throw (ex-info (str "ayatori: a backend must declare :blocks as one of "
                         (pr-str block-layouts) " -- there is no default")
                    {:type :ayatori/undeclared-block-layout
                     :blocks blocks
                     :profile profile})))
  (when (and (= :packed-blocks blocks)
             (not (contains? (set object) :range-read)))
    (throw (ex-info (str "ayatori: :packed-blocks requires :range-read on the "
                         "object plane. Without it a packed reader fetches the "
                         "whole archive per block: fewer round trips, and "
                         "transfer proportional to the pack. That reads as a "
                         "success, so it is refused here.")
                    {:type :ayatori/packed-blocks-without-range-read
                     :object (set object)
                     :profile profile})))
  profile)

(defn- bounded-index
  "The index records with an index-derived read bound attached to each.

  `ipld.car.v2/locate` returns only `:file-offset`, and says why: the CARv2
  index stores where a frame starts and not how long it is, so a reader
  either reads to the next record or over-fetches. Over-fetching is what the
  read-ahead ceiling does, and it is expensive -- measured 2026-09-04, a
  packed query moved 394 KB where the per-object path moved 110 KB, because
  every block was requested as 64 KiB regardless of its real size. Round
  trips went down and transfer went UP.

  Reading to the next distinct record needs no extra fetch. The bound is
  exact only with complete, contiguous frame coverage; sparse indexes can
  include additional sections in the read. The final bound uses :data-size.
  Duplicate offsets are not normalized here and can yield zero-length bounds.
  See docs/ipld-retrieval-contract.md for compatibility gates."
  [{:keys [data-offset data-size]} index]
  (let [sorted (vec (sort-by :payload-offset index))
        end (+ data-offset data-size)]
    (vec (map-indexed
          (fn [i r]
            (let [start (+ data-offset (:payload-offset r))
                  next-start (if-let [n (get sorted (inc i))]
                               (+ data-offset (:payload-offset n))
                               end)]
              (assoc r :file-offset start :frame-length (- next-start start))))
          sorted))))

(defn open-pack
  "Read a pack's header and index, and return a handle for block reads.

  `range-fn` is `(fn [{:keys [range]}] -> bytes)` where `:range` is an HTTP
  `Range` header value; the caller owns transport and the pack's address.
  `profile` is checked by `block-profile!` before any request is made.

  Costs two range reads regardless of how many blocks are later read out of
  the pack. `:reads` counts every range request this handle has issued, so a
  caller can assert round trips rather than assume them."
  [{:keys [range-fn profile read-ahead]}]
  (when-not (fn? range-fn)
    (throw (ex-info "ayatori: range-fn is required"
                    {:type :ayatori/missing-range-fn})))
  (block-profile! profile)
  (let [reads (atom 0)
        fetch (fn [range-value]
                (swap! reads inc)
                (or (range-fn {:range range-value})
                    (throw (ex-info "ayatori: range read returned no bytes"
                                    {:type :ayatori/empty-range-response
                                     :range range-value}))))
        head-bytes (fetch (str "bytes=0-" (dec header-bytes)))
        header (car2/parse-header head-bytes)
        index-offset (:index-offset header)]
    (when-not (pos? index-offset)
      (throw (ex-info (str "ayatori: this CARv2 carries no index (index-offset "
                           "0), so a block cannot be located without reading "
                           "the whole payload. Pack it with :index? true.")
                      {:type :ayatori/pack-without-index
                       :header header})))
    ;; `car2/read-index` re-parses the header and reads at the file-absolute
    ;; `index-offset`, which is not where the index sits in a range response.
    ;; The index bytes come back at offset 0 of their own body, so decode
    ;; them directly rather than reassembling a fake archive around them.
    (let [index (idx/decode (fetch (str "bytes=" index-offset "-")) 0)]
      {:header header
       :index index
       :bounded (bounded-index header index)
       :reads reads
       :read-ahead (or read-ahead default-read-ahead)
       :fetch fetch})))

(defn locate
  "Where `cid` starts in this pack and its read bound, or nil when the
  pack does not carry it.

  Unlike `ipld.car.v2/locate` this returns `:frame-length` too, derived from
  the neighbouring index records rather than fetched -- see `bounded-index`."
  [{:keys [header bounded]} cid]
  (let [{:keys [digest]} (car/read-cid (car/cid->bytes cid) 0)]
    (some (fn [r] (when (b/equal? digest (:digest r)) r)) bounded)))

(defn read-block
  "Read one block out of an opened pack with a single range request.

  Returns the block's bytes, or nil when the pack does not carry `cid` -- a
  pack that does not have a block is an answer, not an error, so a caller can
  fall back to the per-object path. `ipld.car.v2/read-frame` verifies the CID
  of whatever frame it parses, so bytes returned here have already been
  checked against the identity the index claimed."
  [{:keys [fetch read-ahead] :as pack} cid]
  (when-let [loc (locate pack cid)]
    (let [body (fetch (if-let [len (:frame-length loc)]
                        ;; Index neighbours bound the read; sparse indexes
                        ;; may include additional frames in this range.
                        (car2/range-header (assoc loc :frame-length len))
                        ;; a pack whose index gave no neighbour to bound
                        ;; against still works, by over-fetching
                        (car2/range-open loc read-ahead)))
          {frame-cid :cid frame-bytes :bytes} (car2/read-frame body 0)]
      (when-not (= (str cid) (str frame-cid))
        (throw (ex-info "ayatori: pack index pointed at a different block"
                        {:type :ayatori/pack-index-mismatch
                         :wanted (str cid) :found (str frame-cid)})))
      frame-bytes)))

(defn pack-fetcher
  "A `fetch-fn` for `ayatori.remote/provider-block-getter`, backed by a pack.

  Signature matches the per-object fetcher -- `(fn [provider cid] -> bytes)`
  -- so the verified block getter, its CID rehash and its provider fallback
  are unchanged. `provider` is ignored: a pack is already located, and
  discovery answers where a CID may be fetched, not which frame holds it."
  [pack]
  (fn pack-fetch [_provider cid]
    (read-block pack cid)))

(defn reads
  "How many range requests an opened pack has issued."
  [{:keys [reads]}]
  @reads)
