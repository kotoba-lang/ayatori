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
  "Historical read-ahead default, retained in the pack handle for API
  compatibility. `read-block` now always uses validated index-derived bounds
  rather than a fixed-size read-ahead window."
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

(defn- safe-offset? [n]
  (and (integer? n) (<= 0 n b/max-safe-integer)))

(def ^:const max-index-bytes
  "Ceiling on the index this reader will fetch and hold, in bytes.

  The index is fetched before its size is known -- `index-offset` says where
  it starts and nothing says where it ends -- so without a ceiling the read
  is `bytes=N-`, whatever that turns out to be. 8 MiB indexes roughly 200k
  blocks at MultihashIndexSorted's 40 bytes a record, which is far past any
  pack this writer produces (one commit, one pack). A caller packing larger
  passes `:max-index-bytes`; the point is that the number exists and the
  reader chose it, rather than the archive choosing it."
  8388608)

(defn- validate-header!
  "Qualify the whole header, not only the parts this reader happens to use.

  The bounds checks are the payload/index geometry. The characteristics check
  is the other half: CARv2 has a 128-bit bitfield by which an archive says it
  is not an ordinary one, and this reader implements no characteristic. Read
  and discarded, a set bit and a clear bit produce the same read -- the
  archive declared something and the reader proceeded as though it had not.
  So a declared characteristic is refused here rather than ignored."
  [{:keys [data-offset data-size index-offset] :as header}]
  (when-not (and (safe-offset? data-offset) (<= header-bytes data-offset)
                 (safe-offset? data-size) (pos? data-size)
                 (<= data-size (- b/max-safe-integer data-offset))
                 (safe-offset? index-offset)
                 (or (zero? index-offset)
                     (<= (+ data-offset data-size) index-offset)))
    (throw (ex-info "ayatori: invalid CARv2 payload/index bounds"
                    {:type :ayatori/invalid-pack-bounds :header header})))
  (when-not (car2/no-characteristics? header)
    (throw (ex-info (str "ayatori: this CARv2 declares a characteristic and "
                         "this reader implements none. Proceeding would read "
                         "an archive that said it was not ordinary as though "
                         "it were.")
                    {:type :ayatori/unsupported-pack-characteristics
                     :characteristics (:characteristics header)})))
  header)

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
  Repeated offsets share the same next-distinct-offset bound. All offsets
  must lie inside the payload before any block range is requested."
  [{:keys [data-offset data-size]} index]
  (doseq [{:keys [payload-offset]} index]
    (when-not (and (safe-offset? payload-offset) (< 0 payload-offset data-size))
      (throw (ex-info "ayatori: index offset outside CARv1 payload"
                      {:type :ayatori/invalid-index-offset
                       :payload-offset payload-offset :data-size data-size}))))
  (let [offsets (vec (sort (distinct (map :payload-offset index))))
        ends (zipmap offsets (concat (rest offsets) [data-size]))]
    (mapv (fn [{:keys [payload-offset] :as r}]
            (assoc r :file-offset (+ data-offset payload-offset)
                     :frame-length (- (get ends payload-offset) payload-offset)))
          (sort-by :payload-offset index))))

(defn open-pack
  "Read a pack's header and index, and return a handle for block reads.

  `range-fn` is `(fn [{:keys [range]}] -> bytes)` where `:range` is an HTTP
  `Range` header value; the caller owns transport and the pack's address.
  `profile` is checked by `block-profile!` before any request is made.

  Costs two range reads regardless of how many blocks are later read out of
  the pack. `:reads` counts every range request this handle has issued, so a
  caller can assert round trips rather than assume them."
  [{:keys [range-fn profile read-ahead max-index-bytes]
    :or {max-index-bytes max-index-bytes}}]
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
        header (validate-header! (car2/parse-header head-bytes))
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
    ;; Bounded, not open-ended. `bytes=N-` asks for the rest of the object and
    ;; the reader learns how much that was by already holding it; a pack whose
    ;; tail is large -- or a store that ignores the Range and returns the whole
    ;; archive -- is then an unbounded read that no downstream check can undo.
    ;; A short response is fine: `idx/decode` refuses a truncated index rather
    ;; than returning the empty one it cannot distinguish from a real answer.
    (let [index-body (fetch (str "bytes=" index-offset "-"
                                 (+ index-offset max-index-bytes -1)))
          _ (when (> (b/bcount index-body) max-index-bytes)
              (throw (ex-info (str "ayatori: index read exceeded "
                                   max-index-bytes " bytes -- the store "
                                   "returned more than the requested range")
                              {:type :ayatori/index-too-large
                               :bytes (b/bcount index-body)
                               :limit max-index-bytes})))
          index (idx/decode index-body 0)]
      {:header header
       :index index
       :bounded (bounded-index header index)
       :reads reads
       :read-ahead (or read-ahead default-read-ahead)
       :fetch fetch})))

(defn- locations [{:keys [bounded]} cid]
  (let [{:keys [digest mh-code]} (car/read-cid (car/cid->bytes cid) 0)]
    (->> bounded
         (filter #(and (= mh-code (:mh-code %)) (b/equal? digest (:digest %))))
         (reduce (fn [result r]
                   (if (= (:file-offset (peek result)) (:file-offset r))
                     result (conj result r))) [])
         seq)))

(defn locate
  "First multihash candidate's offset and read bound, or nil if none exists.

  Unlike `ipld.car.v2/locate` this returns `:frame-length` too, derived from
  the neighbouring index records rather than fetched -- see `bounded-index`.
  The full CID (including codec) is checked by `read-block`."
  [pack cid]
  (first (locations pack cid)))

(defn read-block
  "Read one block out of an opened pack, verifying the requested full CID.

  Normally costs one range request. Multiple codec aliases for the same
  multihash may require trying multiple distinct indexed offsets.

  Returns the block's bytes, or nil when the pack does not carry `cid` -- a
  pack that does not have a block is an answer, not an error, so a caller can
  fall back to the per-object path. `ipld.car.v2/read-frame` verifies the CID
  of whatever frame it parses, so bytes returned here have already been
  checked against the identity the index claimed."
  [{:keys [fetch] :as pack} cid]
  (loop [candidates (locations pack cid) found nil]
    (if-let [loc (first candidates)]
      (let [body (fetch (car2/range-header loc))
            ;; Even an overlong response cannot extend the index/payload bound.
            bounded-body (b/slice body 0 (min (b/bcount body) (:frame-length loc)))
            {frame-cid :cid frame-bytes :bytes} (car2/read-frame bounded-body 0)]
        (if (= (str cid) (str frame-cid))
          frame-bytes
          ;; An index identifies a multihash, not a codec. Another indexed
          ;; occurrence can carry the requested full CID over the same bytes.
          (recur (next candidates) frame-cid)))
      (when found
        (throw (ex-info "ayatori: pack index pointed at a different block"
                        {:type :ayatori/pack-index-mismatch
                         :wanted (str cid) :found (str found)}))))))

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
