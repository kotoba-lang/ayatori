(ns ayatori.remote
  "Verified, range-pruned queries over a remote persistent arrangement.

  This namespace is the wiring between Ayatori's three existing planes:

  1. `discover-fn` resolves a block CID to IPNI provider records;
  2. `fetch-fn` asks those providers for the IPLD block bytes;
  3. every returned block is rehashed against the requested CID;
  4. `arrangement.source/cursor` follows only the persisted index ranges a
     query names; and
  5. `ayatori.query/q` evaluates Datalog directly over that cursor.

  Discovery is not retrieval, and retrieval is not verification.  The
  separate injected functions keep those effects visible and make provider
  fallback testable without a network.  A verified block is memoised by CID
  for the lifetime of an opened snapshot; unverified bytes are never cached.

  The persistent index is produced by `arrangement.core/commit!` (or its
  incremental variants).  Ayatori reads that IPLD snapshot; it does not copy
  it into a throwaway in-memory database."
  (:require [arrangement.source :as arrangement-source]
            [ayatori.discovery :as discovery]
            [ayatori.query :as query]
            [clojure.string :as str]
            [ipld.core :as ipld]
            [multiformats.multiaddr :as multiaddr]))

(defn- message [e]
  #?(:clj (.getMessage ^Throwable e)
     :cljs (or (.-message e) (str e))))

(defn- byte-count [bytes]
  #?(:clj (count bytes)
     :cljs (or (.-byteLength bytes) (.-length bytes) (count bytes))))

(defn- validate-cid! [cid]
  (when-not (and (string? cid) (some? (ipld/cid-codec cid)))
    (throw (ex-info "ayatori: expected a parseable CID"
                    {:type :ayatori/invalid-cid :value cid})))
  cid)

(def ^:private empty-stats
  {:discoveries 0 :fetch-attempts 0 :provider-failures 0
   :verified-blocks 0 :memo-hits 0 :verified-bytes 0})

(defn- stats-atom [stats]
  (let [stats (or stats (atom {}))]
    (swap! stats #(merge empty-stats %))
    stats))

(defn- failure [provider reason detail]
  (cond-> {:peer (:peer provider) :reason reason}
    detail (assoc :detail detail)))

(defn- validate-discovery! [cid result]
  (when-not (map? result)
    (throw (ex-info "ayatori: discovery did not return a result map"
                    {:type :ayatori/invalid-discovery-result :cid cid})))
  (when-not (:ok? result)
    (throw (ex-info "ayatori: provider discovery failed"
                    {:type :ayatori/discovery-failed
                     :cid cid
                     :reason (:reason result)
                     :answered (:answered result)
                     :quorum (:quorum result)})))
  (when-not (= cid (:cid result))
    (throw (ex-info "ayatori: discovery changed the requested CID"
                    {:type :ayatori/discovery-cid-mismatch
                     :expected-cid cid :actual-cid (:cid result)})))
  (when (true? (:mutates-cid? result))
    (throw (ex-info "ayatori: discovery reported that it mutates CIDs"
                    {:type :ayatori/discovery-mutates-cid :cid cid})))
  (let [providers (vec (:providers result))]
    (doseq [provider providers]
      (when-not (map? provider)
        (throw (ex-info "ayatori: discovery returned a non-map provider"
                        {:type :ayatori/invalid-provider :cid cid})))
      (when (and (contains? provider :cid) (not= cid (:cid provider)))
        (throw (ex-info "ayatori: provider record names a different CID"
                        {:type :ayatori/provider-cid-mismatch
                         :expected-cid cid :actual-cid (:cid provider)
                         :peer (:peer provider)})))
      (when (true? (:mutates-cid? provider))
        (throw (ex-info "ayatori: provider record mutates the content CID"
                        {:type :ayatori/provider-mutates-cid
                         :cid cid :peer (:peer provider)}))))
    (when (empty? providers)
      (throw (ex-info "ayatori: no provider advertises the requested CID"
                      {:type :ayatori/no-providers :cid cid})))
    providers))

(defn gateway-url
  "Turn one HTTP-gateway multiaddr into its raw-block URL.

  Supported address shape is a DNS/IP host, optional TCP port, and `/https`
  (or `/http` only when `:allow-http? true`). The default path is the IPFS
  trustless-gateway block namespace `/ipfs/{cid}`; a provider with another
  stable block route may supply `:path-prefix`.

  Returns nil for a well-formed address that is not an HTTP gateway. Parsing
  errors propagate to the gateway fetcher, which records the bad address and
  continues to the provider's next address."
  ([addr cid] (gateway-url addr cid {}))
  ([addr cid {:keys [allow-http? path-prefix]
              :or {allow-http? false path-prefix "/ipfs/"}}]
   (validate-cid! cid)
   (let [components (multiaddr/components (multiaddr/parse addr))
         values (into {} components)
         protocols (set (map first components))
         scheme (cond (protocols "https") "https"
                      (and allow-http? (protocols "http")) "http"
                      :else nil)
         host (or (get values "dns4") (get values "dns6")
                  (get values "dns") (get values "ip4")
                  (get values "ip6"))
         port (get values "tcp")]
     (when (and scheme host)
       (let [host (if (and (get values "ip6")
                           (not (and (str/starts-with? host "[")
                                     (str/ends-with? host "]"))))
                    (str "[" host "]") host)
             default-port? (or (and (= scheme "https") (= port "443"))
                               (and (= scheme "http") (= port "80")))]
         (str scheme "://" host
              (when (and port (not default-port?)) (str ":" port))
              path-prefix cid))))))

(defn gateway-fetcher
  "Build a `fetch-fn` for `provider-block-getter` from an injected HTTP port.

  `http-fn` receives `{:method :get :url ... :headers ...}` and returns
  `{:status :body}`. Every usable provider address is tried in order with
  `Accept: application/vnd.ipld.raw`. Only a 200 body is returned; CID
  verification still happens outside this function, before the bytes can be
  cached or queried.

  Options are forwarded to `gateway-url`: `:allow-http?` (false by default)
  and `:path-prefix` (`/ipfs/` by default)."
  ([http-fn] (gateway-fetcher http-fn {}))
  ([http-fn opts]
   (when-not (fn? http-fn)
     (throw (ex-info "ayatori: gateway http-fn is required"
                     {:type :ayatori/missing-gateway-http-fn})))
   (fn fetch-provider [provider cid]
     (loop [addrs (:addrs provider) attempts []]
       (if-let [addr (first addrs)]
         (let [url-result (try
                            {:url (gateway-url addr cid opts)}
                            (catch #?(:clj Exception :cljs :default) e
                              {:error (message e)}))
               url (:url url-result)]
           (if (:error url-result)
             (recur (next addrs)
                    (conj attempts {:addr addr :reason :invalid-multiaddr
                                    :detail (:error url-result)}))
             (if-not url
             (recur (next addrs) (conj attempts {:addr addr :reason :unsupported}))
             (let [response (try
                              (http-fn {:method :get :url url
                                        :headers {"Accept" "application/vnd.ipld.raw"}})
                              (catch #?(:clj Exception :cljs :default) e
                                {:transport-error (message e)}))]
               (if (and (= 200 (:status response)) (some? (:body response)))
                 (:body response)
                 (recur (next addrs)
                        (conj attempts
                              (if-let [detail (:transport-error response)]
                                {:addr addr :reason :transport-error :detail detail}
                                {:addr addr :reason :http-error
                                 :status (:status response)}))))))))
         (throw (ex-info "ayatori: provider has no successful HTTP gateway"
                         {:type :ayatori/no-gateway-route
                          :peer (:peer provider) :attempts attempts})))))))

(defn provider-block-getter
  "Build a synchronous `(fn [cid] -> verified-bytes)` block getter.

  Options:

  - `:discover-fn` — `(fn [cid] -> {:ok? :cid :providers ...})`;
  - `:fetch-fn` — `(fn [provider cid] -> bytes-or-nil)`;
  - `:stats` — optional caller-owned atom;
  - `:block-memo` — optional caller-owned atom keyed by CID.

  Providers are tried in discovery order. A missing, throwing, or
  CID-mismatching provider is skipped; success is possible only when some
  provider returns bytes that hash to the requested CID. If every provider
  fails, the function throws with bounded metadata and never exposes or
  caches the unverified bytes.

  The transport is synchronous because `arrangement.source/cursor` is
  synchronous. A Worker-native async cursor is a separate API rather than a
  Promise hidden inside this one."
  [{:keys [discover-fn fetch-fn stats block-memo]}]
  (when-not (fn? discover-fn)
    (throw (ex-info "ayatori: discover-fn is required"
                    {:type :ayatori/missing-discover-fn})))
  (when-not (fn? fetch-fn)
    (throw (ex-info "ayatori: fetch-fn is required"
                    {:type :ayatori/missing-fetch-fn})))
  (let [stats (stats-atom stats)
        block-memo (or block-memo (atom {}))]
    (fn get-verified [cid]
      ;; Reject malformed identities before discovery or provider I/O. A CID
      ;; that cannot be parsed is not a cache miss and must not become a URL.
      (validate-cid! cid)
      (if-let [cached (get @block-memo cid)]
        (do (swap! stats update :memo-hits inc) cached)
        (let [result (try
                       (swap! stats update :discoveries inc)
                       (discover-fn cid)
                       (catch #?(:clj Exception :cljs :default) e
                         (throw (ex-info "ayatori: provider discovery threw"
                                         {:type :ayatori/discovery-failed
                                          :cid cid :detail (message e)} e))))
              providers (validate-discovery! cid result)]
          (loop [remaining providers failures []]
            (if-let [provider (first remaining)]
              (let [attempt
                    (try
                      (swap! stats update :fetch-attempts inc)
                      (if-let [bytes (fetch-fn provider cid)]
                        (try
                          (if-let [verified
                                   (ipld/get-verified-block (fn [_] bytes) cid)]
                            {:verified verified}
                            {:failure (failure provider :missing nil)})
                          (catch #?(:clj Exception :cljs :default) e
                            {:failure (failure provider
                                               (or (:type (ex-data e)) :verification-failed)
                                               (message e))}))
                        {:failure (failure provider :missing nil)})
                      (catch #?(:clj Exception :cljs :default) e
                        {:failure (failure provider :transport-error (message e))}))]
                (if-let [verified (:verified attempt)]
                  (do
                    (swap! block-memo assoc cid verified)
                    (swap! stats (fn [s]
                                   (-> s
                                       (update :verified-blocks inc)
                                       (update :verified-bytes + (byte-count verified)))))
                    verified)
                  (do
                    (swap! stats update :provider-failures inc)
                    (recur (next remaining) (conj failures (:failure attempt))))))
              (throw (ex-info "ayatori: no provider returned a verified block"
                              {:type :ayatori/all-providers-failed
                               :cid cid :attempts failures})))))))))

(defn open-snapshot
  "Open a persistent arrangement snapshot through IPNI-backed block reads.

  Required options are `:snapshot-cid`, `:fetch-fn`, `:blind-fn`, and
  `:decrypt-fn`, plus either:

  - `:discover-fn`, or
  - `:http-fn` and optional `:discovery-opts`, which are composed through
    `ayatori.discovery/find-providers`.

  `:partitions` is the same declared range-partition vector used when the
  snapshot was committed. The return value is an opened session containing
  `:source`, `:get-block`, and caller-observable `:stats`. Opening verifies
  the snapshot block immediately; index blocks remain lazy and are fetched
  only when a query reaches their ranges."
  [{:keys [snapshot-cid discover-fn http-fn discovery-opts fetch-fn
           blind-fn decrypt-fn partitions stats block-memo]}]
  (when-not (string? snapshot-cid)
    (throw (ex-info "ayatori: snapshot-cid must be a CID string"
                    {:type :ayatori/invalid-snapshot-cid :value snapshot-cid})))
  (when-not (fn? blind-fn)
    (throw (ex-info "ayatori: blind-fn is required"
                    {:type :ayatori/missing-blind-fn})))
  (when-not (fn? decrypt-fn)
    (throw (ex-info "ayatori: decrypt-fn is required"
                    {:type :ayatori/missing-decrypt-fn})))
  (let [discover-fn (or discover-fn
                        (when (fn? http-fn)
                          (fn [cid]
                            (discovery/find-providers http-fn cid
                                                      (or discovery-opts {})))))
        stats (stats-atom stats)
        block-memo (or block-memo (atom {}))
        get-block (provider-block-getter {:discover-fn discover-fn
                                          :fetch-fn fetch-fn
                                          :stats stats
                                          :block-memo block-memo})
        source (arrangement-source/cursor get-block snapshot-cid blind-fn
                                          decrypt-fn partitions)]
    {:snapshot-cid snapshot-cid
     :source source
     :get-block get-block
     :stats stats
     :block-memo block-memo}))

(defn q
  "Run Datalog directly over an opened persistent snapshot.

  No in-memory database is materialized. The arrangement cursor asks for the
  index prefix/range needed by each Datalog clause, and every block crossing
  the provider boundary is CID-verified by the opened session."
  ([opened query-form visible?]
   (query/q (:source opened) query-form visible?))
  ([opened query-form visible? inputs]
   (query/q (:source opened) query-form visible? inputs)))

(defn scan-range-report
  "Read one declared value range and report whether persisted range buckets
  actually pruned the read, including their disclosure budget."
  ([opened attr lo hi]
   (scan-range-report opened attr lo hi {}))
  ([opened attr lo hi opts]
   (arrangement-source/scan-range-report (:source opened) attr lo hi opts)))

(defn stats
  "Current verified retrieval counters for `opened`. These are block/byte
  counts, not wall-clock latency and not proof of an external deployment."
  [opened]
  @(:stats opened))
