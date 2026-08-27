(ns ayatori.remote-test
  (:require #?(:clj [arrangement.core :as arr])
            [ayatori.remote :as remote]
            [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]))

(defn- providers [cid peers]
  {:ok? true
   :cid cid
   :mutates-cid? false
   :providers (mapv (fn [peer]
                      {:plane :discovery :cid cid :peer peer
                       :addrs [] :mutates-cid? false})
                    peers)})

(deftest provider-fallback-verifies-before-caching
  (let [{wanted-cid :cid wanted-bytes :bytes}
        (ipld/node->block {"kind" "wanted"})
        {wrong-bytes :bytes} (ipld/node->block {"kind" "wrong"})
        discoveries (atom 0)
        getter (remote/provider-block-getter
                {:discover-fn (fn [cid]
                                (swap! discoveries inc)
                                (providers cid ["bad" "good"]))
                 :fetch-fn (fn [provider _]
                             (if (= "bad" (:peer provider))
                               wrong-bytes
                               wanted-bytes))})]
    (testing "a mismatching provider is never allowed to name the block"
      (is (= {"kind" "wanted"} (ipld/decode (getter wanted-cid)))))
    (testing "the verified block is memoised by content identity"
      (is (= {"kind" "wanted"} (ipld/decode (getter wanted-cid))))
      (is (= 1 @discoveries)))))

(deftest every-bad-provider-fails-closed
  (let [{wanted-cid :cid} (ipld/node->block {"kind" "wanted"})
        {wrong-bytes :bytes} (ipld/node->block {"kind" "wrong"})
        getter (remote/provider-block-getter
                {:discover-fn #(providers % ["bad-a" "bad-b"])
                 :fetch-fn (fn [_ _] wrong-bytes)})
        data (try (getter wanted-cid)
                  nil
                  (catch #?(:clj Exception :cljs :default) e (ex-data e)))]
    (is (= :ayatori/all-providers-failed (:type data)))
    (is (= wanted-cid (:cid data)))
    (is (= 2 (count (:attempts data))))
    (is (every? #(= :ipld/cid-mismatch (:reason %)) (:attempts data)))))

(deftest discovery-cannot-substitute-another-cid
  (let [{cid :cid bytes :bytes} (ipld/node->block {"kind" "wanted"})
        fetches (atom 0)
        getter (remote/provider-block-getter
                {:discover-fn (fn [_]
                                {:ok? true :cid "bafy-substitution"
                                 :providers [] :mutates-cid? false})
                 :fetch-fn (fn [_ _] (swap! fetches inc) bytes)})
        data (try (getter cid)
                  nil
                  (catch #?(:clj Exception :cljs :default) e (ex-data e)))]
    (is (= :ayatori/discovery-cid-mismatch (:type data)))
    (is (zero? @fetches))))

(deftest gateway-fetcher-turns-provider-addresses-into-block-requests
  (let [{cid :cid bytes :bytes} (ipld/node->block {"kind" "gateway"})
        requests (atom [])
        fetch (remote/gateway-fetcher
               (fn [request]
                 (swap! requests conj request)
                 {:status 200 :body bytes}))
        provider {:peer "gateway-a"
                  :addrs ["/not-a-multiaddr"
                          "/dns4/ignored.example/tcp/443"
                          "/dns4/blocks.example/tcp/443/https"]}]
    (is (= (str "https://blocks.example/ipfs/" cid)
           (remote/gateway-url (last (:addrs provider)) cid)))
    (is (= {"kind" "gateway"} (ipld/decode (fetch provider cid))))
    (is (= [{:method :get
             :url (str "https://blocks.example/ipfs/" cid)
             :headers {"Accept" "application/vnd.ipld.raw"}}]
           @requests))))

(deftest malformed-cid-never-reaches-discovery
  (let [discoveries (atom 0)
        getter (remote/provider-block-getter
                {:discover-fn (fn [_] (swap! discoveries inc))
                 :fetch-fn (fn [_ _] nil)})]
    (is (thrown? #?(:clj Exception :cljs js/Error) (getter "not-a-cid")))
    (is (zero? @discoveries))))

(deftest insecure-gateway-is-an-explicit-opt-in
  (let [{cid :cid} (ipld/node->block {"kind" "gateway"})
        addr "/dns4/blocks.example/tcp/80/http"]
    (is (nil? (remote/gateway-url addr cid)))
    (is (= (str "http://blocks.example/ipfs/" cid)
           (remote/gateway-url addr cid {:allow-http? true})))))

#?(:clj
   (defn- fixture [quads partitions]
     (let [blocks (atom {})
           put! (fn [cid bytes] (swap! blocks assoc cid bytes))
           blind-fn pr-str
           encrypt-fn identity
           decrypt-fn identity
           db (reduce arr/assert-quad (arr/empty-db) quads)
           snapshot-cid (arr/commit! put! db nil arr/current-schema-version
                                     blind-fn encrypt-fn partitions)]
       {:blocks blocks :snapshot-cid snapshot-cid
        :blind-fn blind-fn :decrypt-fn decrypt-fn})))

#?(:clj
   (defn- open-fixture [{:keys [blocks snapshot-cid blind-fn decrypt-fn]}
                        partitions fetched]
     (remote/open-snapshot
      {:snapshot-cid snapshot-cid
       :discover-fn #(providers % ["provider-a"])
       :fetch-fn (fn [_ cid]
                   (swap! fetched conj cid)
                   (get @blocks cid))
       :blind-fn blind-fn
       :decrypt-fn decrypt-fn
       :partitions partitions})))

#?(:clj
   (deftest datalog-reads-the-persisted-prefix-without-hydration
     (let [{:keys [blocks snapshot-cid] :as fixture}
           (fixture [{:s "s1" :p "kind" :o "rare"}
                     {:s "s2" :p "kind" :o "common"}
                     {:s "s3" :p "noise" :o "value"}]
                    nil)
           fetched (atom [])
           opened (open-fixture fixture nil fetched)
           snapshot (ipld/decode (get @blocks snapshot-cid))
           pso-root (some-> (get-in snapshot ["index-roots" "pso"])
                            ipld/link-cid)
           other-roots (keep #(some-> (get-in snapshot ["index-roots" %])
                                      ipld/link-cid)
                             ["spo" "pos" "ocp"])]
       (is (= #{["s1" "rare"] ["s2" "common"]}
              (remote/q opened
                        '{:find [?s ?kind]
                          :where [[?s "kind" ?kind]]}
                        (constantly true))))
       (is (some #{snapshot-cid} @fetched))
       (is (some #{pso-root} @fetched))
       (is (not-any? (set other-roots) @fetched)
           "a predicate prefix must not hydrate unrelated covering indices")
       (is (= (count (set @fetched)) (:verified-blocks (remote/stats opened)))))))

#?(:clj
   (deftest declared-range-reads-the-persistent-range-index
     (let [partition {:attr "score" :boundaries [25 50 75] :budget-bits 2}
           quads (for [i (range 100)] {:s (str "s" i) :p "score" :o i})
           {:keys [blocks snapshot-cid] :as fixture} (fixture quads [partition])
           fetched (atom [])
           opened (open-fixture fixture [partition] fetched)
           snapshot (ipld/decode (get @blocks snapshot-cid))
           range-root (some-> (get-in snapshot ["index-roots" "range"])
                              ipld/link-cid)
           pos-root (some-> (get-in snapshot ["index-roots" "pos"])
                            ipld/link-cid)
           report (remote/scan-range-report opened "score" 10 20)]
       (is (true? (:pruned? report)))
       (is (= 2 (:budget-bits report)))
       (is (= 10 (count (:quads report))))
       (is (some #{range-root} @fetched))
       (is (not (some #{pos-root} @fetched))
           "a declared range must not fall back to the ordinary pos tree"))))
