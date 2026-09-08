;; bench/fitness.cljs — ayatori's deterministic fitness function.
;;
;; The judge in a Co-Scientist loop has to be a measurement, or the loop is
;; theatre (ADR-2607132300: an LLM panel scored four libraries 4.0-5.0/5
;; while missing four concrete gaps none of them named). This is that judge.
;;
;; ROUND TRIPS, not wall clock. ADR-2608160100 says the metric is round trip
;; count; this workstation runs many agents at once, so a millisecond figure
;; describes the machine's load and not the transport. Every number here is a
;; count of operations that would cross a network, plus the bytes they move.
;;
;; Two questions:
;;
;;   1. SCALING. For a query whose ANSWER stays the same size, how do block
;;      reads grow as the database grows? Hydrating a whole db is O(database)
;;      -- measured on `arrangement` 2026-08-01, 50 block reads at 2k facts
;;      and 640 at 32k, independent of rows returned. `ayatori.remote` claims
;;      to avoid that by following only the index ranges a query names.
;;
;;   2. COST PER BLOCK. A per-object read costs an IPNI lookup AND a fetch.
;;      A packed read costs one range request. Both arms below answer the
;;      same query over the same snapshot, so the difference is transport.
;;
;; Run: nbb --classpath "<the run_tests classpath>" bench/fitness.cljs
(ns fitness
  (:require [arrangement.core :as arr]
            [ayatori.pack :as pack]
            [ayatori.remote :as remote]
            [kotoba.lang.text :as str]
            [ipld.car.bytes :as b]
            [ipld.car.v2 :as car2]))

;; `arrangement.core/commit!` requires Promise-returning blind/encrypt on
;; cljs; the synchronous `open-snapshot` requires the opposite and now
;; REFUSES a Promise-returning one rather than answering `#{}` (measured
;; 2026-09-04). Two pairs of the same functions, which is the shape of the
;; contract and not a workaround.
(defn- blind [x] (pr-str x))
(defn- crypto [bytes] bytes)
(defn- blind-async [x] (js/Promise.resolve (pr-str x)))
(defn- crypto-async [bytes] (js/Promise.resolve bytes))

(defn- corpus
  "`n` subjects, each with a `kind` and a `name`. Exactly `rare-n` are
  `kind=rare`, so THE ANSWER SIZE IS FIXED while `n` grows -- which is what
  makes the sweep a statement about the database and not about the result."
  [n rare-n]
  (vec (mapcat (fn [i]
                 [{:s (str "s" i) :p "kind" :o (if (< i rare-n) "rare" "common")}
                  {:s (str "s" i) :p "name" :o (str "Subject " i)}])
               (range n))))

(defn- build [quads]
  (let [blocks (atom {})
        put! (fn [cid bytes]
               (swap! blocks assoc cid bytes)
               (js/Promise.resolve cid))]
    (-> (arr/commit! put! (reduce arr/assert-quad (arr/empty-db) quads)
                     nil arr/current-schema-version blind-async crypto-async)
        (.then (fn [cid] {:blocks blocks :snapshot-cid cid})))))

(defn- byte-count [x]
  (cond (nil? x) 0
        (number? (.-length x)) (.-length x)
        (number? (.-byteLength x)) (.-byteLength x)
        :else 0))

(def ^:private query
  '{:find [?s ?name]
    :where [[?s "kind" "rare"]
            [?s "name" ?name]]})

(defn- per-object-arm
  "Today's path: discovery then fetch, per block."
  [{:keys [blocks snapshot-cid]}]
  (let [discoveries (atom 0) fetches (atom 0) bytes (atom 0)]
    (let [opened (remote/open-snapshot
                  {:snapshot-cid snapshot-cid
                   :discover-fn (fn [cid]
                                  (swap! discoveries inc)
                                  {:ok? true :cid cid :mutates-cid? false
                                   :providers [{:plane :discovery :cid cid :peer "p"
                                                :addrs [] :mutates-cid? false}]})
                   :fetch-fn (fn [_ cid]
                               (let [x (get @blocks cid)]
                                 (swap! fetches inc)
                                 (swap! bytes + (byte-count x))
                                 x))
                   :blind-fn blind :decrypt-fn crypto})
          rows (remote/q opened query (constantly true))]
      {:arm "per-object" :rows (count rows)
       :round-trips (+ @discoveries @fetches)
       :discoveries @discoveries :reads @fetches :bytes @bytes})))

(defn- packed-arm
  "Every block of the snapshot packed into one CARv2, read through
  `:block-source`. No IPNI lookup is needed to find a block already in hand."
  [{:keys [blocks snapshot-cid]}]
  (let [entries (mapv (fn [[cid bytes]] {:cid cid :bytes bytes}) @blocks)
        archive (:bytes (car2/pack {:roots [snapshot-cid] :blocks entries}))
        range-reads (atom 0) bytes (atom 0)
        range-fn (fn [{:keys [range]}]
                   (swap! range-reads inc)
                   (let [[_ from to] (re-matches #"bytes=(\d+)-(\d*)" range)
                         start (js/parseInt from 10)
                         total (b/bcount archive)
                         end (if (seq to) (min total (inc (js/parseInt to 10))) total)
                         slice (b/slice archive start (min end total))]
                     (swap! bytes + (b/bcount slice))
                     slice))
        p (pack/open-pack {:range-fn range-fn
                           :profile {:blocks :packed-blocks
                                     :object #{:range-read}}})
        discoveries (atom 0)
        opened (remote/open-snapshot
                {:snapshot-cid snapshot-cid
                 :discover-fn (fn [cid]
                                (swap! discoveries inc)
                                {:ok? true :cid cid :mutates-cid? false
                                 :providers [{:plane :discovery :cid cid :peer "p"
                                              :addrs [] :mutates-cid? false}]})
                 :fetch-fn (fn [_ _] nil)
                 :block-source #(pack/read-block p %)
                 :blind-fn blind :decrypt-fn crypto})
        rows (remote/q opened query (constantly true))]
    {:arm "packed" :rows (count rows)
     :round-trips @range-reads
     :discoveries @discoveries :reads @range-reads :bytes @bytes
     :archive-bytes (b/bcount archive)}))

(defn- packed-arm-async
  "The SAME pack through open-snapshot-async (Worker-native path).
  Iteration 02 seed: before the async getter took :block-source, a Worker
  behind a pack paid discovery per block. This arm is the measured answer."
  [{:keys [blocks snapshot-cid]}]
  (let [entries (mapv (fn [[cid bytes]] {:cid cid :bytes bytes}) @blocks)
        archive (:bytes (car2/pack {:roots [snapshot-cid] :blocks entries}))
        range-reads (atom 0) bytes (atom 0)
        range-fn (fn [{:keys [range]}]
                   (swap! range-reads inc)
                   (let [[_ from to] (re-matches #"bytes=(\d+)-(\d*)" range)
                         start (js/parseInt from 10)
                         total (b/bcount archive)
                         end (if (seq to) (min total (inc (js/parseInt to 10))) total)
                         slice (b/slice archive start (min end total))]
                     (swap! bytes + (b/bcount slice))
                     slice))
        p (pack/open-pack {:range-fn range-fn
                           :profile {:blocks :packed-blocks
                                     :object #{:range-read}}})
        discoveries (atom 0)]
    (-> (remote/open-snapshot-async
         {:snapshot-cid snapshot-cid
          :discover-fn (fn [cid]
                         (swap! discoveries inc)
                         (js/Promise.resolve
                          {:ok? true :cid cid :mutates-cid? false
                           :providers [{:plane :discovery :cid cid :peer "p"
                                        :addrs [] :mutates-cid? false}]}))
          :fetch-fn (fn [_ _] (js/Promise.resolve nil))
          :block-source #(js/Promise.resolve (pack/read-block p %))
          :blind-fn blind-async :decrypt-fn crypto-async})
        (.then (fn [opened]
                 (-> (remote/q-async opened query (constantly true))
                     (.then (fn [rows]
                              {:arm "packed-async" :rows (count rows)
                               :round-trips @range-reads
                               :discoveries @discoveries
                               :reads @range-reads :bytes @bytes
                               :archive-bytes (b/bcount archive)}))))))))

(defn- fmt [& xs] (str/join (map (fn [[w v]] (.padStart (str v) w)) (partition 2 xs))))

(defn -main []
  (println)
  (println "ayatori fitness — counted, not timed (ADR-2608160100)")
  (println "query:" (pr-str query) "  answer fixed at 3 rows")
  (println)
  (println (fmt 7 "subj" 8 "quads" 8 "blocks" 6 "rows"
                14 "|  per-object" 8 "disc" 7 "read" 10 "bytes"
                12 "|  packed" 8 "disc" 7 "read" 10 "bytes"))
  (println (str/join (repeat 140 "-")))
  (-> (reduce
       (fn [pr n]
         (.then pr (fn [acc]
                     (.then (build (corpus n 3))
                            (fn [snap]
                              (.then (packed-arm-async snap)
                                     (fn [c]
                              (let [a (per-object-arm snap)
                                    b (packed-arm snap)]
                                (when-not (= (:rows a) (:rows b) 3 (:rows c))
                                  (println "REFUSING: the two arms did not return"
                                           "the same 3 rows:" (:rows a) "vs" (:rows b))
                                  (js/process.exit 2))
                                (println (fmt 7 n 8 (* 2 n) 8 (count @(:blocks snap))
                                              6 (:rows a)
                                              14 (:round-trips a) 8 (:discoveries a)
                                              7 (:reads a) 10 (:bytes a)
                                              12 (:round-trips b) 8 (:discoveries b)
                                              7 (:reads b) 10 (:bytes b)))
                                (conj acc [n a b c])))))))))
       (js/Promise.resolve [])
       [50 100 200 400 800])
      (.then (fn [rs]
               (println)
               (let [[n0 a0 _] (first rs) [n1 a1 b1 c1] (last rs)]
                 (println (str "scaling: database grew " (/ (* 2 n1) (* 2 n0)) "x, "
                               "per-object round trips grew "
                               (.toFixed (/ (:round-trips a1) (double (:round-trips a0))) 2) "x"))
                 (println (str "transport: at " n1 " subjects, per-object "
                               (:round-trips a1) " round trips vs packed "
                               (:round-trips b1) " (sync) / " (:round-trips c1) " (async) -- "
                               (.toFixed (/ (:round-trips a1) (double (:round-trips b1))) 2) "x")))
               (when (zero? (:round-trips (second (first rs))))
                 (println "REFUSING: zero round trips at the smallest size")
                 (js/process.exit 2))))
      (.catch (fn [e]
                (println "REFUSING: the harness threw:" (str e))
                (js/process.exit 2)))))

(-main)
