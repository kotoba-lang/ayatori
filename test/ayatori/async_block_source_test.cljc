(ns ayatori.async-block-source-test
  "Iteration 02 tests for the async block-source path. Kept in their own
  namespace so the file layout mirrors the feature boundary: the async getter
  serving a pack without discovery, and the corrupt-source fall-through."
  (:require [arrangement.core :as arr]
            [ayatori.remote :as remote]
            [clojure.test :as t :refer [deftest is async]]))

(defn- providers [cid peers]
  {:ok? true :cid cid :mutates-cid? false
   :providers (mapv (fn [p] {:plane :discovery :cid cid :peer p
                             :addrs [] :mutates-cid? false})
                    peers)})

#?(:cljs
   (deftest worker-native-block-source-serves-without-discovery
     ;; Iteration 02 seed: a Worker behind a pack paid discovery per block
     ;; because :block-source reached the SYNCHRONOUS getter only. The async
     ;; getter must serve the source first, verify its bytes like a provider's,
     ;; and coalesce concurrent callers through :in-flight.
     (async done
       (let [blocks (atom {})
             put! (fn [cid bytes]
                    (swap! blocks assoc cid bytes)
                    (js/Promise.resolve cid))
             blind (fn [x] (js/Promise.resolve (pr-str x)))
             crypto (fn [bytes] (js/Promise.resolve bytes))
             quads [{:s "s1" :p "kind" :o "rare"}
                    {:s "s1" :p "name" :o "Rare One"}
                    {:s "s2" :p "kind" :o "common"}]]
         (-> (arr/commit! put! (reduce arr/assert-quad (arr/empty-db) quads)
                          nil arr/current-schema-version blind crypto)
             (.then (fn [snapshot-cid]
                      (remote/open-snapshot-async
                       {:snapshot-cid snapshot-cid
                        :discover-fn #(js/Promise.resolve (providers % ["p1"]))
                        :fetch-fn (fn [_ cid]
                                    (js/Promise.resolve (get @blocks cid)))
                        :block-source (fn [cid]
                                        (js/Promise.resolve (get @blocks cid)))
                        :blind-fn blind :decrypt-fn crypto})))
             (.then (fn [opened]
                      (-> (remote/q-async
                           opened
                           '{:find [?s ?name]
                             :where [[?s "kind" "rare"]
                                     [?s "name" ?name]]}
                           (constantly true))
                          (.then (fn [got]
                                   (is (= #{["s1" "Rare One"]} got)
                                       "the packed async path answers the real join")
                                   (let [s (remote/stats opened)]
                                     (is (= 0 (:discoveries s))
                                         "a block in hand costs ZERO discoveries")
                                     (is (pos? (:source-hits s))
                                         "the source served the reads"))
                                   (done))))))
             (.catch (fn [e]
                       (is false (str "async block-source test threw: " e))
                       (done))))))))

#?(:cljs
   (deftest worker-native-block-source-corrupt-falls-through-to-provider
     ;; A source that answers with the WRONG bytes is skipped like a
     ;; mismatching provider -- and counted, so a corrupt pack is visible
     ;; rather than merely slow. The read still succeeds via discovery.
     (async done
       (let [blocks (atom {})
             put! (fn [cid bytes]
                    (swap! blocks assoc cid bytes)
                    (js/Promise.resolve cid))
             blind (fn [x] (js/Promise.resolve (pr-str x)))
             crypto (fn [bytes] (js/Promise.resolve bytes))
             quads [{:s "s1" :p "kind" :o "rare"}]]
         (-> (arr/commit! put! (reduce arr/assert-quad (arr/empty-db) quads)
                          nil arr/current-schema-version blind crypto)
             (.then (fn [snapshot-cid]
                      (remote/open-snapshot-async
                       {:snapshot-cid snapshot-cid
                        :discover-fn #(js/Promise.resolve (providers % ["p1"]))
                        :fetch-fn (fn [_ cid]
                                    (js/Promise.resolve (get @blocks cid)))
                        :block-source (fn [_cid]
                                        (js/Promise.resolve (js/Uint8Array. 4)))
                        :blind-fn blind :decrypt-fn crypto})))
             (.then (fn [opened]
                      (-> (remote/scan-async opened [nil "kind" nil])
                          (.then (fn [got]
                                   (is (= #{{:s "s1" :p "kind" :o "rare"}} got)
                                       "the provider path rescued a corrupt source")
                                   (let [s (remote/stats opened)]
                                     (is (pos? (:source-failures s))
                                         "the corrupt source is COUNTED, not silent")
                                     (is (pos? (:discoveries s))
                                         "discovery ran after the source fell through"))
                                   (done))))))
             (.catch (fn [e]
                       (is false (str "corrupt-source test threw: " e))
                       (done))))))))

#?(:clj
   ;; The synchronous getter already covers this contract (remote_test.cljc);
   ;; on the JVM there is no async path, so this namespace has nothing new.
   (deftest async-block-source-is-cljs-only-placeholder
     (is true "async block-source is a Worker-native surface")))
