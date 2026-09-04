(ns ayatori.pack-test
  "CARv2 packs, and the refusal that makes them safe to use.

  The measurement that matters here is round trips, not wall clock: this
  workstation runs many agents at once, so a timing number says more about
  the machine than about the transport. Every test below counts range
  requests."
  (:require [ayatori.pack :as pack]
            [ayatori.remote :as remote]
            [clojure.test :refer [deftest is testing]]
            [ipld.car.bytes :as b]
            [ipld.car.v2 :as car2]
            [ipld.core :as ipld]))

(defn- some-bytes?
  "`b/equal?` answers true for two nils, so an assertion that only compares
  bytes passes when BOTH sides are missing. Measured while writing this file:
  `read-block` destructured the wrong key from `read-frame`, returned nil for
  every block, and the round-trip test went green. Every comparison below
  goes through this floor first."
  [x]
  (and (some? x) (pos? (b/bcount x))))

(defn- blocks [n]
  (mapv #(ipld/node->block {"i" % "pad" (apply str (repeat 40 "x"))}) (range n)))

(defn- range-server
  "An HTTP-shaped range reader over `archive`, counting requests.

  Understands `bytes=a-b` and the open form `bytes=a-`, which is what a
  reader that knows where a frame starts but not where it ends must send."
  [archive counter]
  (fn [{:keys [range]}]
    (swap! counter inc)
    (let [[_ from to] (re-matches #"bytes=(\d+)-(\d*)" range)
          start (js/parseInt from 10)
          total (b/bcount archive)
          end (if (seq to) (min total (inc (js/parseInt to 10))) total)]
      (b/slice archive start (min end total)))))

(def ^:private packed-profile
  {:blocks :packed-blocks :object #{:range-read}})

;; ------------------------------------------------ the declaration gate

(deftest a-backend-must-declare-its-block-layout
  (testing "there is no default: a missing :blocks is refused, not guessed"
    (is (thrown? #?(:clj Exception :cljs :default)
                 (pack/block-profile! {:object #{:range-read}})))
    (is (= :ayatori/undeclared-block-layout
           (try (pack/block-profile! {:object #{:range-read}})
                (catch #?(:clj Exception :cljs :default) e (:type (ex-data e)))))))
  (testing "and an invented layout is refused too"
    (is (= :ayatori/undeclared-block-layout
           (try (pack/block-profile! {:blocks :sharded :object #{:range-read}})
                (catch #?(:clj Exception :cljs :default) e (:type (ex-data e)))))))
  (testing ":block-per-object needs nothing from the object plane"
    (is (= {:blocks :block-per-object :object #{}}
           (pack/block-profile! {:blocks :block-per-object :object #{}})))))

(deftest packed-blocks-without-range-read-is-refused
  ;; ADR-2608160100. This is the one that cannot be left to a later layer:
  ;; a packed reader on a store with no Range still returns correct bytes,
  ;; by GETting the whole archive per block. Round trips go DOWN. Every
  ;; assertion about correctness passes. Only transfer explodes.
  (testing "declaring packed blocks over a store that cannot serve ranges"
    (let [e (try (pack/block-profile! {:blocks :packed-blocks :object #{:presigned-transfer}})
                 (catch #?(:clj Exception :cljs :default) e e))]
      (is (some? e) "this must not be allowed through")
      (is (= :ayatori/packed-blocks-without-range-read (:type (ex-data e))))))
  (testing "with :range-read it is allowed"
    (is (= packed-profile (pack/block-profile! packed-profile))))
  (testing "opening a pack checks the profile BEFORE any request is issued"
    (let [asked (atom 0)]
      (is (thrown? #?(:clj Exception :cljs :default)
                   (pack/open-pack {:range-fn (fn [_] (swap! asked inc) nil)
                                    :profile {:blocks :packed-blocks :object #{}}})))
      (is (zero? @asked)
          "a refused profile must not have cost a network request"))))

;; ------------------------------------------------------- reading blocks

(deftest one-range-read-per-block-after-a-two-read-open
  (let [bs (blocks 8)
        archive (:bytes (car2/pack {:roots [(:cid (first bs))] :blocks bs}))
        counter (atom 0)
        p (pack/open-pack {:range-fn (range-server archive counter)
                           :profile packed-profile})]
    (testing "opening costs exactly two range reads: header, then index"
      (is (= 2 @counter))
      (is (= 2 (pack/reads p))))
    (testing "every block is readable and byte-identical to what was packed"
      (doseq [{:keys [cid bytes]} bs]
        (let [got (pack/read-block p cid)]
          (is (some-bytes? got) (str "block " cid " came back empty"))
          (is (b/equal? bytes got) (str "block " cid)))))
    (testing "and cost one range read each -- not one whole-archive GET"
      (is (= (+ 2 (count bs)) @counter)))))

(deftest a-pack-that-lacks-a-block-answers-rather-than-throws
  (let [bs (blocks 3)
        absent (ipld/node->block {"not" "in the pack"})
        archive (:bytes (car2/pack {:roots [(:cid (first bs))] :blocks bs}))
        counter (atom 0)
        p (pack/open-pack {:range-fn (range-server archive counter)
                           :profile packed-profile})
        before @counter]
    (is (nil? (pack/read-block p (:cid absent)))
        "a pack that does not carry a block is an answer, so the caller can
         fall back to the per-object path")
    (is (= before @counter)
        "and it costs no range request, because the index already said so")))

(deftest a-pack-without-an-index-is-refused
  (let [bs (blocks 2)
        archive (:bytes (car2/pack {:roots [(:cid (first bs))] :blocks bs
                                    :index? false}))
        e (try (pack/open-pack {:range-fn (range-server archive (atom 0))
                                :profile packed-profile})
               (catch #?(:clj Exception :cljs :default) e e))]
    (is (= :ayatori/pack-without-index (:type (ex-data e)))
        "without an index a block cannot be located without reading the whole
         payload, which is the cost this namespace exists to avoid")))

;; --------------------------------------------- composed with the getter

(deftest packed-blocks-flow-through-the-verified-getter-unchanged
  (let [bs (blocks 5)
        archive (:bytes (car2/pack {:roots [(:cid (first bs))] :blocks bs}))
        counter (atom 0)
        p (pack/open-pack {:range-fn (range-server archive counter)
                           :profile packed-profile})
        discoveries (atom 0)
        getter (remote/provider-block-getter
                {:discover-fn (fn [cid]
                                (swap! discoveries inc)
                                {:ok? true :cid cid :mutates-cid? false
                                 :providers [{:plane :discovery :cid cid
                                              :peer "pack" :addrs []
                                              :mutates-cid? false}]})
                 :fetch-fn (pack/pack-fetcher p)})]
    (testing "the pack fetcher is a drop-in fetch-fn: the getter's CID rehash,
              provider fallback and memo are untouched"
      (doseq [{:keys [cid bytes]} bs]
        (let [got (getter cid)]
          (is (some-bytes? got) (str "block " cid " came back empty"))
          (is (b/equal? bytes got)))))
    (testing "and the getter's own memo means a re-read costs no range request"
      (let [reads-after-first-pass @counter]
        (doseq [{:keys [cid]} bs] (getter cid))
        (is (= reads-after-first-pass @counter))))))

(deftest a-pack-whose-index-points-at-the-wrong-frame-is-rejected
  ;; read-frame verifies the CID of whatever frame it parses, so the reader
  ;; catches a mislabelled index rather than trusting it. Simulated by asking
  ;; for one block at another block's offset.
  (let [bs (blocks 4)
        packed (car2/pack {:roots [(:cid (first bs))] :blocks bs})
        archive (:bytes packed)
        counter (atom 0)
        p (pack/open-pack {:range-fn (range-server archive counter)
                           :profile packed-profile})
        wanted (:cid (nth bs 1))
        other (:file-offset (nth (:entries packed) 2))
        lying (assoc p :fetch (fn [_]
                                (b/slice archive other (b/bcount archive))))
        e (try (pack/read-block lying wanted)
               (catch #?(:clj Exception :cljs :default) e e))]
    (is (some? e) "bytes that hash to another CID must not be returned")
    (is (= :ayatori/pack-index-mismatch (:type (ex-data e))))))
