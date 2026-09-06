(ns ayatori.pack-bounds-test
  (:require [clojure.test :refer [deftest is]]
            [ayatori.pack :as pack]
            [ipld.car :as car]
            [ipld.car.bytes :as b]
            [ipld.car.index :as idx]
            [ipld.car.v2 :as car2]
            [ipld.core :as ipld]
            [multiformats.core :as mf]))

(defn- open [archive requests]
  (pack/open-pack
   {:profile {:blocks :packed-blocks :object #{:range-read}}
    :range-fn
    (fn [{:keys [range]}]
      (swap! requests conj range)
      (let [[_ a z] (re-matches #"bytes=(\d+)-(\d*)" range)
            parse #?(:clj #(Long/parseLong %) :cljs #(js/parseInt % 10))
            start (parse a)
            end (if (seq z) (inc (parse z)) (b/bcount archive))]
        (b/slice archive start (min end (b/bcount archive)))))}))

(defn- reindex [packed entries]
  (b/concat [(b/slice (:bytes packed) 0 (:index-offset packed))
             (idx/encode entries)]))

(defn- fixture []
  (let [blocks (mapv #(ipld/node->block {"n" %}) (range 3))]
    (car2/pack {:roots [(:cid (first blocks))] :blocks blocks})))

(deftest duplicate-offsets-use-the-next-distinct-bound
  (let [packed (fixture)
        entries (:entries packed)
        archive (reindex packed (concat entries [(first entries) (first entries)]))
        requests (atom [])
        p (open archive requests)]
    (doseq [{:keys [cid frame-length]} entries]
      (is (= frame-length (:frame-length (pack/locate p cid))))
      (is (b/equal? (get (:blocks (car2/read-all (:bytes packed))) cid)
                    (pack/read-block p cid))))
    (is (= 5 (count @requests)))))

(deftest sparse-index-reads-remain-inside-the-payload
  (let [packed (fixture)
        entries (:entries packed)
        indexed [(first entries) (last entries)]
        requests (atom [])
        p (open (reindex packed indexed) requests)]
    (is (= (+ (:frame-length (first entries)) (:frame-length (second entries)))
           (:frame-length (pack/locate p (:cid (first entries))))))
    (doseq [{:keys [cid]} indexed]
      (is (b/equal? (get (:blocks (car2/read-all (:bytes packed))) cid)
                    (pack/read-block p cid))))
    (is (nil? (pack/read-block p (:cid (second entries)))))
    (is (= 4 (count @requests)))))

(deftest same-digest-different-hash-code-is-not-a-candidate
  (let [packed (fixture)
        [a c] (:entries packed)
        {digest :digest} (car/read-cid (car/cid->bytes (:cid c)) 0)
        foreign (mf/cidv1 0x71 (b/concat [(b/varint 0x11)
                                         (b/varint (b/bcount digest)) digest]))
        ;; The foreign-code record deliberately points to a different block.
        requests (atom [])
        p (open (reindex packed [(assoc a :cid foreign) c]) requests)]
    (is (= (:file-offset c) (:file-offset (pack/locate p (:cid c)))))
    (is (b/equal? (get (:blocks (car2/read-all (:bytes packed))) (:cid c))
                  (pack/read-block p (:cid c))))
    (is (= 3 (count @requests)))))

(deftest codec-aliases-try-all-multihash-candidates
  (let [node (ipld/node->block {"same" "bytes"})
        raw (assoc node :cid (mf/cidv1-raw (:bytes node)))
        packed (car2/pack {:roots [(:cid node)] :blocks [raw node]})
        archive (reindex packed (conj (:entries packed) (first (:entries packed))))
        requests (atom [])
        p (open archive requests)]
    (is (b/equal? (:bytes node) (pack/read-block p (:cid node))))
    (is (= 4 (count @requests)) "duplicate offset is fetched only once")
    (is (b/equal? (:bytes raw) (pack/read-block p (:cid raw))))
    (is (= 5 (count @requests)))))

(deftest wrong-full-cid-still-fails-closed
  (let [node (ipld/node->block {"same" "bytes"})
        raw (assoc node :cid (mf/cidv1-raw (:bytes node)))
        packed (car2/pack {:blocks [raw]})
        p (open (:bytes packed) (atom []))]
    (is (= :ayatori/pack-index-mismatch
           (try (pack/read-block p (:cid node))
                (catch #?(:clj Exception :cljs :default) e (:type (ex-data e))))))))

(deftest invalid-header-bounds-stop-before-index-fetch
  (doseq [header [{:data-offset 50 :data-size 1 :index-offset 51}
                  {:data-offset 51 :data-size 0 :index-offset 51}
                  {:data-offset 51 :data-size 100 :index-offset 52}
                  {:data-offset b/max-safe-integer :data-size 1
                   :index-offset b/max-safe-integer}]]
    (let [requests (atom [])]
      (is (= :ayatori/invalid-pack-bounds
             (try (open (b/concat [car2/pragma (car2/header header)]) requests)
                  (catch #?(:clj Exception :cljs :default) e (:type (ex-data e))))))
      (is (= 1 (count @requests))))))

(deftest invalid-index-offsets-stop-before-block-fetch
  (let [packed (fixture)]
    (doseq [offset [0 (:data-size packed) (inc (:data-size packed))]]
      (let [requests (atom [])]
        (is (= :ayatori/invalid-index-offset
               (try (open (reindex packed [(assoc (first (:entries packed))
                                                  :payload-offset offset)]) requests)
                    (catch #?(:clj Exception :cljs :default) e (:type (ex-data e))))))
        (is (= 2 (count @requests)))))))

(deftest padded-data-and-index-offsets-are-honored
  (let [packed (fixture)
        data-offset 73
        index-offset (+ data-offset (:data-size packed) 13)
        archive (b/concat [car2/pragma
                            (car2/header {:data-offset data-offset
                                          :data-size (:data-size packed)
                                          :index-offset index-offset})
                            (b/->bytes (repeat 22 0))
                            (b/slice (:bytes packed) (:data-offset packed)
                                     (:index-offset packed))
                            (b/->bytes (repeat 13 0))
                            (idx/encode (:entries packed))])
        requests (atom [])
        p (open archive requests)]
    (doseq [{:keys [cid payload-offset frame-length]} (:entries packed)]
      (is (= (+ data-offset payload-offset) (:file-offset (pack/locate p cid))))
      (is (= frame-length (:frame-length (pack/locate p cid))))
      (is (b/equal? (get (:blocks (car2/read-all (:bytes packed))) cid)
                    (pack/read-block p cid))))))

(deftest provider-cannot-extend-a-frame-past-the-read-bound
  (let [packed (fixture)
        entry (first (:entries packed))
        ;; Forged index boundary splits the valid first frame.
        archive (reindex packed [entry (assoc (second (:entries packed))
                                              :payload-offset (inc (:payload-offset entry)))])
        p (open archive (atom []))
        overlong (assoc p :fetch (fn [_] (b/slice (:bytes packed) (:file-offset entry)
                                                  (:index-offset packed))))]
    (is (thrown? #?(:clj Exception :cljs :default)
                 (pack/read-block overlong (:cid entry))))))

(deftest truncated-or-corrupt-frames-never-return-bytes
  (let [packed (fixture)
        entry (first (:entries packed))
        p (open (:bytes packed) (atom []))
        frame (b/slice (:bytes packed) (:file-offset entry)
                       (+ (:file-offset entry) (:frame-length entry)))
        corrupt (b/concat [(b/slice frame 0 (dec (b/bcount frame)))
                           (b/->bytes [(bit-xor 1 (b/bget frame (dec (b/bcount frame))))])])]
    (doseq [bytes [(b/->bytes [0x80])
                   (b/slice frame 0 (dec (b/bcount frame))) corrupt]]
      (is (thrown? #?(:clj Exception :cljs :default)
                   (pack/read-block (assoc p :fetch (constantly bytes)) (:cid entry)))))))
