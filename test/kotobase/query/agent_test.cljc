(ns kotobase.query.agent-test
  "Both directions. A validator that only ever refuses is as useless as one
  that only ever passes, and this one has been each of those in turn:
  on 2026-08-18 an earlier copy rejected eighteen correct queries by treating
  value strings as attributes, and the fix for that walked zero clauses and
  passed everything."
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase.query.agent :as agent]))

(def schema
  {:datasets ["market-intel" "repo-taxonomy"]
   :attributes [{:attr "source/dataset" :doc "which dataset"}
                {:attr "company/lei" :doc "join key"}
                {:attr "company/ticker"}
                {:attr "company/name"}
                {:attr "company/revenue-usd"}
                {:attr "company/net-income-usd"}
                {:attr "company/legal-name"}
                {:attr "repo/kind"}
                {:attr "repo/path"}]})

;; ---------------------------------------------------------------- passes

(deftest values-are-not-attributes
  (testing "a value string in the value position is not checked against the
            attribute list -- the failure that invalidated a 60-minute run"
    (is (nil? (agent/validate
               '[:find ?t :where [?e "source/dataset" "market-intel"]
                                 [?e "company/ticker" ?t]]
               schema)))))

(deftest wrapped-predicates-pass
  (is (nil? (agent/validate
             '[:find ?t :where [?e "company/revenue-usd" ?r] [(>= ?r 1e11)]
                               [?e "company/ticker" ?t]]
             schema))))

(deftest joins-and-aggregates-pass
  (is (nil? (agent/validate
             '[:find ?lei :where [?a "source/dataset" "market-intel"] [?a "company/lei" ?lei]
                                 [?b "source/dataset" "repo-taxonomy"] [?b "company/lei" ?lei]]
             schema)))
  (is (nil? (agent/validate '[:find (count ?e) :where [?e "repo/kind" "actor"]] schema)))
  (is (nil? (agent/validate '[:find ?k (count ?e) :where [?e "repo/kind" ?k]] schema))
      "a variable in the attribute position is legal Datalog"))

;; ------------------------------------------------- the two real survivors
;; Both came out of the 2026-08-18 measurement, from the schema+repair
;; condition, and both reached the engine because nothing checked for them.
;; They read as semantic errors and are not: each is a predicate written as
;; a data pattern.

(deftest operator-in-entity-position-is-refused
  (let [r (agent/validate
           '[:find ?ticker :where [?e "source/dataset" "market-intel"]
                                  [?e "company/revenue-usd" ?revenue]
                                  [?e "company/ticker" ?ticker]
                                  [>= ?revenue 100000000000]]
           schema)]
    (is (= :predicate-not-wrapped (:error r))
        "DataScript answers this one with \"Missing rules var '%' in :in\"")
    (is (re-find #"\[\(>=" (:hint r)) "the hint shows the wrapped form")))

(deftest infix-predicate-is-refused
  (let [r (agent/validate
           '[:find ?name :where [?e "source/dataset" "market-intel"]
                                [?e "company/net-income-usd" ?ni]
                                [?e "company/name" ?name]
                                [?ni < 0]]
           schema)]
    (is (= :predicate-not-wrapped (:error r))
        "DataScript answers this one with \"Cannot compare company/sic to <\"")))

;; ---------------------------------------------------------------- refuses

(deftest fabricated-attributes-are-refused
  (testing "every bare-condition failure in the measurement was one of these"
    (doseq [[bad q] {"company/revenue"
                     '[:find ?t :where [?e "company/revenue" ?r] [?e "company/ticker" ?t]]
                     "market-intel/lei"
                     '[:find ?l :where [?e "market-intel/lei" ?l]]}]
      (let [r (agent/validate q schema)]
        (is (= :unknown-attributes (:error r)) (str "should refuse " bad))
        (is (= bad (:got r)))))))

(deftest keyword-attributes-are-refused
  (let [r (agent/validate '[:find ?t :where [?e :company/ticker ?t]] schema)]
    (is (= :keyword-attributes (:error r)))
    (is (re-find #"company/ticker" (:hint r)))))

(deftest structural-refusals
  (is (= :not-a-vector (:error (agent/validate {:find "x"} schema))))
  (is (= :missing-find (:error (agent/validate '[:where [?e "repo/kind" ?k]] schema))))
  (is (= :missing-where (:error (agent/validate '[:find ?e] schema))))
  (is (= :empty-find (:error (agent/validate '[:find :where [?e "repo/kind" ?k]] schema)))
      "passes every other structural check and makes DataScript throw"))

(deftest the-walk-actually-visits-clauses
  (testing "a regression guard: an earlier fix used JS indexOf on a keyword,
            found nothing, and passed every query including this one"
    (is (seq (agent/where-clauses '[:find ?x :where [?e "company/lei" ?x]])))
    (is (some? (agent/validate '[:find ?x :where [?e "nope/nope" ?x]] schema)))))

;; ---------------------------------------------------------------- prompt

(deftest prompt-carries-the-schema
  (let [p (agent/system-prompt schema)]
    (is (re-find #"company/revenue-usd" p))
    (is (re-find #"market-intel" p))
    (is (re-find #"\[\(>= \?r 1e11\)\]" p) "the predicate form is shown, not just named"))
  (testing "the bare baseline the measurement used omits it"
    (let [p (agent/system-prompt schema {:schema? false :examples? false})]
      (is (not (re-find #"company/revenue-usd" p)))
      (is (re-find #"kotobase" p)))))

(deftest repair-turn-carries-the-refusal
  (let [r (agent/validate '[:find ?t :where [?e :company/ticker ?t]] schema)
        t (agent/repair-turn r)]
    (is (re-find #"keyword-attributes" t))))

;; ---------------------------------------------------------------- extract

(deftest extraction
  (is (= "[:find ?t :where [?e \"a/b\" ?t]]"
         (agent/extract-query "[:find ?t :where [?e \"a/b\" ?t]]")))
  (is (= "[:find ?t :where [?e \"a/b\" ?t]]"
         (agent/extract-query "```clojure\n[:find ?t :where [?e \"a/b\" ?t]]\n```")))
  (is (= "[:find ?t :where [?e \"a/b\" ?t]]"
         (agent/extract-query "はい:\n[:find ?t :where [?e \"a/b\" ?t]]\n以上です。"))
      "trailing prose is dropped at the balanced close")
  (is (= "[:find ?x :where [?e \"a/b\" ?x]]"
         (agent/extract-query "<think>reasoning</think>\n[:find ?x :where [?e \"a/b\" ?x]]")))
  (is (re-find #"\(>= \?a 1e12\)"
               (agent/extract-query "[:find ?n :where [?e \"a/b\" ?a] [(>= ?a 1e12)]]"))
      "nested predicate parens do not close the query early")
  (is (nil? (agent/extract-query "すみません、分かりません。"))
      "no query is a different answer from an empty one"))
