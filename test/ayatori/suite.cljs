(ns ayatori.suite
  "The suite, derived from disk rather than from a list.

  ## Why this exists

  Measured 2026-09-04, this repo had FOUR nbb entry points and each reported
  a different green, all `0 failures`:

      bin/run_tests.cljs    53 tests / 106 assertions   (GitHub CI)
      run-tests.cljs        66 / 137                    (fleet gate, self-described
                                                         as \"the whole suite\")
      run-tests-pure.cljs   18 /  47                    (dependency-free half)
      all five namespaces   77 / 165                    (nothing ran this)

  None of them ran the whole suite, and the two fleet gates disagreed with
  each other: `:jvm-test` discovers namespaces through cognitect's runner
  and saw all five, while `nbb-cross-runtime-query` ran `run-tests.cljs` and
  saw four -- missing `ayatori.remote-test`, the ONE namespace that
  exercises discovery -> fetch -> CID verification, which is the thing this
  repo is for.

  `run-tests.cljs` had already diagnosed the failure mode in a comment --
  \"a runner that repeats the list can fall behind the suite and report a
  subset as a pass\" -- and replaced its list with a `t/run-all-tests`
  namespace PATTERN. That does not fix it, for a reason the comment does
  not reach: the pattern filters namespaces that are LOADED, and what gets
  loaded is decided by the `:require` list the pattern was meant to
  replace. Its regex matches `ayatori.remote-test`. The require list omits
  it. So the guard against falling behind was itself downstream of the
  thing it was guarding.

  ## What this does instead

  Enumerate `test/**/*_test.clj[cs]` on disk, let the entry `require` that
  set at its top level, and run exactly it. There is no list to fall behind,
  because there is no list.

  The require has to happen at the ENTRY's top level: nbb awaits top-level
  loads and does not await one issued from inside a function, so requiring
  from within `run!` loads nothing and `t/run-tests` throws
  `No namespace: ... found`. Loud, but for the wrong reason.

  Two floors, because a runner that cannot see the suite must not return
  the same value as a runner that saw it and found nothing wrong:

    - zero test namespaces found  -> exit 2 (wrong directory / bad classpath)
    - zero assertions executed    -> exit 2 (namespaces loaded but empty)

  Exit 2 is neither 0 nor 1: \"could not answer\" is not a pass and not a
  failure."
  (:require [cljs.test :as t]
            [clojure.string :as str]
            ["fs" :as fs]
            ["path" :as path]))

(defn- walk [dir]
  (mapcat (fn [entry]
            (let [p (path/join dir (.-name entry))]
              (if (.isDirectory entry) (walk p) [p])))
          (fs/readdirSync dir #js {:withFileTypes true})))

(defn- file->ns [p]
  (-> p
      (str/replace #"^test[/\\]" "")
      (str/replace #"\.clj[cs]$" "")
      (str/replace #"[/\\]" ".")
      (str/replace "_" "-")
      symbol))

(defn test-namespaces
  "Every `*_test.clj[cs]` under `test/`, as namespace symbols, sorted."
  []
  (->> (walk "test")
       (filter #(re-find #"_test\.clj[cs]$" %))
       (map file->ns)
       sort
       vec))

(defn refuse!
  "Exit 2 -- the value that means the suite could not be measured."
  [& msg]
  (println (str/join " " (cons "REFUSING to report a pass:" msg)))
  (js/process.exit 2))

(defn run!
  "Load and run every test namespace found on disk."
  []
  (let [nss (test-namespaces)]
    (when (zero? (count nss))
      (refuse! "no *_test.clj[cs] under test/ --"
               "run this from the repo root with test/ on the classpath"))
    (println "SCANNED" (count nss) "test namespaces:" (str/join ", " nss))
    (defmethod t/report [:cljs.test/default :end-run-tests] [m]
      (if (zero? (+ (:pass m) (:fail m) (:error m)))
        (refuse! "0 assertions ran across" (count nss) "namespaces")
        (when-not (t/successful? m)
          (set! (.-exitCode js/process) 1))))
    (apply t/run-tests nss)))
