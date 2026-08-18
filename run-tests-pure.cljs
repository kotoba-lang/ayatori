(ns run-tests-pure
  "The half of this suite that needs no dependencies.

  `run-tests.cljs` is the whole suite and requires `arrangement`, `kotobase`
  and four transitive repos on the classpath — reasonable for CI, expensive
  for a gate. `kotobase.query.agent` deliberately depends on nothing, so the
  check that fabricated attributes and unwrapped predicates are refused can
  run anywhere with:

      npx nbb --classpath src:test run-tests-pure.cljs

  ⚠ This entry names its namespaces in `:require`, so a future
  dependency-free test namespace that is not added here would be silently
  skipped by it — the failure mode `run-tests.cljs` warns about. It is here
  anyway because a gate nobody can afford to run is not a gate; the whole
  suite remains the authority, and this one is a subset that says so."
  (:require [cljs.test :as t]
            [kotobase.query.agent-test]))

(defmethod t/report [::t/default :end-run-tests] [m]
  (when-not (t/successful? m)
    (js/process.exit 1)))

(t/run-all-tests #"^kotobase\.query\.agent-test$")
