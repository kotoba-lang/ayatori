;; nbb test runner — first-class runtime per repo rule (kotoba wasm >
;; clojurewasm > cljs > nbb > (jvm/bb)). Run from the repo root:
;;
;;   nbb --classpath "src:test:.deps/kotobase/src:.deps/security/src:.deps/arrangement/src:.deps/prolly-tree/src:.deps/io-ipld/src:.deps/io-ipld-car/src:.deps/io-multiformats/src:.deps/org-ietf-cbor/src:.deps/dev-protobuf/src:.deps/datom-source/src:.deps/datalog/src:.deps/io-ipni-specs/src:.deps/block-cache/src:.deps/org-nist-sha2/src" bin/run_tests.cljs
;;
;; where every .deps/<name> is a checkout of the matching kotoba-lang repo
;; at the SHA pinned in deps.edn (kotobase, arrangement, io-ipld,
;; io-multiformats, io-ipni-specs) or in arrangement's own deps.edn
;; transitively (prolly-tree, datom-source, datalog, block-cache).
;; CI pins every one of them to the same SHAs.
;;
;; This entry names no test namespaces. `ayatori.suite` derives them from
;; the files on disk and refuses (exit 2) when it cannot see them -- see its
;; docstring for the four disagreeing greens that made that necessary.
;;
;; The `require` is at the top level on purpose: nbb awaits top-level loads
;; and does NOT await one made inside a function, so requiring from within
;; `run!` loads nothing and `t/run-tests` then throws `No namespace: ...`.
(ns run-tests (:require [ayatori.suite :as suite]))

(apply require (suite/test-namespaces))
(suite/run!)
