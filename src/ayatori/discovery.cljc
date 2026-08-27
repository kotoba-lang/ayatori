(ns ayatori.discovery
  "IPNI provider discovery for Ayatori.

  Discovery answers *where a CID may be fetched*. It does not fetch IPLD
  blocks, verify their CIDs, traverse a graph, or evaluate a query. Keeping
  those effects separate prevents an indexer response from being mistaken
  for content or query evidence. HTTP transport and JSON parsing are injected
  by the caller through `ipni.find`."
  (:require [ipni.find :as ipni]))

(def default-indexers ipni/default-indexers)
(def default-routers ipni/default-routers)

(defn find-providers
  "Discover providers for `cid` through Delegated Routing V1/IPNI.

  Returns `ipni.find/find-providers`'s structured result and never rewrites
  the content CID. `http-fn` owns transport; `opts` may supply `:routers`,
  `:quorum`, and `:parse-fn`."
  [http-fn cid opts]
  (ipni/find-providers http-fn cid opts))

(defn get-cid
  "Query one IPNI-native `/cid/{cid}` endpoint when ContextID or Metadata is
  needed. Provider discovery alone is not proof that the IPLD block exists or
  hashes to `cid`; the retrieval layer must still verify it."
  ([http-fn indexer cid]
   (ipni/get-cid http-fn indexer cid))
  ([http-fn indexer cid opts]
   (ipni/get-cid http-fn indexer cid opts)))
