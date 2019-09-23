# Strategies for linking genome features to GO terms in the KBase Relation Engine

## Background 

When a genome is indexed into the RE, the following happens:

* The shadow object is created
* For each feature in the genome, a feature vertex is created
* For each feature, an edge from the shadow object to the feature vertex is created.

On a reindex, all these vertices and edges are replaced, but since they have unique `_keys`
that are generated from the immutable object data the replacements are no-ops (unless the schema
has been updated with field changes, etc.)

For feature -> GO (Gene Ontology) edges, it’s more complicated because GO is not immutable
and changes over time - represented in the RE via `created` / `expired` timestamps on the
GO vertices and edges. This means that an index at time `t0` may have different feature -> GO
edges than a reindex at time `t1`.

In either of the two strategies presented below, a field that denotes the edge was added by the
indexer should be added to separate the indexer edges from app or user added edges.

**NOTE**: Reindexing delta loaded collections from scratch (e.g. if the collection is dumped)
leads to non-reproducible data as the creation/expiration dates will change. Delta loaded
collections *must* have backups.
In other words, once a feature -> GO link is created, its creation date must never be changed
by reindexing. It can be expired by a new edge (or if the edge is intentionally deleted) but
the expiration date must be in the future at the time the edge is update to avoid unreproducible
queries.

### Resources

* [Feature to GO edge schema](https://github.com/kbase/relation_engine_spec/blob/develop/schemas/ws/ws_feature_has_GO_annotation.yaml)

## Option 1: Edges never expire

Add feature -> GO edges for all the links, make the creation date `now` and expiration ∞.
Each edge has a unique ID consisting of the feature ID, edge source (e.g. `indexer`) and the
GO term `_key`. On a reindex import the edges as usual but ignore key collisions (thus leaving
the pre-existing edge with the same creation and expiration dates). This means that updated
edges are added to the schema with a new created timestamp and existing edges are untouched.

The created timestamp in both cases should be reasonably far in the future such that all edges
can be loaded before it occurs to avoid queries against partial loads.

Note that if the load fails midway reproducibility may be impacted if it cannot be completed
or rolled back before the creation date passes in real time. Developing a roll back tool may be
worthwhile.

### Pros
* The implementation is relatively simple (although still involved) and is not subject to
  race conditions.
* If two indexers are processing the object at the same time and a GO create/expire timepoint
  passes in real time at the same time, the results should be the same regardless of the order of
  the saves (with the exception of small changes in the creation date).

### Cons
* Edges are never expired. This means that in traversal queries originating from the feature side
  of the graph, older edges may appear in the results (but are dead ends since presumably the
  traversal will stop at expired GO vertices). Traversals originating from the GO side of the
  graph are unaffected.
* If fields are added to edges in the schema, the edges will not be updated with new fields on
  a reindex (which may be good for reproducibility’s sake). These issues could be worked around
  via writing new code to modify existing edges as a one off.

## Option 2: Delta algorithm via transaction per feature

On an index or reindex, for each feature create a transaction that performs the delta load
algorithm for the feature’s old and new edges, expiring old edges and creating new edges.
Specifically:

* Find the GO vertices corresponding to the feature’s GO terms and the current timestamp.
  * Find extant vertices for the term
  * If no extant nodes, find the newest merge edges from the term and get the new term
  * Go back to the start
* Create new edges to the GO vertices
* Find all the old edges originating from the feature that were created by the indexer
* If the edge isn’t in the set of new edges, expire it
  * There are currently no edge fields that could change that would require special comparisons

The created timestamp should be reasonably far in the future such that all edges can be
created/expired before it occurs to avoid queries against partial loads.

Note that if the load fails midway reproducibility may be impacted if it cannot be completed
or rolled back before the creation date passes in real time. Developing a roll back tool may be
worthwhile.

### Pros
* There is no time where an edge is expired but its replacement missing, or both the edge
  and its replacement exist.
* Race conditions are eliminated (I think... pretty sure)

### Cons
* The implementation is much more complex and may require more than 100K transactions
  (perhaps some of the features could be batched together).
* I’m not even sure if the transaction described is possible in AQL.
* The implementation is most likely much slower than simple batch loads.
* Most of the compute cost is moved to arangodb from the indexer.



