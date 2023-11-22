## Multi-node Deprecation

Multi-node support has been deprecated.
TimescaleDB 2.13 is the last version that will include multi-node support.
Multi-node support in 2.13 is available for PostgreSQL 13, 14 and 15.

If you want to migrate from multi-node TimescaleDB to single-node TimescaleDB
read the [migration documentation](https://docs.timescale.com/migrate/latest/multi-node-to-timescale-service/).

### Why we have deprecated multi-node support

We began to work on multi-node support in 2018 and released the first version in 2020 to provide higher scalability
for TimescaleDB deployments. Since then and as we’ve continued to evolve our community and cloud products,
we’ve come to the realization that multi-node was not the most forward-looking approach,
and we have instead turned our focus to more cloud-native designs that inherently leverage compute/storage separation for higher scalability.

These are the challenges we encountered with our multi-node architecture:
- <b>Lower development speed</b>

  Multi-node was hard to maintain and evolve and added a very high tax on any new feature we developed,
  significantly slowing down our development speed.
  Additionally, only ~1% of TimescaleDB deployments use multi-node.

- <b>Inconsistent developer experience</b>

  Multi-node imposed a key initial decision on developers adopting TimescaleDB: start with single-node or start with multi-node.
  Moving from one to the other required a migration.
  On top of that there were a number of features supported on single-node that were not available on multi-node making
  the experience inconsistent and the choice even more complicated.

- <b>Expensive price / performance ratio</b>

  We were very far from linear scalability, not least due to the natural “performance hit” when scaling from a single-node to distributed transactions.
  For example, handling 2x the load a single node could handle required 8 servers.
  At the same time we’ve been able to dramatically improve the performance of single-node to handle 2 million inserts
  per second as well as improve query performance dramatically with a
  number of [query optimizations](https://www.timescale.com/blog/8-performance-improvements-in-recent-timescaledb-releases-for-faster-query-analytics/)
  and [vectorized query execution](https://www.timescale.com/blog/teaching-postgres-new-tricks-simd-vectorization-for-faster-analytical-queries/). 

- <b>Not designed to leverage new cloud architectures</b>

  As we’ve gained experience developing and scaling cloud solutions we’ve realized that we can solve
  the same problems more easily through new more modern approaches that leverage cloud technologies,
  which often separate compute and storage to scale them independently.
  For example, we’re now leveraging object storage in our cloud offering to deliver virtually infinite
  storage capacity at a very low cost with [Tiered Storage](https://www.timescale.com/blog/scaling-postgresql-for-cheap-introducing-tiered-storage-in-timescale/),
  and our [columnar compression](https://www.timescale.com/blog/building-columnar-compression-in-a-row-oriented-database/) (built concurrently to multi-node)
  offers effective per-disk capacity that’s 10-20x that of traditional Postgres.
  Similarly, users can scale compute both vertically and horizontally by either dynamically resizing compute
  allocated to cloud databases, or adding additional server replicas (each of which can use the same tiered storage).

Given all those reasons, we’ve made the difficult decision to deprecate multi-node so we can accelerate feature development
and performance improvements, and deliver a better developer experience for the 99% of our community that is not using multi-node.

### Questions and feedback

We understand the news will be disappointing to users of multi-node. We’d like to help and provide advice on the best path forward for you.

If you have any questions or feedback, you can share them in the #multi-node channel in our [community Slack](https://slack.timescale.com/).
