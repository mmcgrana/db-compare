# db-compare

This project evaluates the performance of a variety of open-source databases. Each databases is benchmarked executing a set of common operations with varying levels of concurrency. The benchmarks are designed to be simple, representative of production code, statistically useful, and fully reproducible.

## Status

Very much a work in progress.

Databases implemented:

 * Memcached
 * Redis
 * MongoDB
 * FleetDB embedded
 * FleetDB server
 * H2
 * MySQL
 * PostgreSQL
 * Ping server
 * Concurrent hash map
 * Null store

Operations implemented:

 * Ping
 * Insert records one-by-one
 * Insert records in bulk
 * Get record by id, sequentially and randomly
 * Get multiple records by ids, sequentially and randomly
 * Find record by indexed value
 * Find multiple records by indexed value
 * Find multiple records by indexed and unindexed values
 * Update records one-by-one
 * Update records in bulk
 * Read/write mixed
 
Databases todo:

 * Berkeley DB Java Edition
 * CouchDB
 * Tokyo Tyrant

Other todo:

 * Linter
 * Programatic reporting
 * Request-level statistics
 * Fully automated execution on cloud servers
 
## License

Copyright 2010 Mark McGranaghan and released under an MIT license.