# hashdb

A database for clojure maps with support for database indexes and multiple tenants. Complete history of changes also maintained.

Originally, the database was planned to work like Cassandra, i.e. the latest value per column wins. Also works for maps.

However, it got too complicated, and I do not need to for my scenarios.


# Design goals

 * Store clojure core datastructures: maps, lists, sets and primitive types.
 * Document oriented, it should be easy to make conservative extensions of the data. Not a lot of small entities like in a relational database schema. A typical system maybe as 10-30 entities.
 * We should not be able to have to deserialize to find stuff, i.e. standard DB-indexes needs to be supported (on pre-declared keywords).
 * Should use cheap relational databases like mySQL, mariaDB or postgreslq.
 * Hostable on Amazon or Google, where they manage backups, high-availability etc.
 * No database schema changes after initial deploy. Messy in production.
 * Should manage database size up to a few million entities/documents.
 * Number of concurrent users is at most a few hundred users.
 * Crash-proof, i.e. should use database transactions to make sure internal structure is ok, or be able to repair itself.
 * Optimistic locking.
 * History of changes.
 * Support multiple tenants, and make it hard to write code that accesses data from more than one tenant. Maybe, we should even be able to use row-level priviligies for [mariaDB][3] and [postgresql][4] in the future.

[3]: https://mariadb.com/resources/blog/protect-your-data-row-level-security-mariadb-100
[4]: https://www.postgresql.org/docs/9.5/static/ddl-rowsecurity.html




## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

## Running

You need mysql/mariadb. Update profiles.clj with your information.

    {:profiles/dev  {:env {:database-url "mysql://localhost:3306/hashdb_dev?user=XXXXX&password=YYYY&autoReconnect=true&useSSL=false"}}

I added useSSL=false to make the SSL warnings go away in dev environment.

To create the tables, run

    lein run migrate

or if repl

    (user/start)

## Samples

A quick demo. We run in single tenant mode, we use no indexes, so the indexes-fn always return the empty map. We save a map, and we load it by id.

```
user> (require '[hashdb.db.core :refer [*db*]])
nil
user> (mount/start)
....
user> (require '[hashdb.db.commands :as hashdb])
nil
user> (luminus-migrations.core/migrate ["migrate"] (select-keys hashdb.config/env [:database-url]))
....
nil
user> (hashdb.db.commands/single-tenant-mode)
#'hashdb.db.commands/*tenant*
user> (hashdb.db.commands/set-*indexes-fn* (fn [_] {}))
#'hashdb.db.commands/*indexes-fn*
user> (hashdb.db.commands/create! {:m 30})
{:m 30, :id "ddc2aff5-9867-48ab-9d80-3aa1b2a18fc3", :tenant :single, :updated #inst "2017-11-25T12:25:29.642-00:00", :version 1, :entity :unknown}
user> (hashdb.db.commands/get "ddc2aff5-9867-48ab-9d80-3aa1b2a18fc3")
{:m 30, :id "ddc2aff5-9867-48ab-9d80-3aa1b2a18fc3", :tenant :single, :updated #inst "2017-11-25T12:25:29.642-00:00", :version 1, :entity :unknown}
user>
```

For more operations see

https://github.com/mattiasw2/hashdb/blob/master/test/clj/hashdb/db/commands_test.clj

### Basic terms

 * In a multi-tenant system, the thread can only access data from a single tenant. (There is one exception: `select-by-global`.)
 * Each map has a type called entity.
 * Indexes are defined from tenant x entity to map with keywords -> :long or :string
 * You need to define the indexes before you put in data for that entity, otherwise the index tables are not filled properly.

### The most functions important are

 * Define which top-level map keys should be indexes: `(set-*indexes-fn* <f>)`
 * Start with single-tenant by calling `(single-tenant-mode)`
 * Store maps into using `(create! m)`
 * Update a map on disk using `(update! m changes)` where changes are the top-level keys that should be updated.
 * Load a map using id using `(get id)` and `(try-get id)`.
 * Load many maps using `select-all` `select-by-entity`.
 * Load many maps through database index using `select-by`.
 * Delete maps using `(delete! m)`
 * You find the history (or audit log) of a map using `(history id)`


# mySQL specifics

Originally, I did optimistic locking using timestamp and then I want everything to be UTC. To make sure mysql set to utc

    my.ini

    [mysqld]
    basedir=C:\\tools\\mysql\\current
    datadir=C:\\ProgramData\\MySQL\\data
    default-time-zone='+00:00'

This actually didn't help, so I used a version integer for optimistic locking instead. But it is nice to see the same time in REPL and in SQL studio.


## Ongoing development

 * Make a clojar
 * Add LIKE lookups for strings

## Performance

About 100 update or write operations per second on my 2015 Thinkpad X260.

### Open questions

 * Decide if we should only handle the first index by the database, and the rest by filtering inside Clojure.
 * Should we store as JSON instead of EDN, to make other clients easily read the data?

### Possible experiments

 * Using sequential uuids, https://github.com/danlentz/clj-uuid#sequential-temporal-namespace

## clojure.spec (requires Clojure 1.9RC1)

All important functions and data have SPECs. They are always on, cost about 5% in performance. Not only the call is checked, the return value too, by using the [Orchestra patch][2].

[2]: https://github.com/jeaye/orchestra

## Project template

Generated using Luminus version "2.9.11.91" where a lot has been removed.

## License

Distributed under the Eclipse Public License version 1.0, just like Clojure.

Copyright © 2017 Mattias W
