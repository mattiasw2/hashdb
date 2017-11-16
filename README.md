# hashdb

Originally, the database was planned to work like Cassandra, i.e. the latest value per column wins. Also works for maps.

However, it got too complicated, and I do not need to for my scenarios.

# mysql

Originally, I did optimistic locking using timestamp and then I want everything to be UTC. To make sure mysql set to utc

    my.ini

    [mysqld]
    basedir=C:\\tools\\mysql\\current
    datadir=C:\\ProgramData\\MySQL\\data
    default-time-zone='+00:00'

This actually didn't help, so I used a version integer for optimistic locking instead. But it is nice to see the same time in REPL and in SQL studio.


## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

## Running

You need mysql/mariadb. Update profiles.clj with your information.

    {:profiles/dev  {:env {:database-url "mysql://localhost:3306/hashdb_dev?user=XXXXX&password=YYYY&autoReconnect=true&useSSL=false"}}

I added useSSL=false to make the SSL warnings go away in dev environment.

To create the tables, run

    lein run migrate

To start a web server for the application, run:

    lein run

or om repl

    (user/start)

## Project template

Generated using Luminus version "2.9.11.91" incl a webserver. That part isn't needed, and maybe I will remove the non-db parts later.

## License

Copyright © 2017 Mattias W
