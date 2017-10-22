# hashdb

generated using Luminus version "2.9.11.91"

Cassandra works like this, i.e. the latest value per column wins. Also works for maps.

# mysql

Since I compare using timestamp, make sure mysql set to utc

my.ini

   [mysqld]
   basedir=C:\\tools\\mysql\\current
   datadir=C:\\ProgramData\\MySQL\\data
   default-time-zone='+00:00'

This actually didn't help, so I used a version instead. But anyway, it is nice to see the same time in REPL and in SQL studio.


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

## License

Copyright © 2017 Mattias W
