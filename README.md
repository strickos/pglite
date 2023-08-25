# pglite

`pglite` is a server app that exposes SQLite databases over a Postgres connection.

`pglite` unlocks a number of use-cases for hosting + serving SQLite databases to various users/apps over a standard PG connection.

**WARNING**: This is a very much an alpha release to demonstrate the concept - there is very little real functionality implemented, eg. no real authenticator + and no clustering support - do not use this in production!

## Features

* Access to SQLite databases over a standard PG Wire connection
* Authentication using Plaintext Username/Password Auth
* Database selection based on connection user + database
* Database handle sharing between connections
* Support for: 
  * Simple Queries
  * Queries with positional paramters
  * Prepared statements
* Basic building blocks to enable building: 
  * Custom Authentication handlers
  * Custom backend providers

## Build and Run

After cloning the repo, navigate to the root directory of this repo, then run: 

```Bash
cargo build
```

Then, run the binary, either directly or via cargo: 

```Bash
## Directly
./target/debug/pglite

## Via Cargo
cargo run
```

## Configuring PGLite

There are a number of configuration options available to customise PGLite to your needs.

To see the complete list of options run: `pglite --help`

A key option is: `--db-root`, which specifies the directory under which the SQLite databases are found.

eg. If your databases are located under a directory call `databases`, then run pglite like this: 

```Bash
./pglite --db-root databases
```

## Authentication

`pglite` currently only supports a simple authenticator that uses a static password (configured via the `--auth-config={password}` arg).

When you connect to `pglite`, specify your username, the database you wish to connect to and the password that matches the static password.

With the simple authenticator, your username determines the folder under the database root to look in for the specified database.

eg, if your username is `john` and your database is `data.sqlite` - then you will be interacting with the database at: `{dbroot}/john/data.sqlite`.


## Performance

Very little work has gone into optimising performance, so currently it's ok when not under stress, but doesn't scale well.

Following are results from a basic test, sharing a single database, with one table, between all connections, for a simple `SELECT` query that contains one field equality WHERE clause (e.g. `select * from links where url = ?`)

Under *non-stress* conditions: (1 connection, continuously querying the same database)
* Avg. Simple Query time: `0.14ms`
* Avg. Prepared Query time: `0.11ms`

Under *stress* conditions (100 concurrent connections, continuously querying the same Database):
* Avg. Simple Query time: `4.8ms`
* Avg. Prepared Query time: `3.4ms`


## TODO

There are sooo many things left to do, too many to list ;p

Here's a non-exhaustive list of the next few things to do: 

* Support for MD5 Passowrd + SCRAM Auth
* Support for an external Auth provider (eg. A user DB, or an external service)
* Add TLS Support
* WAL + clustering support
* Performance + impproved concurrent connection handling
* Alternative Backends like: 
  * Amazon S3
  * External SQL providers like: 
    * Amazon RDS/Aurora
    * Amazon Athena
    * A Presto cluster
    * etc...
  * Amazon DynamoDB
* DB Size + Usage Quotas
* Metering
* Custom functions - maybe support webassembly or something like that


## Contributing

All suggestions + contributions are welcome.

## License

This project is licensed under the MIT license - enjoy :)