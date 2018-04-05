# How to Run

1. Create `books` table in PostgreSQL database `booktown`:

```SQL
CREATE TABLE books (
	id SERIAL PRIMARY KEY NOT NULL,
	title TEXT NOT NULL,
	author_id INT NOT NULL,
	tags text[] NOT NULL,
	price double precision NOT NULL,
	publish_date timestamp NOT NULL,
	"text" TEXT NOT NULL,
	"text2" TEXT NOT NULL,
	"text3" TEXT NOT NULL
);

CREATE INDEX ON books(title);
CREATE INDEX ON books(price);
```

2. Benchmark INSERT:

```bash
go run ormbench.go -insert <library>
```

3. Benchmark SELECT:

```bash
go run ormbench.go -select <library>
```

## Available Libraries

* `pg` (ORM) - github.com/upper/db
* `go-pg` (ORM) - github.com/go-pg/pg
* `pq` (driver) - github.com/lib/pq
* `pgx` (driver) - github.com/jackc/pgx
