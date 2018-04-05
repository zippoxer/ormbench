package main

import (
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-pg/pg"

	"upper.io/db.v3"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/icrowley/fake"
	"github.com/jackc/pgx"

	"upper.io/db.v3/postgresql"

	_ "github.com/lib/pq"
)

var (
	insertDB   = flag.String("insert", "", "benchmark inserts with the given library")
	selectDB   = flag.String("select", "", "benchmark selects with the given library")
	cpuprofile = flag.String("cpu", "", "write cpu profile to file")
	memprofile = flag.String("mem", "", "write mem profile to file")
)

func main() {
	flag.Parse()

	fake.Seed(time.Now().UnixNano())

	if *memprofile != "" {
		debug.SetGCPercent(-1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	switch *insertDB {
	case "pg":
		pgInsert()
	case "pgx":
		pgxInsert()
	case "pq":
		pqInsert()
	case "go-pg":
		goPGInsert()
	case "mongo":
		mongoInsert()
	}

	switch *selectDB {
	case "pg":
		pgSelect()
	case "mongo":
		mongoSelect()
	case "sqlx":
		sqlxSelect()
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("allocated mem: %.2fM out of %.2fM", float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}

type Book struct {
	ID          uint           `db:"id,omitempty"`
	Title       string         `db:"title"`
	AuthorID    uint           `db:"author_id"`
	Tags        pq.StringArray `db:"tags"`
	Price       float64        `db:"price"`
	PublishDate time.Time      `db:"publish_date"`
	Text        string         `db:"text"`
	Text2       string         `db:"text2"`
	Text3       string         `db:"text3"`
}

var tags = make([]string, 10)

func fakeBook(book *Book) {
	book.Title = fake.Title()
	book.AuthorID = 1
	for i := 0; i < len(tags); i++ {
		tags[i] = fake.Word()
	}
	book.Tags = tags
	book.Price = rand.Float64()
	book.PublishDate = time.Now()
	book.Text = fake.WordsN(10)
	book.Text2 = fake.WordsN(20)
	book.Text3 = fake.WordsN(30)
}

const (
	insertCount      = 10e3
	totalInsertCount = 100e3
)

func pgInsert() {
	sess, err := postgresql.Open(postgresql.ConnectionURL{
		Database: "booktown",
		Host:     "localhost",
		User:     "postgres",
		Password: "121234",
		Options: map[string]string{
			"sslmode": "require",
		},
	})
	sess.SetTxOptions(sql.TxOptions{
		Isolation: sql.LevelDefault,
	})
	sess.SetPreparedStatementCache(true)
	sess.SetLogging(false)
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	books := sess.Collection("books")
	start := time.Now()
	batchStart := time.Now()
	for i := 0; i <= totalInsertCount; i++ {
		fakeBook(&book)
		id, err := books.Insert(&book)
		if err != nil {
			log.Fatal(err)
		}
		if i > 0 && i%insertCount == 0 {
			log.Printf("inserted %.1fK within %v (current ID is %v)", insertCount/1e3, time.Since(batchStart), id)
			batchStart = time.Now()
		}
	}
	log.Printf("insert finished within %v", time.Since(start))
}

const query = `insert into books
(title, author_id, tags, price, publish_date, text, text2, text3)
values
($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id`

func pgxInsert() {
	conn, err := pgx.Connect(pgx.ConnConfig{
		Database: "booktown",
		Host:     "localhost",
		User:     "postgres",
		Password: "121234",
	})
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	_, err = conn.Prepare("insert", query)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	batchStart := time.Now()
	for i := 0; i <= totalInsertCount; i++ {
		fakeBook(&book)
		var id uint
		err := conn.QueryRow("insert",
			book.Title, book.AuthorID,
			book.Tags, book.Price, book.PublishDate,
			book.Text, book.Text2, book.Text3).Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		if i > 0 && i%insertCount == 0 {
			log.Printf("inserted %.1fK within %v (current ID is %v)", insertCount/1e3, time.Since(batchStart), id)
			batchStart = time.Now()
		}
	}
	log.Printf("insert finished within %v", time.Since(start))
}

func pqInsert() {
	db, err := sql.Open("postgres", "postgres://postgres:121234@localhost/booktown")
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	batchStart := time.Now()
	for i := 0; i <= totalInsertCount; i++ {
		fakeBook(&book)
		row := stmt.QueryRow(
			book.Title, book.AuthorID,
			book.Tags, book.Price, book.PublishDate,
			book.Text, book.Text2, book.Text3)
		var id uint
		err := row.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		if i > 0 && i%insertCount == 0 {
			log.Printf("inserted %.1fK within %v (current ID is %v)", insertCount/1e3, time.Since(batchStart), id)
			batchStart = time.Now()
		}
	}
	log.Printf("insert finished within %v", time.Since(start))
}

func goPGInsert() {
	db := pg.Connect(&pg.Options{
		User:     "postgres",
		Password: "121234",
		Database: "booktown",
	})
	defer db.Close()
	// db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
	// 	query, err := event.FormattedQuery()
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	log.Printf("%s %s", time.Since(event.StartTime), query)
	// })
	var book Book
	start := time.Now()
	batchStart := time.Now()
	for i := 0; i <= totalInsertCount; i++ {
		book.ID = 0
		fakeBook(&book)
		_, err := db.Model(&book).Returning("id").Insert()
		if err != nil {
			log.Fatal(err)
		}
		if i > 0 && i%insertCount == 0 {
			log.Printf("inserted %.1fK within %v (current ID is %v)", insertCount/1e3, time.Since(batchStart), book.ID)
			batchStart = time.Now()
		}
	}
	log.Printf("insert finished within %v", time.Since(start))
}

func mongoInsert() {
	sess, err := mgo.Dial("localhost")
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	start := time.Now()
	batchStart := time.Now()
	for i := 0; i <= totalInsertCount; i++ {
		fakeBook(&book)
		err := sess.DB("booktown").C("books").Insert(book)
		if err != nil {
			log.Fatal(err)
		}
		if i > 0 && i%insertCount == 0 {
			log.Printf("inserted %.1fK within %v", insertCount/1e3, time.Since(batchStart))
			batchStart = time.Now()
		}
	}
	log.Printf("insert finished within %v", time.Since(start))
}

const selectCount = 5e3

func pgSelect() {
	sess, err := postgresql.Open(postgresql.ConnectionURL{
		Database: "booktown",
		Host:     "localhost",
		User:     "postgres",
		Password: "121234",
	})
	sess.SetPreparedStatementCache(true)
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	books := sess.Collection("books")
	found := 0
	start := time.Now()
	for i := 0; i < selectCount; i++ {
		res := books.Find(db.Cond{
			"price": db.Gt(0.9),
		}).Limit(100)
		for res.Next(&book) {
			found++
		}
		if res.Err() != nil {
			if res.Err() != db.ErrNoMoreRows {
				log.Fatal(err)
			}
		}
	}
	took := time.Since(start)
	log.Printf("selected %.0fK times within %v (%.2f selects per second, %v latency, %.0fK found)",
		float64(selectCount)/1e3,
		took,
		1/(float64(took.Seconds())/selectCount),
		took/selectCount,
		float64(found)/1e3)
}

func mongoSelect() {
	sess, err := mgo.Dial("localhost")
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	found := 0
	books := sess.DB("booktown").C("books")
	start := time.Now()
	for i := 0; i < selectCount; i++ {
		res := books.Find(bson.M{"price": bson.M{"$gt": 0.9}}).Limit(100)
		iter := res.Iter()
		for iter.Next(&book) {
			found++
		}
		if iter.Err() != nil {
			if iter.Err() != mgo.ErrNotFound {
				log.Fatal(iter.Err())
			}
		}
	}
	took := time.Since(start)
	log.Printf("selected %.0fK times within %v (%.2f selects per second, %v latency, %.0fK found)",
		float64(selectCount)/1e3,
		took,
		1/(float64(took.Seconds())/selectCount),
		took/selectCount,
		float64(found)/1e3)
}

func sqlxSelect() {
	panic("todo")

	db, err := sqlx.Connect("postgres", "postgres://postgres:121234@localhost/booktown")
	if err != nil {
		log.Fatal(err)
	}
	var book Book
	found := 0
	stmt, err := db.Preparex(`select * from books where price > 0.9 limit 1`)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < selectCount; i++ {
		err := stmt.Get(&book)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Fatal(err)
			}
		} else {
			found++
		}
	}
	took := time.Since(start)
	log.Printf("selected 10K times within %v (%.2f selects per second, %v latency, %.0f%% found)",
		took,
		1/(float64(took.Seconds())/selectCount),
		took/selectCount,
		float64(found)/float64(selectCount)*100)
}
