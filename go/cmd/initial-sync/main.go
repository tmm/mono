package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	lite "github.com/eatonphil/gosqlite"
	"github.com/jackc/pgx/v5/pgconn"
)

const kBatchSize = 50

func copyTable(wg *sync.WaitGroup, lock *sync.Mutex, db string, replica *lite.Conn, table string, columns ...string) {
	r, w := io.Pipe()
	go parseTable(wg, lock, r, replica, table, columns)

	pg, err := pgconn.Connect(context.Background(), db)
	if err != nil {
		log.Fatalf("Connect error: %v", err)
	}

	if err = pg.Exec(context.Background(), "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").Close(); err != nil {
		log.Fatalf("BEGIN error: %v", err)
	}

	_, err = pg.CopyTo(context.Background(), w,
		fmt.Sprintf(`COPY (SELECT %s FROM "%s") TO STDOUT`, strings.Join(columns, ","), table))
	if err != nil {
		log.Fatalf("COPY error: %v", err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("writer close error: %v", err)
	}
	if err = pg.Exec(context.Background(), "COMMIT").Close(); err != nil {
		log.Fatalf("COMMIT error: %v", err)
	}
	if err = pg.Close(context.Background()); err != nil {
		log.Fatalf("pg close error: %v", err)
	}
}

func parseTable(wg *sync.WaitGroup, lock *sync.Mutex, r io.Reader, replica *lite.Conn, table string, columns []string) {
	defer wg.Done()
	start := time.Now()
	numCols := len(columns)
	vals := make([]any, kBatchSize*numCols)
	pos := 0
	rows := 0
	var flushTime time.Duration = 0

	qs := make([]string, numCols)
	for i := range qs {
		qs[i] = "?"
	}

	valuesStr := fmt.Sprintf("(%s)", strings.Join(qs, ","))
	insertStr := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES %s`, table, strings.Join(columns, ","), valuesStr)
	insertBatchStr := fmt.Sprintf("%s%s", insertStr, strings.Repeat(","+valuesStr, kBatchSize-1))

	lock.Lock()
	insertStmt, err := replica.Prepare(insertStr)
	if err != nil {
		log.Fatalf("prepare insert %v", err)
	}
	insertBatchStmt, err := replica.Prepare(insertBatchStr)
	if err != nil {
		log.Fatalf("prepare insert batch %v", err)
	}
	lock.Unlock()

	for lines := bufio.NewScanner(r); lines.Scan(); {
		row := strings.Split(lines.Text(), "\t")
		if len(row) != numCols {
			log.Fatalf("expected %d values in row %s", len(columns), row)
		}
		for i, v := range row {
			if v == "\\N" {
				vals[pos+i] = nil
			} else {
				vals[pos+i] = v
			}
		}
		pos += len(row)
		rows++
		if rows%kBatchSize == 0 {
			s := time.Now()
			lock.Lock()
			if err = insertBatchStmt.Exec(vals...); err != nil {
				log.Fatalf("insert batch %s", err)
			}
			lock.Unlock()
			flushTime += time.Since(s)
			pos = 0
		}
	}
	for i := range rows % kBatchSize {
		s := time.Now()
		lock.Lock()
		if err = insertStmt.Exec(vals[i*numCols : ((i + 1) * numCols)]...); err != nil {
			log.Fatalf("insert %s", err)
		}
		lock.Unlock()
		flushTime += time.Since(s)
	}
	log.Printf("Finished writing %d %s rows (flush: %s) (total: %s)", rows, table, flushTime, time.Since(start))
}

func main() {
	replica, err := lite.Open("/tmp/initial-sync.db")
	if err != nil {
		log.Fatalf("Open error: %v", err)
	}
	if err = replica.Exec(`
	DROP TABLE IF EXISTS issue;
	CREATE TABLE issue (
		id VARCHAR,
		shortID INTEGER,
		title VARCHAR,
		open BOOLEAN,
		modified DOUBLE,
		created DOUBLE,
		creatorID VARCHAR,
		assigneeID VARCHAR,
		description VARCHAR,
		visibility VARCHAR,
		testJson JSONB
	);

	DROP TABLE IF EXISTS comment;
	CREATE TABLE comment (
		id VARCHAR,
		issueID INTEGER,
		created DOUBLE,
		body TEXT,
		creatorID VARCHAR
	);

	DROP TABLE IF EXISTS issueLabel;
	CREATE TABLE issueLabel (
		labelID VARCHAR,
		issueID INTEGER
	);
	`); err != nil {
		log.Fatalf("CREATEs: %v", err)
	}

	if err = replica.Exec(`
	PRAGMA locking_mode = EXCLUSIVE;
	PRAGMA foreign_keys = OFF;
	PRAGMA journal_mode = OFF;
	PRAGMA synchronous = OFF;
	`); err != nil {
		log.Fatalf("PRAGMAS: %v", err)
	}

	db, found := os.LookupEnv("ZERO_UPSTREAM_DB")
	if !found {
		log.Fatalf("No ZERO_UPSTREAM_DB")
	}

	if err = replica.Exec("BEGIN"); err != nil {
		log.Fatalf("BEGIN: %v", err)
	}

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(3)

	var lock sync.Mutex

	go copyTable(&wg, &lock, db, replica, "issue",
		"\"id\"",
		"\"shortID\"",
		"\"title\"",
		"\"open\"",
		"\"modified\"",
		"\"created\"",
		"\"creatorID\"",
		"\"assigneeID\"",
		"\"description\"",
		"\"visibility\"",
		"\"testJson\"",
	)

	go copyTable(&wg, &lock, db, replica, "comment",
		"\"id\"",
		"\"issueID\"",
		"\"created\"",
		"\"body\"",
		"\"creatorID\"",
	)

	go copyTable(&wg, &lock, db, replica, "issueLabel",
		"\"labelID\"",
		"\"issueID\"",
	)

	wg.Wait()

	if err = replica.Exec("COMMIT"); err != nil {
		log.Fatalf("COMMIT: %v", err)
	}
	log.Printf("Copy took %s", time.Since(start))
}
