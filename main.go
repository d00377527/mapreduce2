package main

import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "strings"
    "log"
    "os"
    "io"
    "path/filepath"
    "fmt"
    "net/http"
    "net/url"
)

const (
    database = "austen.db"
    tempDir = "/tmp/output/data"
    outDir = "output"
)

var (
    db_length int
)
func main() {
    _, err := os.Open(database)
    if err != nil {
        log.Fatalf("checking existence of the database: %v", err)
    }
    db, err := openDatabase(database)
    if err != nil {
        log.Fatalf("opening database: %v", err)
    }
    defer db.Close()

    // FIGURE OUT PARTITION LENGTH AND SPLIT
    row := db.QueryRow("SELECT count(key) FROM pairs")
    var db_length int
    _ = row.Scan(&db_length)
    //fmt.Printf("length of db in rows: %d\n", db_length)
    //fmt.Printf("Average partition length in rows: %d\n", db_length/50)
    //fmt.Printf("Remaining rows are in a partition of length: %d\n", db_length-((db_length/50)*50))
    _, err = splitDatabase(database, tempDir, "output-%d.db", 50)
    if err != nil {
        log.Fatalf("splitting db: %v", err)
    }
    fmt.Printf("SUCCESS!\n")

    // SERVER STARTUP
    go func() {
        http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("/tmp/output/data"))))
        if err := http.ListenAndServe("localhost:3410", nil); err != nil {
            log.Printf("Error in HTTP server for %s: %v", "localhost:3410", err)
        }
    }()

    // ITERATE THROUGH SPLIT DIRECTORY AND CREATE URLS
    fileList := make([]string, 0)
	e := filepath.Walk(tempDir, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return err
	})

	if e != nil { // WAYTOODANK
		panic(e)
	}
    var urls []string
	for i, file := range fileList {
		url := strings.Split(file, "/")
        if i == 0 {continue} // SKIP THE DIRECTORY ITSELF
        urls = append(urls, "http://localhost:3410/data/" + url[len(url)-1])
	}
    db, err = mergeDatabases(urls, "new.db", tempDir)
    if err != nil {
        log.Fatalf("merging: %v", err)
    }
    fmt.Printf("SUCCESS!\n")
}

func openDatabase(path string) (*sql.DB, error) {
    options :=
        "?" + "_busy_timeout=10000" +
            "&" + "_case_sensitive_like=OFF" +
            "&" + "_foreign_keys=ON" +
            "&" + "_journal_mode=OFF" +
            "&" + "_locking_mode=NORMAL" +
            "&" + "mode=rw" +
            "&" + "_synchronous=OFF"
    db, err := sql.Open("sqlite3", path+options)
    return db, err
}

func createDatabase(path string) (*sql.DB, error) {
    options :=
        "?" + "_busy_timeout=10000" +
            "&" + "_case_sensitive_like=OFF" +
            "&" + "_foreign_keys=ON" +
            "&" + "_journal_mode=OFF" +
            "&" + "_locking_mode=NORMAL" +
            "&" + "mode=rw" +
            "&" + "_synchronous=OFF"
    if _, err := os.Create(path); err != nil {
        log.Fatalf("creating database file: %v", err)
    }
    db, err := sql.Open("sqlite3", path+options)
    tx, errr := db.Begin()
    if errr != nil {
        log.Fatalf("beginning table create tx: %v", errr)
    }
    _, errr = tx.Exec("CREATE TABLE pairs(key text, value text)")
    if errr != nil {
        log.Fatalf("creating pairs table: %v", errr)
    }
    tx.Commit()
    return db, err
}

func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error) {
    fmt.Printf("splitting %s into %d new files in %s\n", source, m, outputDir)
    var names []string
    var err error
    db, err := openDatabase(source)
    defer db.Close()
    var r = db.QueryRow("SELECT count(key) from pairs")
    var count int
    _ = r.Scan(&count)
    if count < m {
        return names, err
    }
    var partition_length = count / m
    var remainder = count - ((count / m) * m)
    if err != nil {return names, err}
    var splits []*sql.DB
    if err != nil {
        log.Fatalf("opening database for splitting: %v", err)
    }
    rows, err := db.Query("SELECT key, value FROM pairs")
    for i := 1; i <= m; i++ {
        var path = filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))
        names = append(names, path)
        out_db, err := createDatabase(path)
        if err != nil {
            return names, err
        }
        splits = append(splits, out_db)

        var j = 0
        for j := 0; j < partition_length; j++ {
            rows.Next()
            var key, value string
            _ = rows.Scan(&key, &value)
            out_db.Exec("INSERT INTO pairs(key, value) values(?, ?)", key, value)
        }
        if i == 50 { // ON THE LAST ITERATION, DISTRIBUTE THE REMAINING DATA
            for j = 0; j < remainder; j++ {
                rows.Next()
                var key, value string
                _ = rows.Scan(&key, &value)
                splits[j].Exec("INSERT INTO pairs(key, value) values(?, ?)", key, value)
            }
        }

    }
    return names, err
}

func mergeDatabases(urls []string, path, temp string) (*sql.DB, error) {
    fmt.Printf("downloading %d files from %s into %s and merging them into new file %s\n", len(urls), temp, outDir, path)
    db, err := createDatabase(path)
    for _, u := range urls {
        // DOWNLOAD
        file, err := url.Parse(u) // EXTRACT THE URL OBJECT
        filename := file.Path // GET THE ACTUAL FILENAME
        s := strings.Split(filename, "/")
        filename = s[len(s)-1]
        p := filepath.Join("output", filename)
        f, err := os.Create(p)
        if err != nil {
            return db, err
        }
        defer f.Close()
        resp, err := http.Get(u) // GET REQUEST TO SERVER
        _, err = io.Copy(f, resp.Body) // COPY THE RESPONSE BODY TO THE NEW FILE
        resp.Body.Close()
        if err != nil {
            return db, err
        }
        // MERGE
        if err != nil {
            return db, err
        }
        _, err = db.Exec("attach ? as merge; insert into pairs select * from merge.pairs; detach merge", p)
        if err != nil {
            return db, err
        }
        // DELETE
        parts := strings.Split(u, "/")
        err = os.Remove(filepath.Join(tempDir, parts[len(parts)-1]))
        if err != nil {
            return db, err
        }
        err = os.Remove(p)
        if err != nil {
            return db, err
        }
    }
    db.Close()
    return db, err
}
