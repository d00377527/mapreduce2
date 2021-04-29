package main

import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "strings"
    "log"
    "os"
    "io"
    "io/fs"
    "path/filepath"
    "fmt"
    "net/http"
    "net/url"
    "hash/fnv"
    "unicode"
    "strconv"
)

const (
    URL = "http://localhost:1337/data/"
)
type MapTask struct {
    M, R        int
    N           int
    SourceHost  string
}

type ReduceTask struct {
    M, R        int
    N           int
    SourceHosts  []string
}

type Pair struct {
    Key     string
    Value   string
}

type Interface interface {
    Map(key, value string, output chan<- Pair) error
    Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string {return fmt.Sprintf("map_%d_source.db", m)}
func mapInputFile(m int) string {return fmt.Sprintf("map_%d_input.db", m)}
func mapOutputFile(m, r int) string {return fmt.Sprintf("map_%d_output_%d.db", m, r)}
func reduceInputFile(r int) string {return fmt.Sprintf("reduce_%d_input.db", r)}
func reduceOutputFile(r int) string {return fmt.Sprintf("reduce_%d_output.db", r)}
func reducePartialFile(r int) string {return fmt.Sprintf("reduce_%d_partial.db", r)}
func reduceTempFile(r int) string {return fmt.Sprintf("reduce_%d_temp.db", r)}
func makeURL(host, file string) string {return fmt.Sprintf("http://%s/data/%s", host, file)}

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
    var r = db.QueryRow("SELECT count(key) from pairs limit 1000")
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
    for i := 0; i < m; i++ {
        var path = filepath.Join(outputDir, fmt.Sprintf(outputPattern, i))
        fmt.Printf("path: %s\n", path)
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
    //fmt.Printf("downloading %d files from %s into %s and merging them into new file %s\n", len(urls), temp, outDir, path)
    db, err := createDatabase(path)
    for _, u := range urls {
        // DOWNLOAD
        file, err := url.Parse(u) // EXTRACT THE URL OBJECT
        filename := file.Path // GET THE ACTUAL FILENAME
        s := strings.Split(filename, "/")
        filename = s[len(s)-1]
        p := filepath.Join(temp, filename)
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
        _, err = db.Exec("attach ? as merge; insert into pairs select * from merge.pairs; detach merge", p)
        if err != nil {
            return db, err
        }
        // DELETE
        parts := strings.Split(u, "/")
        err = os.Remove(filepath.Join(temp, parts[len(parts)-1]))
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


func download_map_input_file(n int, source, input, tempdir string) (string, error) {
    path := filepath.Join(tempdir, input)
    f, err := os.Create(path)
    if err != nil {
        return path, err
    }
    defer f.Close()
    resp, err := http.Get(source)
    if err != nil {
        return path, err
    }
    _, err = io.Copy(f, resp.Body)
    if err != nil {
        return path, err
    }
    resp.Body.Close()
    return path, err
}

func (task *MapTask) Process(tempdir string, client Interface, is_routine bool, used_routines *int) error {
    u := makeURL(task.SourceHost, mapSourceFile(task.N))
    path, err := download_map_input_file(task.N, u, mapInputFile(task.N), tempdir)
    //fmt.Printf("path for input file: %s\n", path)
    if err != nil {
        return err
    }
    db, err := openDatabase(path)
    if err != nil {
        return err
    }
    for r := 0; r < task.R; r++ {
        out_db, err := createDatabase(filepath.Join(tempdir, mapOutputFile(task.N, r)))
        _, _ = out_db.Exec("CREATE TABLE pairs(key text, value text)")
        if err != nil {
            return err
        }
    }
    pairs, err := db.Query("SELECT key, value FROM pairs")
    if err != nil {
        return err
    }
    count := 0
    for pairs.Next() {
        var k, v string
        pairs.Scan(&k, &v)
        pair := Pair{Key: k, Value: v}
        output := make(chan Pair)
        finishedMap := make(chan error)
        go task.writeOutput(output, finishedMap, tempdir, &count)
        if err := client.Map(pair.Key, pair.Value, output); err != nil {
            return fmt.Errorf("Issue with client map: %v", err)
        }
        if err := <-finishedMap; err != nil {
            return fmt.Errorf("Issue writing output: %v", err)
        }
    }
    db.Close()
    if is_routine == true {
        *used_routines -= 1;
        fmt.Printf("task %d is done and there are now %d used goroutines\n", task.N, *used_routines)
    } else {
        fmt.Printf("task %d is done but did not use a goroutine\n", task.N)
    }
    return err
}
/*
func (task *ReduceTask) Process(tempdir string, client Interface) error {
    var urls []string
    // for each source host, make urls for each of the reduce tasks from them. NOTE: IMPOSSIBLE
    path := filepath.Join(tempdir, reduceInputFile(task.N))

    db, err := mergeDatabases(urls, path, tempdir)
    if err != nil {
        return err
    }
    out_db, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.N)))
    if err != nil {
        return err
    }

    pairs, err := db.Query("SELECT key, value, from pairs order by key, value")
    if err != nil {
        return err
    }
    input := make(chan string)
    output := make(chan Pair)
    pairs.Next() // handle first key
    var previous Pair
    pairs.Scan(previous.Key, previous.Value)
    err = client.Reduce(previous.Key, input, output)
    if err != nil {
        return err
    }
    for pairs.Next() { // handle all other keys
        var pair Pair
        pairs.Scan(pair.Key, pair.Value)
        if pair.Key != previous.Key {
            close(input)
            // and then what???
            err = client.Reduce(pair.Key, input, output)
            if err != nil {
                return err
            }
        } else {
            continue
        }
    }
    for p := range output {
        out_db.Exec("INSERT INTO pairs(key, value) values(?,?)", p.Key, p.Value)
    }
    close(input)
    close(output)
    out_db.Close()
    db.Close()
    return err
}*/

type KeySet struct {
  Key string
  Input <-chan string
}


func (task *ReduceTask) Process(tempdir string, client Interface) error {
  // This method processes a single reduce task. It must:


  // stores input files into a url slice of strings
  urls := make([]string, task.M)
  for i := 0; i < task.M; i++ {
    urls[i] = makeURL(task.SourceHosts[i], mapInputFile(i))
  }


  // Create the input database by merging all of the appropriate output databases from the map phase
  inputDB, err := mergeDatabases(urls, filepath.Join(tempdir, reduceInputFile(task.N)), "tmp.db")
  // check the merge error
  if err != nil {
    return fmt.Errorf("issue merging database: %v", err)
  }

  defer inputDB.Close()

  // Create the output database
  outputDB, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.N)))
  if err != nil {
    return fmt.Errorf("issue creating output database: %v", err)
  }
  defer outputDB.Close()

  // create the output statements
  outputStatements, err := outputDB.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
  if err != nil {
    return fmt.Errorf("issue with the prepare insert: %v", err)
  }
  defer outputStatements.Close()
  // Process all pairs in the correct order
  i, v, j := 0,0,0

  rows, err := inputDB.Query("SELECT key, value FROM pairs ORDER BY key, value")
  if err != nil {
    return fmt.Errorf("issue querying input db: %v", err)
  }
  defer rows.Close()

  KeySets := make(chan KeySet)
  readDone, writeDone := make(chan error), make(chan error)
  //As you loop over the key/value pairs,
  //take note whether the key for a new row is the same or
  //different from the key of the previous row.
  go readInput(rows, KeySets, readDone, &v)

  for set := range KeySets {
    i++
    reduceOutput := make(chan Pair, 100)

    go task.writeOutput(reduceOutput, writeDone, outputStatements, &j)
    if err := client.Reduce(set.Key, set.Input, reduceOutput); err != nil {
      return fmt.Errorf("issue with client reduce: %v", err)
    }
  }
  log.Printf("Map Task %d processed %d pairs and generated %d pairs\n", task.N, i, v, j)

  return nil
}

func (task *MapTask) writeOutput(output chan Pair, finishedMap chan<- error, tempdir string, count *int) {
    for pair := range output {
      *count++
      if *count % 10000 == 0 {
          //fmt.Printf("task %d reached %d\n", task.N, *count)
      }
      hash := fnv.New32()
      hash.Write([]byte(pair.Key))
      r := int(hash.Sum32() % uint32(task.R))
      out_db, err := openDatabase(filepath.Join(tempdir, mapOutputFile(task.N, r)))
      defer out_db.Close()
      if err != nil {
          log.Fatalf("wtf is this/n")
      }
      if _, err = out_db.Exec("INSERT INTO pairs values(?, ?)", pair.Key, pair.Value); err != nil {
          finishedMap <- fmt.Errorf("issue inserting: %v", err)
          return
      }
    }
    finishedMap <- nil
}

func (task *ReduceTask) writeOutput(output <-chan Pair, finishedReduce chan<- error, stmt *sql.Stmt, count *int) {
  for pair := range output {
    *count++
    if _, err := stmt.Exec(pair.Key, pair.Value); err != nil {
      finishedReduce <- fmt.Errorf("issue inserting: %v", err)
      return
    }
  }
  finishedReduce <- nil
}

// send reading input database to the reduce function on the client

// a lot of this function is part of what we discussed with the lecture today
func readInput(rows *sql.Rows, setChannel chan<- KeySet, finishedReduce chan error, valueCount *int) {
  var erwar error
  var previousKey string
  var current chan string

  defer func() {
    close(setChannel)
    finishedReduce <- erwar
  }()

  for rows.Next() {
    // check for same and different keys through looping through them
    var key, value string
    if err := rows.Scan(&key, &value); err != nil {
      if previousKey != "" {
        close(current)
      }
      erwar = fmt.Errorf("issue reading a row from the input database: %v", err)
      return
    }

    // check if keys have changed
    if previousKey != key {
      if previousKey != "" {
        // Close call if it isn't the first key
        close(current)
        // wait for the output to finish
        if err := <-finishedReduce; err != nil {
          return
        }
      }
      // initiate a new key set
      current = make(chan string, 100)
      setChannel <- KeySet {
        Key: key,
        Input: current,
      }
    }
    *valueCount++
    current <- value
    previousKey = key
  }
  // check for errors in rows, just like how we did before
  if err := rows.Err(); err != nil {
		if previousKey != "" {
			close(current)
		}
		erwar = fmt.Errorf("issue on rows of downloaded db: %v", err)
	}

	// close last call
	close(current)

	// Wait for output to finish
	if err := <-finishedReduce; err != nil {
		return
	}
}

func main() {
    var m = 50
    var r = 3
    os.Chmod(os.TempDir()+"/data", 0777)
    tempdir := filepath.Join(os.TempDir()+"/data", fmt.Sprintf("mapreduce.%d", os.Getpid()))
    os.RemoveAll(tempdir)
    err := os.Mkdir(tempdir, fs.ModePerm)
    if err != nil {
        log.Fatalf("mkdir: %v", err)
    }
    fmt.Printf("tempdir: %s", tempdir)
    //defer os.RemoveAll(tempdir) // REMOVE IT LATER I GUESS
    address := "localhost:1337"
    go func() {
        http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
        if err := http.ListenAndServe(address, nil); err != nil {
                log.Printf("Error in HTTP server for %s: %v", address, err)
        }
    }()
    _, _ = splitDatabase("austen.db", tempdir, "map_%d_source.db", m) // SPLIT INTO /TMP/DATA/

    var client = Client{}
    used_routines := new(int)
    *used_routines = 0
    for i := 0; i < m; i++ {
        task := MapTask{M: m, R: r, N: i, SourceHost: address}
        if *used_routines < 4 && i != m-1 {
            *used_routines += 1
            go task.Process(tempdir, client, true, used_routines)
            fmt.Printf("processing task %d. there are %d more goroutines available\n", i, 4 - *used_routines)
        } else {
            fmt.Printf("processing task %d without using a goroutine\n", i)
            task.Process(tempdir, client, false, used_routines)
        }

    }
    for *used_routines > 0 {
        continue
    }
    fmt.Printf("Finished mapping\n")
}


type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
    lst := strings.Fields(value)
    for _, elt := range lst {
        word := strings.Map(func(r rune) rune {
            if unicode.IsLetter(r) || unicode.IsDigit(r) {
                    return unicode.ToLower(r)
            }
            return -1
        }, elt)
        if len(word) > 0 {
            output <- Pair{Key: word, Value: "1"}
        }
    }
    close(output)
    return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
    defer close(output)
    count := 0
    for v := range values {
        i, err := strconv.Atoi(v)
        if err != nil {
            return err
        }
        count += i
    }
    p := Pair{Key: key, Value: strconv.Itoa(count)}
    output <- p
    return nil
}
