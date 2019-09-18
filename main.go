package main

// https://medium.com/smsjunk/handling-1-million-requests-per-minute-with-golang-f70ac505fcaa
import (
	"conc/logger"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bwmarrin/snowflake"

	_ "github.com/go-sql-driver/mysql"
)

type (
	// Payload ..
	Payload struct {
		id string
		db *sql.DB
	}
	// Job ..,
	Job struct {
		Payload Payload
	}

	// Worker ...
	Worker struct {
		id         int
		WorkerPool chan chan Job
		JobChannel chan Job
		quit       chan bool
	}

	dispatcher struct {
		wpool      chan chan Job
		maxWorkers int
	}
)

var (
	log       *logger.Log
	jobQueue  chan Job
	dbCon     *sql.DB
	snowNode  *snowflake.Node
	maxWorker = os.Getenv("MAX_WORKER")
	maxQueue  = os.Getenv("MAX_QUEUE")
	dsn       = "fakeaccount:fakepassword!@tcp(172.20.10.30)/fakedb?charset=utf8mb4"
)

// Save ...
func (p Payload) Save(workerID int) error {
	log.Infof("(%s) [%d] told to save", p.id, workerID)

	_, err := p.db.Exec("INSERT INTO `concurency` (`name`) VALUES (?)", p.id)
	if err != nil {
		return err
	}

	log.Infof("(%s) [%d] Saving %s", p.id, workerID, p.id)

	return nil
}

func newDispatcher(maxWorker int) *dispatcher {
	pool := make(chan chan Job, maxWorker)
	return &dispatcher{wpool: pool, maxWorkers: maxWorker}
}

func (d *dispatcher) dispatch() {
	for {
		select {
		case job := <-jobQueue:
			go func(job Job) {
				jobChannel := <-d.wpool
				jobChannel <- job
			}(job)
		}
	}
}

func (d *dispatcher) run() {
	log.Infof("Running dispatcher for %d worker", d.maxWorkers)
	for i := 0; i < d.maxWorkers; i++ {
		worker := newWorker(d.wpool, i)
		log.Infof("running worker #%d", i)
		worker.Start()
	}

	go d.dispatch()
}

func newWorker(wpool chan chan Job, workerNum int) Worker {
	return Worker{
		id:         workerNum,
		WorkerPool: wpool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

// Start ...
func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				log.Infof("(%s) [w%d] Got job!", job.Payload.id, w.id)
				if err := job.Payload.Save(w.id); err != nil {
					log.Infof("(%s) [w%d]  error: %s", job.Payload.id, w.id, err.Error())
				} else {
					log.Infof("(%s) [w%d]  Success!", job.Payload.id, w.id)
				}
				fmt.Println("============================================")
			case <-w.quit:
				log.Infof("[w%d] told to quit", w.id)
				return
			}
		}
	}()
}

func (w Worker) stop() {
	go func() {
		w.quit <- true
	}()
}

func index(w http.ResponseWriter, r *http.Request) {
	uid := snowNode.Generate().String()
	log.Infof("(%s) got incoming request", uid)

	var p = Job{
		Payload: Payload{
			id: uid,
			db: dbCon,
		},
	}

	jobQueue <- p

	w.WriteHeader(http.StatusOK)
}

func init() {
	l, err := logger.New(logger.Config{
		Level:      logger.DebugLevel,
		Fileoutput: "/home/wid/Development/golang/conc/all.log",
	})

	if err != nil {
		panic(err)
	}

	log = l

	maxQueue, err := strconv.Atoi(maxQueue)
	if err != nil {
		maxQueue = 100
	}

	log.Infof("Running with max queue: %d", maxQueue)
	jobQueue = make(chan Job, maxQueue)

	maxWorker, err := strconv.Atoi(maxWorker)
	if err != nil {
		maxWorker = 100
	}

	node, err := snowflake.NewNode(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	snowNode = node

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println(err)
		return
	}

	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(maxWorker)
	db.SetConnMaxLifetime(time.Minute * 5)
	dbCon = db

	disp := newDispatcher(maxWorker)
	disp.run()
}

func main() {
	http.HandleFunc("/", index)
	log.Info("listening on :2000")
	if err := http.ListenAndServe("127.0.0.1:2000", nil); err != nil {
		panic(err)
	}
}
