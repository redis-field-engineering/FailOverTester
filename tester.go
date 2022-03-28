package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/go-redis/redis/v8"
	"go.uber.org/ratelimit"
)

var ctx = context.Background()

func errHndlr(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

type WriteLog struct {
	Timestamp time.Duration
	Job       int
	Client    int
}

type RedisConfig struct {
	Host     string
	Password string
	Port     int
	Conn     net.Conn
	Logfile  log.Logger
}

func worker(id int, jobs <-chan int, results chan<- WriteLog, rl ratelimit.Limiter, conf RedisConfig) {

	redisClient := redis.NewClient(&redis.Options{
		Dialer:          conf.randomDialer, // Randomly pick an IP address from the list of ips retruned
		Password:        args.RedisPassword,
		MinRetryBackoff: 1 * time.Millisecond, //minimum amount of time to try and backoff
		MaxRetryBackoff: 50 * time.Millisecond,
		MaxConnAge:      0,    //3 * time.Second this will cause everyone to reconnect every 3 seconds - 0 is keep open forever
		MaxRetries:      1000, // retry 10 times : automatic reconnect if a proxy is killed
	})

	for j := range jobs {
		rl.Take()
		startTime := time.Now()
		_, err := redisClient.Ping(ctx).Result()
		errHndlr(err)
		results <- WriteLog{
			Timestamp: time.Since(startTime),
			Job:       j,
			Client:    id,
		}
	}
}

func (conf RedisConfig) randomDialer(context.Context, string, string) (net.Conn, error) {
	ips, reserr := net.LookupIP(conf.Host)
	if reserr != nil {
		return nil, reserr
	}

	sort.Slice(ips, func(i, j int) bool {
		return bytes.Compare(ips[i], ips[j]) < 0
	})

	n := rand.Int() % len(ips)

	conf.Logfile.Printf("Dialing: %s:%d\n", ips[n].String(), conf.Port)

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ips[n], conf.Port))
	return conn, err
}

var args struct {
	RedisServer   string `help:"Redis to connect to" default:"localhost" arg:"--redis-host, -s, env:REDIS_SERVER"`
	RedisPort     int    `help:"Redis port to connect to" default:"6379" arg:"--redis-port, -p, env:REDIS_PORT"`
	RedisPassword string `help:"Redis password" default:"" arg:"--redis-password, -a, env:REDIS_PASSWORD"`
	MessageCount  int    `help:"Number of writes" default:"100000" arg:"--writes, -w, env:REDIS_WRITES"`
	ClientCount   int    `help:"Number of clients" default:"10" arg:"--clients, -c, env:REDIS_CLIENTS"`
	RateLimit     int    `help:"Number of writes/second" default:"1000" arg:"--rate-limit, -r, env:REDIS_RATE_LIMIT"`
	LogFile       string `help:"Where to Log" default:"" arg:"--logfile, -l, env:REDIS_LOGFILE"`
	OutFile       string `help:"Where to write" default:"results.csv" arg:"--out-file, -o, env:REDIS_OUTFILE"`
}

func main() {
	arg.MustParse(&args)

	rconf := RedisConfig{
		Host:     args.RedisServer,
		Password: args.RedisPassword,
		Port:     args.RedisPort,
	}

	if args.LogFile == "" {
		rconf.Logfile.SetOutput(os.Stdout)
	} else {

		file, err := os.OpenFile(args.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		rconf.Logfile.SetOutput(file)
	}

	rconf.Logfile.SetFlags(log.Lmicroseconds)

	rl := ratelimit.New(args.RateLimit)

	jobs := make(chan int, args.MessageCount)
	results := make(chan WriteLog, args.MessageCount)

	for w := 0; w < args.ClientCount; w++ {
		go worker(w, jobs, results, rl, rconf)
	}

	for j := 0; j <= args.MessageCount-1; j++ {
		jobs <- j
	}
	close(jobs)

	f, err := os.Create(args.OutFile)
	defer f.Close()
	if err != nil {

		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(f)

	if err := w.Write([]string{"duration(μs)", "message", "client"}); err != nil {
		log.Fatalln("error writing record to file", err)
	}

	for a := 0; a <= args.MessageCount-1; a++ {
		r := <-results
		if err := w.Write([]string{
			fmt.Sprintf("%d", r.Timestamp.Microseconds()),
			fmt.Sprintf("%d", r.Job),
			fmt.Sprintf("%d", r.Client),
		}); err != nil {
			log.Fatalln("error writing record to file", err)
		}

	}
	w.Flush()
	os.Exit(0)

}
