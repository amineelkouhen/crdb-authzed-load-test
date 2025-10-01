package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
	"crdb-authzed-load-test/cmd/generator"
	"crdb-authzed-load-test/internal/config"
	"crdb-authzed-load-test/internal/metrics"
)

var createSchemaResponse map[string]interface{}

func main() {
	duration := flag.Int("duration-sec", 0, "Override duration in seconds")
    readRatio := flag.Int("read-ratio", 0, "Override read/write ratio (e.g. 100 = 100:1)")
	dryRun := flag.Bool("dry-run", false, "Simulate workload without API calls")
	workloadConfig := flag.String("workload-config", "config/config.yaml", "Path to workload config")
	logFile := flag.String("log-file", "", "Path to log output file")
	serveMetrics := flag.Bool("serve-metrics", false, "Keep Prometheus metrics endpoint alive after run")
	verbose := flag.Bool("verbose", true, "Enable verbose logging")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
📦 crdb-authzed-load-test: Workload simulator for AuthZed + CockroachDB

Usage:
  ./crdb-authzed-load-test [flags]

Options:
  -checks-per-second   Max permission checks per second (overrides config file)
  -duration-sec        Run for this many seconds (default from config file)
  -read-ratio          Read-to-write ratio (e.g. 100 means 100 reads per 1 write)
  -workload-config     Path to workload config file (default: config/config.yaml)
  -log-file            Path to write logs to (default: stdout only)
  -serve-metrics       Keep Prometheus metrics endpoint alive after run (default: false)
  -dry-run             Skip actual writes and permission checks
  -help                Show this help message

🔒 This tool assumes AuthZed + CockroachDB Sandbox is deployed and reachable.
📖 See install docs: https://github.com/amineelkouhen/crdb-authzed-sandbox/?tab=readme-ov-file#-deployment
`)
	}

	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(0)
	}

	if err := config.LoadConfig(*workloadConfig); err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

    if *duration > 0 {
        config.AppConfig.Workload.DurationSec = *duration
    }
	if *readRatio > 0 {
		config.AppConfig.Workload.ReadRatio = *readRatio
	}

	if *logFile != "" {
		f, err := os.Create(*logFile)
		if err != nil {
			log.Fatalf("❌ Failed to create log file: %v", err)
		}
		defer f.Close()

		if *verbose {
			log.SetOutput(io.MultiWriter(os.Stdout, f))
		} else {
			log.SetOutput(f)
		}
	} else if !*verbose {
		log.SetOutput(io.Discard)
	}

    if !*dryRun {
        check()
    }
    metrics.Init()
    generator.RunWorkload(*dryRun)


	if *serveMetrics {
		fmt.Println("📊 Prometheus metrics available at http://localhost:2112/metrics")
		fmt.Println("🔁 Waiting indefinitely for Prometheus to scrape. Ctrl+C to exit.")
		select {}
	}
}

func check(){
    if config.AppConfig.AuthZed.API == nil {
        log.Fatalf("❌ AuthZed Endpoint is Missing")
        os.Exit(-1)
    }

    if config.AppConfig.AuthZed.Key == nil {
        log.Fatalf("❌ AuthZed Auth Bearer (Pre-shared) Key is Missing")
        os.Exit(-1)
    }

    healthURL := *config.AppConfig.AuthZed.API + "/healthz"
    client := http.Client{Timeout: 3 * time.Second}
    resp, err := client.Get(healthURL)
    if err != nil || resp.StatusCode != 200 {
        log.Fatalf(`❌ Unable to reach AuthZed API at %s.

        Make sure AuthZed API is running and reachable.
        Refer to: https://authzed.com/blog/authzed-http-api

        Details:
        - Error: %v
        - HTTP Status: %v
        `, config.AppConfig.AuthZed.API, err, resp.StatusCode)
    }
}
