package generator

import (
	"log"
	"sync"
	"time"
    "github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
    "crdb-authzed-load-test/internal/authzed"
    "crdb-authzed-load-test/internal/config"
    "crdb-authzed-load-test/internal/metrics"
)

type fakeTuple struct {
	SubjectID string `fake:"{firstname}"`
	Relation  string `fake:"{randomstring:[editor,viewer,admin]}"`
	Permission string `fake:"{randomstring:[view,edit,remove]}"`
}

type channelTuple struct {
	SubjectType string
	SubjectID string
	Relation string
	ObjectType string
	ObjectID string
}

func RunWorkload(dryRun bool) {
	cfg := config.AppConfig.Workload
	duration := time.Duration(cfg.DurationSec) * time.Second
	endTime := time.Now().Add(duration)
    gofakeit.Seed(0)

	writeWorkers := 1
	readWorkers := cfg.ReadRatio
	totalWorkers := writeWorkers + readWorkers

	var wg sync.WaitGroup
	tupleChannel := make(chan channelTuple, 10000)

	var allowedCount, deniedCount, failedReads, failedWrites, readCount, writeCount int64

	log.Printf("🚧 Load generation for %v with %d total workers (%d writers, %d readers)...",
		duration, totalWorkers, writeWorkers, readWorkers)

	// Phase 1: Start write worker(s)
	for i := 0; i < writeWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for time.Now().Before(endTime) {
                // Pass your struct as a pointer
                var f fakeTuple
                gofakeit.Struct(&f)
                objectType := "document"
                objectID := "doc_" + uuid.New().String()
                subjectType := "user"
				if !dryRun {
					err := authzed.WriteTuple(objectType, objectID, f.Relation, subjectType, f.SubjectID)
					if err != nil {
						log.Printf("❌ WriteTuple failed: %v", err)
						failedWrites++
					} else {
						// Push the same tuple read_ratio times
						for j := 0; j < cfg.ReadRatio; j++ {
							tupleChannel <- channelTuple{ObjectType: objectType, ObjectID: objectID, Relation: f.Relation, SubjectType: subjectType, SubjectID: f.SubjectID}
						}
						writeCount++
					}
				}
			}
		}(i)
	}

	// Phase 2: Start read workers
	for i := 0; i < readWorkers; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for time.Now().Before(endTime) {
				select {
				case t := <-tupleChannel:
					allowed := false
					var err error
					if !dryRun {
                        var f fakeTuple
                        gofakeit.Struct(&f)
						allowed, err = authzed.CheckPermission(t.ObjectType, t.ObjectID, f.Permission, t.SubjectType, t.SubjectID)
						log.Printf("🔒 Checking Permission '%s' for %s '%s' (%s) to %s '%s', allowed=%v", f.Permission, t.SubjectType, t.SubjectID, t.Relation, t.ObjectType, t.ObjectID, allowed)
					    if err != nil {
                            failedReads++
                        }
					}

					if allowed {
						metrics.PermissionCheckCounter.WithLabelValues("allowed").Inc()
						allowedCount++
					}
                    if !allowed && err == nil {
						metrics.PermissionCheckCounter.WithLabelValues("denied").Inc()
						deniedCount++
					}
					readCount++
				default:
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
	log.Println("🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧")
	log.Println("✅  AuthZed Load generation and permission checks complete")
	log.Printf("⏱️  Duration:              %v", duration)
	log.Printf("⚙️  Concurrency:           %d", totalWorkers)
	log.Printf("🚦 Checks/sec:            %.1f", float64(readCount)/float64(cfg.DurationSec))
	log.Printf("🧪 Mode:                  %s", map[bool]string{true: "DRY RUN", false: "LIVE"}[dryRun])
	log.Printf("✔️  Allowed:               %d", allowedCount)
	log.Printf("🚫 Denied:                %d", deniedCount)
	log.Printf("✏️  Writes:                %d", writeCount)
	log.Printf("👁️  Reads:                 %d", readCount)
	if writeCount > 0 {
	    log.Printf("📊 Read/Write ratio:      %.1f:1", float64(readCount)/float64(writeCount))
	}
	log.Printf("🚨 Failed writes to AuthZed: %d", failedWrites)
	log.Printf("🚨 Failed reads to AuthZed:  %d", failedReads)

	if dryRun {
		log.Println("⚠️  Dry-run mode: No tuples were written to AuthZed.")
	}

	log.Println("🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧")
}
