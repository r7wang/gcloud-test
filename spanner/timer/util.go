package timer

import (
	"log"
	"time"
)

// Track keeps track of time taken to run an operation function.
func Track(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
