package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"log"
	"math/rand"
	"sort"
	"time"
)

var (
	real = flag.Int("real_dice", 20, "Actual dice to use (cropped to formal).")
	dice = flag.Int("dice", 6, "Formal dice to use.")
)

// roll is a construction-time dice roll. The value is encoded in the shape of
// the pipeline, which will produce a single element of that value.
func roll(p *beam.Pipeline) beam.PCollection {
	num := rand.Intn(*real) + 1

	p = p.Composite(fmt.Sprintf("roll[%v]", num))

	col := beam.Source(p, zeroFn)
	for i := 0; i < num; i++ {
		col = beam.ParDo(p, incFn, col)
	}
	col = beam.ParDo(p, minFn, col, beam.Data{Data: *dice})

	log.Printf("Lucky number %v!", num)
	return col
}

type minOpt struct {
	Num int `beam:"opt"`
}

func minFn(opt minOpt, num int) int {
	if opt.Num < num {
		return opt.Num
	}
	return num
}

func incFn(num int) int {
	return num + 1
}

func zeroFn(emit func(int)) {
	emit(0)
}

func eq(n int, other ...int) bool {
	for _, num := range other {
		if num != n {
			return false
		}
	}
	return true
}

// evalFn takes 5 dice rolls as singleton side inputs.
func evalFn(_ string, a, b, c, d, e int) {
	r := []int{a, b, c, d, e}
	sort.Ints(r)

	log.Printf("Roll: %v", r)

	switch {
	case eq(r[0], r[1], r[2], r[3], r[4]):
		log.Print("Yatzy!")
	case eq(r[0], r[1], r[2], r[3]) || eq(r[1], r[2], r[3], r[4]):
		log.Print("Four of a kind!")
	case eq(r[0], r[1], r[2]) && r[3] == r[4], r[0] == r[1] && eq(r[2], r[3], r[4]):
		log.Print("Full house!")
	case r[0] == 1 && r[1] == 2 && r[2] == 3 && r[3] == 4 && r[4] == 5:
		log.Print("Small straight!")
	case r[0] == 2 && r[1] == 3 && r[2] == 4 && r[3] == 5 && r[4] == 6:
		log.Print("Big straight!")
	default:
		log.Print("Sorry, try again.")
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	rand.Seed(time.Now().UnixNano())

	log.Print("Running yatzy")

	// Construct a construction-time-randomized pipeline.
	p := beam.NewPipeline()
	beam.ParDo0(p, evalFn, debug.Tick(p),
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
	)

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
