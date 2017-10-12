package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	real = flag.Int("real_dice", 20, "Actual dice to use (cropped to formal).")
	dice = flag.Int("dice", 6, "Formal dice to use.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*minFn)(nil)).Elem())
}

// roll is a construction-time dice roll. The value is encoded in the shape of
// the pipeline, which will produce a single element of that value.
func roll(ctx context.Context, p *beam.Pipeline) beam.PCollection {
	num := rand.Intn(*real) + 1
	log.Debugf(ctx, "Lucky number %v!", num)

	p = p.Composite(fmt.Sprintf("roll[%v]", num))

	col := beam.Create(p, 0)
	for i := 0; i < num; i++ {
		col = beam.ParDo(p, incFn, col)
	}
	return beam.ParDo(p, minFn{Num: *dice}, col)
}

type minFn struct {
	Num int `json:"num"`
}

func (m minFn) ProcessElement(num int) int {
	if m.Num < num {
		return m.Num
	}
	return num
}

func incFn(num int) int {
	return num + 1
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
func evalFn(ctx context.Context, _ []byte, a, b, c, d, e int) {
	r := []int{a, b, c, d, e}
	sort.Ints(r)

	log.Infof(ctx, "Roll: %v", r)

	switch {
	case eq(r[0], r[1], r[2], r[3], r[4]):
		log.Info(ctx, "Yatzy!")
	case eq(r[0], r[1], r[2], r[3]) || eq(r[1], r[2], r[3], r[4]):
		log.Info(ctx, "Four of a kind!")
	case eq(r[0], r[1], r[2]) && r[3] == r[4], r[0] == r[1] && eq(r[2], r[3], r[4]):
		log.Info(ctx, "Full house!")
	case r[0] == 1 && r[1] == 2 && r[2] == 3 && r[3] == 4 && r[4] == 5:
		log.Info(ctx, "Small straight!")
	case r[0] == 2 && r[1] == 3 && r[2] == 4 && r[3] == 5 && r[4] == 6:
		log.Info(ctx, "Big straight!")
	default:
		log.Info(ctx, "Sorry, try again.")
	}
}

func main() {
	flag.Parse()
	beam.Init()

	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()

	log.Info(ctx, "Running yatzy")

	// Construct a construction-time-randomized pipeline.
	p := beam.NewPipeline()
	beam.ParDo0(p, evalFn, beam.Impulse(p),
		beam.SideInput{Input: roll(ctx, p)},
		beam.SideInput{Input: roll(ctx, p)},
		beam.SideInput{Input: roll(ctx, p)},
		beam.SideInput{Input: roll(ctx, p)},
		beam.SideInput{Input: roll(ctx, p)},
	)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
