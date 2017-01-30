package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"log"
	"math/rand"
	"sort"
	"time"
)

var (
	real = flag.Int("real_dice", 20, "Actual dice to use (cropped to formal).")
	dice = flag.Int("dice", 6, "Formal dice to use.")
)

type Context struct {
	Num int `beam:"data"`
}

func Min(ctx Context, in <-chan int, out chan<- int) {
	for elm := range in {
		if ctx.Num < elm {
			out <- ctx.Num
		} else {
			out <- elm
		}
	}
}

func Inc(in <-chan int, out chan<- int) {
	for elm := range in {
		out <- elm + 1
	}
}

func Zero(out chan<- int) {
	out <- 0
}

// roll is a construction-time dice roll. The value is encoded in the shape of
// the pipeline, which will produce a single element of that value.
func roll(p *beam.Pipeline) beam.PCollection {
	num := rand.Intn(*real) + 1

	col, _ := beam.Source(p, Zero)
	for i := 0; i < num; i++ {
		col, _ = beam.ParDo1(p, Inc, col)
	}
	col, _ = beam.ParDo1(p, Min, col, beam.Data{Data: *dice})

	log.Printf("Lucky number %v!", num)
	return col
}

func Tick(out chan<- string) {
	out <- "tick"
}

func eq(n int, other ...int) bool {
	for _, num := range other {
		if num != n {
			return false
		}
	}
	return true
}

// Eval takes 5 dice rolls as side input.
func Eval(_ <-chan string, a, b, c, d, e <-chan int) {
	r := []int{<-a, <-b, <-c, <-d, <-e}
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

func build(p *beam.Pipeline) error {
	tick, _ := beam.Source(p, Tick)
	return beam.ParDo0(p, Eval, tick,
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
		beam.SideInput{Input: roll(p)},
	)
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	p := beam.NewPipeline()
	if err := build(p); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := local.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
