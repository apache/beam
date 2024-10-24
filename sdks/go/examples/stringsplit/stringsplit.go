// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// An example of using a Splittable DoFn in the Go SDK with a portable runner.
//
// The following instructions describe how to execute this example in the
// Flink local runner.
//
// 1. From a command line, navigate to the top-level beam/ directory and run
// the Flink job server:
//
//	./gradlew :runners:flink:1.19:job-server:runShadow -Djob-host=localhost -Dflink-master=local
//
// 2. The job server is ready to receive jobs once it outputs a log like the
// following: `JobService started on localhost:8099`. Take note of the endpoint
// in that log message.
//
// 3. While the job server is running in one command line window, create a
// second one in the same directory and run this example with the following
// command, using the endpoint you noted from step 2:
//
//	go run sdks/go/examples/stringsplit/stringsplit.go --runner=universal --endpoint=localhost:8099
//
// 4. Once the pipeline is complete, the job server can be closed with ctrl+C.
// To check the output of the pipeline, search the job server logs for the
// phrase "StringSplit Output".
package main

// beam-playground:
//   name: StringSplit
//   description: An example of using a Splittable DoFn in the Go SDK with a portable runner.
//   multifile: false
//   context_line: 154
//   categories:
//     - Debugging
//     - Flatten
//   complexity: MEDIUM
//   tags:
//     - pipeline
//     - split
//     - runner

import (
	"context"
	"flag"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	register.DoFn4x0[context.Context, *sdf.LockRTracker, string, func(string)](&StringSplitFn{})
	register.DoFn2x0[context.Context, string](&LogFn{})
	register.Emitter1[string]()
}

// StringSplitFn is a Splittable DoFn that splits strings into substrings of the
// specified size (for example, to be able to fit them in a small buffer).
// See ProcessElement for more details.
type StringSplitFn struct {
	BufSize int64
}

// CreateInitialRestriction creates an offset range restriction for each element
// with the size of the restriction corresponding to the length of the string.
func (fn *StringSplitFn) CreateInitialRestriction(s string) offsetrange.Restriction {
	rest := offsetrange.Restriction{Start: 0, End: int64(len(s))}
	log.Debugf(context.Background(), "StringSplit CreateInitialRestriction: %v", rest)
	return rest
}

// SplitRestriction performs initial splits so that each restriction is split
// into 5.
func (fn *StringSplitFn) SplitRestriction(_ string, rest offsetrange.Restriction) []offsetrange.Restriction {
	splits := rest.EvenSplits(5)
	log.Debugf(context.Background(), "StringSplit SplitRestrictions: %v -> %v", rest, splits)
	return splits
}

// RestrictionSize returns the size as the difference between the restriction's
// start and end.
func (fn *StringSplitFn) RestrictionSize(_ string, rest offsetrange.Restriction) float64 {
	size := rest.Size()
	log.Debugf(context.Background(), "StringSplit RestrictionSize: %v -> %v", rest, size)
	return size
}

// CreateTracker creates an offset range restriction tracker out of the offset
// range restriction, and wraps it a thread-safe restriction tracker.
func (fn *StringSplitFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// ProcessElement splits a string into substrings of a specified size (set in
// StringSplitFn.BufSize).
//
// Note that the substring blocks are not guaranteed to line up with the
// restriction boundaries. ProcessElement is expected to emit any substring
// block that begins in its restriction, even if it extends past the end of the
// restriction.
//
// Example: If BufSize is 100, then a restriction of 75 to 325 should emit the
// following substrings: [100, 200], [200, 300], [300, 400]
func (fn *StringSplitFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, elem string, emit func(string)) {
	log.Debugf(ctx, "StringSplit ProcessElement: Tracker = %v", rt)
	i := rt.GetRestriction().(offsetrange.Restriction).Start
	if rem := i % fn.BufSize; rem != 0 {
		i += fn.BufSize - rem // Skip to next multiple of BufSize.
	}
	strEnd := int64(len(elem))

	for rt.TryClaim(i) == true {
		if i+fn.BufSize > strEnd {
			emit(elem[i:])
		} else {
			emit(elem[i : i+fn.BufSize])
		}
		i += fn.BufSize
	}
}

// LogFn is a DoFn to log our split output.
type LogFn struct{}

// ProcessElement logs each element it receives.
func (fn *LogFn) ProcessElement(ctx context.Context, in string) {
	log.Infof(ctx, "StringSplit Output:\n%v", in)
}

// FinishBundle waits a bit so the job server finishes receiving logs.
func (fn *LogFn) FinishBundle() {
	time.Sleep(2 * time.Second)
}

// Use our StringSplitFn to split Shakespeare monologues into substrings and
// output them.
func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p := beam.NewPipeline()
	s := p.Root()

	monologues := beam.Create(s, macbeth, juliet, helena)
	split := beam.ParDo(s, &StringSplitFn{50}, monologues)
	beam.ParDo0(s, &LogFn{}, split)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

var macbeth = `Is this a dagger which I see before me,
The handle toward my hand? Come, let me clutch thee.
I have thee not, and yet I see thee still.
Art thou not, fatal vision, sensible
To feeling as to sight? or art thou but
A dagger of the mind, a false creation,
Proceeding from the heat-oppressed brain?
I see thee yet, in form as palpable
As this which now I draw.
Thou marshall'st me the way that I was going;
And such an instrument I was to use.
Mine eyes are made the fools o' the other senses,
Or else worth all the rest; I see thee still,
And on thy blade and dudgeon gouts of blood,
Which was not so before. There's no such thing:
It is the bloody business which informs
Thus to mine eyes. Now o'er the one halfworld
Nature seems dead, and wicked dreams abuse
The curtain'd sleep; witchcraft celebrates
Pale Hecate's offerings, and wither'd murder,
Alarum'd by his sentinel, the wolf,
Whose howl's his watch, thus with his stealthy pace.
With Tarquin's ravishing strides, towards his design
Moves like a ghost. Thou sure and firm-set earth,
Hear not my steps, which way they walk, for fear
Thy very stones prate of my whereabout,
And take the present horror from the time,
Which now suits with it. Whiles I threat, he lives:
Words to the heat of deeds too cold breath gives.
[A bell rings]
I go, and it is done; the bell invites me.
Hear it not, Duncan; for it is a knell
That summons thee to heaven or to hell.`

var juliet = `O Romeo, Romeo! wherefore art thou Romeo?
Deny thy father and refuse thy name;
Or, if thou wilt not, be but sworn my love,
And I'll no longer be a Capulet.
'Tis but thy name that is my enemy;
Thou art thyself, though not a Montague.
What's Montague? it is nor hand, nor foot,
Nor arm, nor face, nor any other part
Belonging to a man. O, be some other name!
What's in a name? that which we call a rose
By any other name would smell as sweet;
So Romeo would, were he not Romeo call'd,
Retain that dear perfection which he owes
Without that title. Romeo, doff thy name,
And for that name which is no part of thee
Take all myself.`

var helena = `Lo, she is one of this confederacy!
Now I perceive they have conjoin'd all three
To fashion this false sport, in spite of me.
Injurious Hermia! most ungrateful maid!
Have you conspired, have you with these contrived
To bait me with this foul derision?
Is all the counsel that we two have shared,
The sisters' vows, the hours that we have spent,
When we have chid the hasty-footed time
For parting us,--O, is it all forgot?
All school-days' friendship, childhood innocence?
We, Hermia, like two artificial gods,
Have with our needles created both one flower,
Both on one sampler, sitting on one cushion,
Both warbling of one song, both in one key,
As if our hands, our sides, voices and minds,
Had been incorporate. So we grow together,
Like to a double cherry, seeming parted,
But yet an union in partition;
Two lovely berries moulded on one stem;
So, with two seeming bodies, but one heart;
Two of the first, like coats in heraldry,
Due but to one and crowned with one crest.
And will you rent our ancient love asunder,
To join with men in scorning your poor friend?
It is not friendly, 'tis not maidenly:
Our sex, as well as I, may chide you for it,
Though I alone do feel the injury.`
