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
package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/pkg/errors"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/", "Path to the directory or GCS prefix containing files to read from")
	output = flag.String("output", "tmp/tfidf.csv", "Path to the directory or GCS prefix to write results to")
)

const (
	gcsFilesystem = "gs://"
	love          = "love"
)

type uriKeyParDo struct {
	uri string
}

func (u *uriKeyParDo) addUriKey(line string) (string, string) {
	return u.uri, line
}

// ReadDocuments reads in each line in each uri, adding the uri name
// as a key
func ReadDocuments(s beam.Scope, uris []string) beam.PCollection {
	s = s.Scope("ReadDocuments")
	pcollections := []beam.PCollection{}
	for _, uri := range uris {
		// we create a DoFn with state to pass in a static filename
		// note that we also could have created a singleton PCollection and
		// passed the filename as a side input
		parDo := &uriKeyParDo{
			uri: uri,
		}
		lines := textio.Read(s, uri)
		keyedLines := beam.ParDo(s, parDo.addUriKey, lines)
		pcollections = append(pcollections, keyedLines)
	}
	// returns a PCollection<filename, line>
	return beam.Flatten(s, pcollections...)
}

// WriteTfIdf accepts PCollection<KV<String, TfIdf>>
// It formats the word-tfidf mappings and writes them to the output location
func WriteTfIdf(s beam.Scope, tfIdfs beam.PCollection, output string) {
	s = s.Scope("WriteTfIdf")
	formatted := beam.ParDo(s.Scope("Format"), func(word string, tfIdf TfIdf) string {
		return fmt.Sprintf("%s,\t%s,\t%f", word, tfIdf.File, tfIdf.Frequency)
	}, tfIdfs)
	textio.Write(s, output, formatted)
}

// WordCount groups a word and the number of times it appears
type WordCount struct {
	Word      string
	Instances int
}

// WordFile groups a word and its file
type WordFile struct {
	Word string
	File string
}

// FileFrequency groups a frequency value and a file
type FileFrequency struct {
	Frequency float64
	File      string
}

// TfIdf is the same type as FileFrequency
type TfIdf FileFrequency

// ComputeTfIdf takes in a PCollection<KV<String,String>>, where the key is a filename
// and the value is a line in that file. It returns a term frequency–inverse document frequency
// for each word in each file as PCollection<KV<String, TfIdf>>
func ComputeTfIdf(s beam.Scope, uriContent beam.PCollection) beam.PCollection {
	s = s.Scope("ComputeTfIdf")

	// Get all filenames
	values := beam.DropValue(s, uriContent)
	// Distinct the filenames
	distinctUris := filter.Distinct(s, values)
	// Add fixed key, drop value, count, then drop key to get the number
	// of distinct filenames. This equivalent is Count.globally() in Java.
	// documentCount is then a singleton PCollection<int>
	distinctUrisWithKey := beam.AddFixedKey(s, distinctUris)
	allFixedKey := beam.DropValue(s, distinctUrisWithKey)
	fixedKeyCount := stats.Count(s, allFixedKey)
	documentCount := beam.DropKey(s, fixedKeyCount)

	// Get each word and its corresponding file.
	// Use the WordFile struct to storee this so that we can avoid KV's
	uriToWords := beam.ParDo(s.Scope("SplitWords"), func(uri, line string, emit func(WordFile)) {
		regex := regexp.MustCompile(`\W+`)
		words := regex.Split(line, -1)
		for _, word := range words {
			if strings.EqualFold(love, word) {
				log.Infof(context.Background(), "Found %s", strings.ToLower(word))
			}
			if word != "" {
				emit(WordFile{
					File: uri,
					Word: strings.ToLower(word),
				})
			}
		}
	}, uriContent)

	// Get distinct (word, file) pairs
	distinctUriWords := filter.Distinct(s, uriToWords)
	// Get just the words
	words := beam.ParDo(s.Scope("GetWords"), func(wordFile WordFile) string {
		return wordFile.Word
	}, distinctUriWords)
	// Compute a mapping from each word to the total
	// number of documents in which it appears.
	wordToDocCount := stats.Count(s, words)

	// Get just the filenames
	uris := beam.ParDo(s.Scope("GetFilenames"), func(wordFile WordFile) string {
		return wordFile.File
	}, uriToWords)
	// Compute a mapping from each URI to the total
	// number of words in the document associated with that URI.
	uriToWordsTotal := stats.Count(s, uris)

	// Count, for each (URI, word) pair, the number of
	// occurrences of that word in the document associated
	// with the URI.
	uriAndWordToCount := stats.Count(s, uriToWords)

	// Adjust the above collection to a mapping from
	// (URI, word) pairs to counts into an isomorphic mapping
	// from URI to (word, count) pairs, to prepare for a join
	// by the URI key.
	uriToWordAndCount := beam.ParDo(s.Scope("SwitchKeys"), func(wordFile WordFile, instances int) (string, WordCount) {
		return wordFile.File, WordCount{
			Word:      wordFile.Word,
			Instances: instances,
		}
	}, uriAndWordToCount)

	// Compute a mapping from each word to a (URI, term frequency)
	// pair for each URI. A word's term frequency for a document
	// is simply the number of times that word occurs in the document
	// divided by the total number of words in the document.
	uriToWordAndCountAndTotal := beam.CoGroupByKey(s, uriToWordsTotal, uriToWordAndCount)
	wordToUriAndTermFrequency := beam.ParDo(s.Scope("ComputeTermFrequencies"), func(filename string, totalDocCountValues func(*int) bool, wordCountValues func(*WordCount) bool, emit func(string, FileFrequency)) {
		var curWordCount WordCount
		var wordTotal int
		totalDocCountValues(&wordTotal)
		for wordCountValues(&curWordCount) {
			freq := FileFrequency{
				File:      filename,
				Frequency: float64(curWordCount.Instances) / float64(wordTotal),
			}
			emit(curWordCount.Word, freq)
		}
	}, uriToWordAndCountAndTotal)

	// Format the KV<word, count documents> as a WordCount object to avoid KV's
	wordCount := beam.ParDo(s.Scope("FormatWordCount"), func(word string, count int) WordCount {
		return WordCount{
			Word:      word,
			Instances: count,
		}
	}, wordToDocCount)

	// Compute a mapping from each word to its document frequency.
	// A word's document frequency in a corpus is the number of
	// documents in which the word appears divided by the total
	// number of documents in the corpus. Note how the total number of
	// documents is passed as a side input; the same value is
	// presented to each invocation of the DoFn.
	wordToDocFrequency := beam.ParDo(s.Scope("ComputeDocFrequency"), func(wordCount WordCount, totalDocuments int, emit func(string, float64)) {
		emit(wordCount.Word, float64(wordCount.Instances)/float64(totalDocuments))
	}, wordCount, beam.SideInput{Input: documentCount})

	// Group word to term frequency and word to doc frequency
	wordToUriAndTfAndDf := beam.CoGroupByKey(s, wordToUriAndTermFrequency, wordToDocFrequency)

	// Compute a mapping from each word to a (URI, TF-IDF) score
	// for each URI. There are a variety of definitions of TF-IDF
	// ("term frequency - inverse document frequency") score;
	// here we use a basic version that is the term frequency
	// divided by the log of the document frequency.
	return beam.ParDo(s.Scope("ComputeTfIdf"), func(word string, wordFrequencies func(*FileFrequency) bool, docFrequencies func(*float64) bool, emit func(string, TfIdf)) {
		var docFrequency float64
		docFrequencies(&docFrequency)
		var curWordFrequency FileFrequency
		for wordFrequencies(&curWordFrequency) {
			uri := curWordFrequency.File
			termFrequency := curWordFrequency.Frequency
			tfIdf := termFrequency * math.Log(1/docFrequency)
			emit(word, TfIdf{
				File:      uri,
				Frequency: tfIdf,
			})
		}
	}, wordToUriAndTfAndDf)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if input == nil || output == nil {
		log.Exitf(ctx, "Must specify both input and output")
	}
	p := beam.NewPipeline()
	s := p.Root()
	inputDocuments, inputErr := listInputDocuments(*input)
	if inputErr != nil {
		log.Exitf(ctx, "Failed to create list of input documents: %v", inputErr)
	}

	col := ReadDocuments(s, inputDocuments)
	tfIdfs := ComputeTfIdf(s, col)
	WriteTfIdf(s, tfIdfs, *output)
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
func listInputDocuments(absoluteUri string) ([]string, error) {
	if strings.HasPrefix(absoluteUri, gcsFilesystem) {
		gcsClient := gcs.New(context.Background())
		return gcsClient.List(context.Background(), fmt.Sprintf("%s*", absoluteUri))
	}
	fileInfos, readDirErr := ioutil.ReadDir(absoluteUri)
	if readDirErr != nil {
		return nil, errors.Wrapf(readDirErr, "failed to read directory %s", absoluteUri)
	}
	fileNames := []string{}
	for _, fileInfo := range fileInfos {
		fileNames = append(fileNames, fileInfo.Name())
	}
	return fileNames, nil
}
