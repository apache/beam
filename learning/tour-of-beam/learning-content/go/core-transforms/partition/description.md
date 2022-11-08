# Partition

`Partition` is a Beam transform for `PCollection` objects that store the same data type. `Partition` splits a single `PCollection` into a fixed number of smaller collections.

`Partition` divides the elements of a `PCollection` according to a partitioning function that you provide. The partitioning function contains the logic that determines how to split up the elements of the input `PCollection` into each resulting partition `PCollection`. The number of partitions must be determined at graph construction time. You can, for example, pass the number of partitions as a command-line option at runtime (which will then be used to build your pipeline graph), but you cannot determine the number of partitions in mid-pipeline (based on data calculated after your pipeline graph is constructed, for instance).

The following example divides a `PCollection` into percentile `groups.Partition`.

```
func decileFn(student Student) int {
	return int(float64(student.Percentile) / float64(10))
}

func init() {
	register.Function1x1(decileFn)
}



// Partition returns a slice of PCollections
studentsByPercentile := beam.Partition(s, 10, decileFn, students)
// Each partition can be extracted by indexing into the slice.
fortiethPercentile := studentsByPercentile[4]
```

### Description for example

The input is integers . Inside `applyTransform()` compares each element with 100. Numbers that are larger than one `PCollection` array, if smaller in another `PCollection` array.