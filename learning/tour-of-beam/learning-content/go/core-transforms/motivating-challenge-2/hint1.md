You can solve the challenge with this way:
1. First you need to divide `PCollection` by `beam.ParDo(s, func(line string) (string,int){ .. .}`.
2. Combine by key the values should be combined `(score1 + score2)` `beam.CombinePerKey(s, func(score1, score2 int) int{ ... }`
