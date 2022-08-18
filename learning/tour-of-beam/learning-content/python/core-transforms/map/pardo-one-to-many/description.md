# ParDo one-to-many

It works like `ParDo` one-to-one, but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
class BreakIntoWordsDoFn(beam.DoFn):

    def process(self, element):
        return element.split()
        
with beam.Pipeline() as p:

  (p | beam.Create(['Hello Beam', 'It is awesome'])
     | beam.ParDo(BreakIntoWordsDoFn())
     | LogElements())
```