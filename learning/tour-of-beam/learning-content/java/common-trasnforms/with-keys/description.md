# WithKeys

Takes a ```PCollection<V>``` and produces a ```PCollection<KV<K, V>>``` by associating each input element with a key.

There are two versions of WithKeys, depending on how the key should be determined:

```WithKeys.of(SerializableFunction<V, K> fn)``` takes a function to compute the key from each value.

```
PCollection<String> words = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
PCollection<KV<Integer, String>> lengthAndWord = words.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
    @Override
    public Integer apply(String word) {
        return word.length();
    }
}));
```

Output
```
KV{6, Apache}
KV{5, World}
KV{4, Beam}
KV{5, Hello}
```


```WithKeys.of(K key)``` associates each value with the specified key.

```
PCollection<String> words = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
PCollection<KV<String, String>> specifiedKeyAndWord = words.apply(WithKeys.of("SpecifiedKey"));
```

Output
```
KV{SpecifiedKey, Apache}
KV{SpecifiedKey, Hello}
KV{SpecifiedKey, World}
KV{SpecifiedKey, Beam}
```