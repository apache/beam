import tensorflow as tf

base_model = tf.keras.applications.MobileNetV2(weights='imagenet')


@tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
def serve_tf_example(serialized_tf_examples):
  features = tf.io.parse_example(
      serialized_tf_examples,
      features={'image': tf.io.FixedLenFeature(shape=(), dtype=tf.string)})

  # TODO: Pass the output type as serialized value through tf.train.Example
  # TODO: Pass the input size as well in the Proto
  deserialized_value = tf.io.parse_tensor(
      features['image'][0], out_type=tf.uint8)
  # deserialized_value = tf.reshape(deserialized_value, [-

  # expand dimension of the image
  deserialized_value = tf.expand_dims(deserialized_value, axis=0)
  return base_model(deserialized_value, training=False)


signature = {'serving_default': serve_tf_example}
tf.keras.models.save_model(base_model, '/tmp/tf', signatures=signature)
