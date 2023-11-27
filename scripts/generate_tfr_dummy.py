import tensorflow as tf


D = 100
B = 64

# Function to create a TFRecord example
def create_tf_example(features_list):
    # Create a dictionary mapping the feature name to the tf.Example-compatible data type
    feature = {}
    for features in features_list:
        for key, value in features.items():
            if isinstance(value, int):
                feature[key] = tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[value] * D)
                )
            elif isinstance(value, float):
                feature[key] = tf.train.Feature(
                    float_list=tf.train.FloatList(value=[value] * D)
                )
            elif isinstance(value, str):
                value = bytes(value, "utf-8")  # Convert string to bytes
                feature[key] = tf.train.Feature(
                    bytes_list=tf.train.BytesList(value=[value] * D)
                )
            else:
                raise ValueError(f"Unsupported data type: {type(value)}")

    # Create a Features message using tf.train.Example
    example = tf.train.Example(features=tf.train.Features(feature=feature))
    return example


features_list = [{"feature2": 0.5}]
# Create a TFRecord example
examples = [create_tf_example(features_list) for _ in range(B)]

# Write the TFRecord example to a file
with tf.io.TFRecordWriter("tfrs/example.tfrecord") as writer:
    for example in examples:
        writer.write(example.SerializeToString())
