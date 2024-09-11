import tensorflow as tf
mnist = tf.keras.datasets.mnist

# (x_train, y_train), (x_test, y_test) = mnist.load_data()
# x_train, x_test = x_train / 255.0, x_test / 255.0

# model = tf.keras.models.Sequential([
#     tf.keras.layers.Flatten(input_shape=(28, 28)),
#     tf.keras.layers.Dense(128, activation='relu'),
#     tf.keras.layers.Dropout(0.2),
#     tf.keras.layers.Dense(10, activation='softmax')
# ])

# model.compile(optimizer='adam',
#               loss='sparse_categorical_crossentropy',
#               metrics=['accuracy'])


# model.fit(x_train, y_train, epochs=5)

# model.evaluate(x_test,  y_test, verbose=2)


# 假设有 10 个不同的类别  
category_column = tf.feature_column.categorical_column_with_identity(  
    key='category', num_buckets=10)  
  
# 将类别列转换为嵌入列，假设嵌入向量的维度为 4  
embedding_column = tf.feature_column.embedding_column(category_column, dimension=4)

print(embedding_column.variable_shape)