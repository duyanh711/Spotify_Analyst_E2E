from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, year, month, dayofmonth
from pyspark.ml.feature import Imputer, MinMaxScaler, VectorAssembler

def transform():

    spark = SparkSession.builder.appName("SpotifyDataTransformation").getOrCreate()

    artist_df = spark.read.csv("./data/artists.csv", header=True, inferSchema=True)

    artist_df = artist_df.withColumn("spotify_url", col("external_urls")) \
                        .withColumn("followers", col("followers").cast("int")) \
                        .drop("external_urls")

    artist_df.show(truncate=False)

    albums_df = spark.read.csv("./data/albums.csv", header=True, inferSchema=True)

    albums_df = albums_df.withColumn("spotify_url", col("external_urls")) \
                        .withColumn("release_year", year(col("release_date"))) \
                        .withColumn("release_month", month(col("release_date"))) \
                        .withColumn("release_day", dayofmonth(col("release_date"))) \
                        .drop("external_urls", "release_date")

    albums_df.show(truncate=False)

    tracks_df = spark.read.csv("./data/tracks.csv", header=True, inferSchema=True)

    tracks_df = tracks_df.withColumn("spotify_url", col("external_urls")) \
                        .withColumn("explicit", col("explicit").cast("boolean")) \
                        .drop("external_urls")

    tracks_df.show(truncate=False)

    audio_features_df = spark.read.csv("./data/audio_features.csv", header=True, inferSchema=True)

    def preprocess_audio_data(df, feature_cols):
        imputer = Imputer(inputCols=feature_cols, outputCols=[f"imputed_{col}" for col in feature_cols]).setStrategy("mean")
        df_imputed = imputer.fit(df).transform(df)
        
        assembler = VectorAssembler(inputCols=[f"imputed_{col}" for col in feature_cols], outputCol="features")
        df_vector = assembler.transform(df_imputed)
        
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_vector)
        df_scaled = scaler_model.transform(df_vector)
        
        for col_name in feature_cols:
            mean_value = df_scaled.agg(mean(col(f"imputed_{col_name}"))).first()[0]
            stddev_value = df_scaled.agg(stddev(col(f"imputed_{col_name}"))).first()[0]
            df_scaled = df_scaled.withColumn(f"zscore_{col_name}", (col(f"imputed_{col_name}") - mean_value) / stddev_value)
        
        return df_scaled

    audio_feature_columns = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms']

    df_processed = preprocess_audio_data(audio_features_df, audio_feature_columns)

    df_processed.select('scaled_features', *[f'zscore_{col}' for col in audio_feature_columns]).show(truncate=False)


    # Ghi dữ liệu ra CSV
    audio_features_df.write.csv("./data/output/audio_features_transformed.csv", mode="overwrite", header=True)


    artist_df.write.csv("./data/output/artists_transformed.csv", mode="overwrite", header=True)
    albums_df.write.csv("./data/output/albums_transformed.csv", mode="overwrite", header=True)
    tracks_df.write.csv("./data/output/tracks_transformed.csv", mode="overwrite", header=True)
