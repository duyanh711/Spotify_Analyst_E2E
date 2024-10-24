from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, year, month, dayofmonth

# Initialize Spark session
spark = SparkSession.builder.appName("SpotifyDataTransformation").getOrCreate()

# Load artist data
artist_df = spark.read.csv("./data/artists.csv", header=True, inferSchema=True)

# Transform artist data
artist_df = artist_df.withColumn("spotify_url", col("external_urls")) \
                     .withColumn("followers", col("followers").cast("int")) \
                     .drop("external_urls")

artist_df.show(truncate=False)

# Load albums data
albums_df = spark.read.csv("./data/albums.csv", header=True, inferSchema=True)

# Transform albums data: Split release date and extract external URLs
albums_df = albums_df.withColumn("spotify_url", col("external_urls")) \
                     .withColumn("release_year", year(col("release_date"))) \
                     .withColumn("release_month", month(col("release_date"))) \
                     .withColumn("release_day", dayofmonth(col("release_date"))) \
                     .drop("external_urls", "release_date")

albums_df.show(truncate=False)

# Load tracks data
tracks_df = spark.read.csv("./data/tracks.csv", header=True, inferSchema=True)

# Transform tracks data: Extract external URL and handle explicit column
tracks_df = tracks_df.withColumn("spotify_url", col("external_urls")) \
                     .withColumn("explicit", col("explicit").cast("boolean")) \
                     .drop("external_urls")

tracks_df.show(truncate=False)

# Load audio features data
audio_features_df = spark.read.csv("./data/audio_features.csv", header=True, inferSchema=True)

# Optional: Normalize some columns (like danceability, energy, etc.) between 0 and 1 for consistency.
# Assuming these values are already normalized, we can skip this, but you could scale them using a UDF if needed.

audio_features_df.show(truncate=False)

# Write transformed data back to parquet or CSV
# artist_df.write.csv("./data/output/artists_transformed.csv", mode="overwrite", header=True)
# albums_df.write.csv("./data/output/albums_transformed.csv", mode="overwrite", header=True)
# tracks_df.write.csv("./data/output/tracks_transformed.csv", mode="overwrite", header=True)
# audio_features_df.write.csv("./data/output/audio_features_transformed.csv", mode="overwrite", header=True)
