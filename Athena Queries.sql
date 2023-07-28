
/* Notes:
   1. Final_Clean table,final_predict table and Prediction_count tables are created.
   2. Please note that all the feature engineering are done in databricks and only
    finalised dataframes are extracted to athena
 */


# to create final_clean
CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_elon_project`.`final_clean` (
  `tweet_id` bigint,
  `user_name` string,
  `user_screen_name` string,
  `tweet_text` string,
  `tweet_retweet_count` int,
  `user_location` string,
  `tweet_hashtags` string,
  `tweet_created_at` string,
  `tweet_sentiment_label` string,
  `timestamp` string,
  `hour` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-twitter-project/twitter_project_elonmusk/final_clean.csv/';

select * from final_clean limit 5;


# to create final_predict
CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_elon_project`.`final_predict` (
  `tweet_text` string,
  `tweet_sentiment_label` string,
  `label` double,
  `prediction` double,
  `prediction_cat` string,
  `isrightprediction` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-twitter-project/twitter_project_elonmusk/final_predict.csv/';

select * from final_predict limit 5;

# to create prediction_count

CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_elon_project`.`Prediction_count` (
  `prediction_cat` string,
  `isrightprediction` string,
  `count` bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b17-twitter-project/twitter_project_elonmusk/prediction_count.csv/';

select * from prediction_count limit 5;