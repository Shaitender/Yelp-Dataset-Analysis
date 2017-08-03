businesses = LOAD '/user/hue/yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as json:map[];

attributes = FOREACH businesses GENERATE (int)json#'review_count' AS review_count, json#'city' AS city, FLATTEN(json#'categories') as categories;

groupcitycat = GROUP attributes BY (city, categories);

reviewcounts = FOREACH groupcitycat GENERATE FLATTEN(group) as (city, categories), COUNT(attributes.review_count) as total_count;

orderreviewcount = ORDER reviewcounts BY city;

STORE orderreviewcount INTO '/user/hue/question1/ans.tsv';
