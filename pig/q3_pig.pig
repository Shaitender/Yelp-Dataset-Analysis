businesses = LOAD '/user/hue/yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as json:map[];

attributes = FOREACH businesses GENERATE (float)json#'stars' AS stars, FLATTEN(json#'categories') as categories, (double)json#'latitude' AS latitude, (double)json#'longitude' AS longitude;

filterlatlong = FILTER attributes BY latitude < 43.22145313 AND longitude < -89.21487592 AND latitude > 42.93172719 AND longitude > -89.61009908;

groupcitycat = GROUP filterlatlong BY categories;

result = FOREACH groupcitycat GENERATE FLATTEN(group) as (categories), AVG(filterlatlong.stars) as st;

STORE result INTO '/user/hue/question3/ans.tsv';

