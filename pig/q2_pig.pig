businesses = LOAD '/user/hue/yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as json:map[];

attributes = FOREACH businesses GENERATE (float)json#'stars' AS stars, json#'city' AS city, FLATTEN(json#'categories') as categories;

groupcitycat = GROUP attributes BY (city,categories);

avgstars = FOREACH groupcitycat GENERATE AVG(attributes.stars) as st, FLATTEN(group) as (city,categories);

result = RANK avgstars by categories ASC, st DESC;

STORE result INTO '/user/hue/question2/ans.tsv';


