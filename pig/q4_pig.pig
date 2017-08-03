data_user = LOAD '/user/hue/yelp_academic_dataset_user.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

data_review = LOAD '/user/hue/yelp_academic_dataset_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

data_business = LOAD '/user/hue/yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

at_user = FOREACH data_user GENERATE json#'user_id' AS user_id, (int)json#'review_count' as review_count;

at_review = FOREACH data_review GENERATE json#'user_id' AS user_id, json#'review_id' as review_id, json#'business_id' as business_id, (float)json#'stars' AS star;

at_business = FOREACH data_business GENERATE json#'business_id' AS bid, FLATTEN (json#'categories') as categories;

rank_users = ORDER at_user BY review_count DESC;

rank_limit = LIMIT rank_users 10;

rev_bus = JOIN at_review BY business_id, at_business BY bid;

total_combine = JOIN rev_bus BY at_review::user_id, rank_limit BY user_id;

get_reqd = FOREACH total_combine GENERATE at_review::user_id, at_business::categories, at_review::star;

grouping = GROUP get_reqd by (at_review::user_id, at_business::categories);

avg_stars = FOREACH grouping GENERATE FLATTEN(group) as (usr, cato), AVG(get_reqd.star) as star;

STORE avg_stars INTO '/user/hue/question4/ans_1.tsv';

