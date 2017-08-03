businessdata = LOAD '/user/hue/yelp_academic_dataset_business.json' USING
                com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);
                
reviewdata = LOAD '/user/hue/yelp_academic_dataset_review.json' USING
                com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

bus_attributes = FOREACH businessdata GENERATE (float)json#'stars' AS stars,
                FLATTEN(json#'categories') as categories, (double)json#'latitude' AS latitude, (double)json#'longitude' AS longitude, json#'business_id' AS bid;

filterlatlong = filter bus_attributes by latitude < 43.22145313 AND longitude < -89.21487592
                AND latitude > 42.93172719 AND longitude > -89.61009908
                AND (categories == 'Food');

topfilter = ORDER filterlatlong BY stars DESC;

bottomfilter = ORDER filterlatlong BY stars ASC;

topele = LIMIT topfilter 10;

bottomele = LIMIT bottomfilter 10;

topbottomunion = UNION topele, bottomele;

rev_attributes = FOREACH reviewdata GENERATE json#'business_id' AS business_id,
                (datetime)json#'date' AS date,
json#'review_id' AS review_id, (float)json#'stars' AS stars, json#'user_id' AS user_id;

monthfilter = FILTER rev_attributes BY (GetMonth(date)==1) OR (GetMonth(date)==2) OR
                (GetMonth(date)==3) OR (GetMonth(date)==4) OR (GetMonth(date)==5);

combined = JOIN topbottomunion BY bid, monthfilter BY business_id;

comb_result = FOREACH combined GENERATE monthfilter::stars, monthfilter::business_id,
                monthfilter::review_id;

res = GROUP comb_result by monthfilter::business_id;

final = FOREACH res GENERATE group as bus_id, AVG(comb_result.stars) as star;

STORE final INTO '/user/hue/question5/ans_1.tsv';
