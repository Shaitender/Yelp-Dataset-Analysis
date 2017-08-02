![New York University](https://upload.wikimedia.org/wikipedia/en/thumb/5/58/NYU_logo.svg/1280px-NYU_logo.svg.png)
## Yelp Dataset Challenge

### About the Data
- 4.1M reviews and 947K tips by 1M users for 144K businesses
- 1.1M business attributes, e.g., hours, parking availability, ambience.
- Aggregated check-ins over time for each of the 125K businesses
- 200,000 pictures from the included businesses

### Analysis on Zeppelin
1. Summarize the number of reviews by US city, by business category.
```scala
import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json")
 
val b = business.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))
    
b.registerTempTable("business")
```
#### Visualization
```
%sql SELECT city,category, SUM(review_count) AS total_review,FROM business group by category,city order by city

```
![alt tag](http://url/to/img1.png)

2. Rank all cities by # of stars descending, for each category
```scala
import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json")
 
val b = business.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))
    
b.registerTempTable("business")
```
#### Visualization
```
%sql SELECT  category,city,avg(stars) as avg_stars from business group by category,city order by category asc, avg_stars desc;

```
![alt tag](http://url/to/img2.png)

3. What is the average rank (# stars) for businesses within 10 miles of the University of Wisconsin - Madison, by type of business?
>Center: University of Wisconsin - Madison
>Latitude: 43 04’ 30” N, Longitude: 89 25’ 2” W
>Decimal Degrees: Latitude: 43.0766, Longitude: -89.4125
>The bounding box for this problem is ~10 miles, which we will loosely define as 10 minutes. So the bounding box is a square box, 20 minutes long each side (of longitude and latitude), with UWM at the center.
```scala
import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json")
 
val b = business.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))
    
b.registerTempTable("business")
```
#### Visualization
```
%sql Select category, avg(stars) as avg_star from business where latitude < 43.22145313 AND longitude < -89.21487592 AND latitude > 42.93172719 AND longitude > -89.61009908 group by category order by category;

```
![alt tag](http://url/to/img3.png)
4. Rank reviewers by number of reviews. For the top 10 reviewers, show their average number of stars, by category.
```scala
import spark.implicits._
import org.apache.spark.sql.functions._

val spark = new org.apache.spark.sql.SQLContext(sc)

val user_data = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_user.json")

val user_attributes = user.select("user_id","name","review_count")

user_attributes.registerTempTable("user");

val user_attributes_sorted = spark.sql("select * from user order by review_count desc");

val top10 = user_attributes_sorted.limit(10);

top10.registerTempTable("top10");

val reviews_data = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_review.json")

val review_attributes = reviews_data.select("user_id","business_id","stars")

review_attributes.registerTempTable("review");

val user_review = spark.sql("select top10.user_id, top10.name, top10.review_count, review.business_id, review.stars from top10 JOIN review on top10.user_id=review.user_id")

user_review.registerTempTable("user_review")

val business_data = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json")

val b = business.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))

val business_attributes = b.select("business_id","category");
business_attributes.registerTempTable("business");

val user_review_business = spark.sql("select user_review.business_id,user_review.name,user_review.stars,business.category from user_review JOIN business on user_review.business_id = business.business_id");

user_review_business.registerTempTable("user_review_business")

val resTable = spark.sql("select name,category, sum(stars) from user_review_business group by name,category")
```
#### Visualization
```
%sql select name, category, sum(stars) from user_review_business group by name,category
```
![alt tag](http://url/to/img4.png)

5. For the top 10 and bottom 10 food business near UWM (in terms of stars), summarize star rating for reviews in January through May.
```
import org.apache.spark.sql.functions._
import spark.implicits._

val spark = new org.apache.spark.sql.SQLContext(sc)

val business_data = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json")

val b = business_data.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))
    
b.registerTempTable("business")

val catlatlong = spark.sql("select business_id,name,categories, stars from business where latitude >= 42.908333 AND latitude <= 43.241667 AND longitude >= -89.583889 AND longitude <= -89.250556 AND category == 'Food'");

val catflat = catlatlong.withColumn("category", explode(
    when(col("categories").isNotNull, col("categories"))
    .otherwise(array(lit(null).cast("string")))
    ))

catflat.registerTempTable("catflat")	
	
val topstars = spark.sql("select business_id,name,category, stars from catflat order by stars desc limit 10")

val bottomstars = spark.sql("select business_id,name,category, stars from catflat order by stars asc limit 10")

topstars.registerTempTable("topstars")

bottomstars.registerTempTable("bottomstars")

val review_data = spark.read.json("hdfs://localhost:8020/usr/data/yelp_dataset_challenge_round9/yelp_academic_dataset_review.json")

review_data.registerTempTable("review")

val filterdate = spark.sql("select business_id,date,stars from review where month(date)>=1 and month(date)<=5")

filterdate.registerTempTable("filterdate")

val joinedTableTop = spark.sql("select topstars.name,topstars.category, topstars.business_id, filterdate.date, filterdate.stars from topstars INNER JOIN filterdate on topstars.business_id = filterdate.business_id")

val joinedTableBottom = spark.sql("select bottomstars.name, bottomstars.category, bottomstars.business_id, filterdate.date, filterdate.stars from bottomstars INNER JOIN filterdate on bottomstars.business_id = filterdate.business_id")

joinedTableTop.registerTempTable("joinedTableTop")

joinedTableBottom.registerTempTable("joinedTableBottom")

val jointotal = spark.sql("Select * from joinedTableTop as topfull union Select * from joinedTableBottom as bottomfull")

jointotal.registerTempTable("jointotal")

val avg_stars = spark.sql("select business_id, avg(stars) as avg_stars from jointotal group by business_id")

```
#### Visualization
```
%sql select business_id, avg(stars) as avg_stars from jointotal group by business_id

```
![alt tag](http://url/to/img5.png)
