-- Pig script to calculate co-occurence counts

-- Load LinkedIn's DataFu Library
register '/data/wgsn/datafu-0.0.4.jar'
define UnorderedPairs datafu.pig.bags.UnorderedPairs();

-- filter just the product clicks
click_stream = LOAD '/data/wgsn/click_stream' USING PigStorage(',') AS (user:chararray, url:chararray, timestamp:int, ip:chararray);
click_stream_new = FOREACH click_stream GENERATE user, REGEX_EXTRACT(url, '(http:\\/\\/company.com\\?product=)(.*)(\')', 2) AS product, ip;
click_stream_product = FILTER click_stream_new BY (product != '');

-- remove blacklisted ip's
blacklist_ip_address = LOAD '/data/wgsn/blacklist_ip_address' AS (ip:chararray);
click_stream_product_joined = JOIN click_stream_product BY ip LEFT OUTER, blacklist_ip_address BY ip;
click_stream_product_filtered = FILTER click_stream_product_joined BY ($3 IS NULL);
user_product = FOREACH click_stream_product_filtered GENERATE user, product;

-- filter just the users from UK
user_country = LOAD '/data/wgsn/user_db' USING PigStorage(',') AS (user:chararray, country:chararray);
user_uk = FILTER user_country BY (country == 'UK');
user_product_uk_joined = JOIN user_uk BY user, user_product BY user;
user_product_final = FOREACH user_product_uk_joined GENERATE $2 AS user, $3 AS product;

-- Use LinkedIn's DataFu to generate unordered product pairs
user_grp = GROUP user_product_final BY user;
product_pair = FOREACH user_grp GENERATE FLATTEN(UnorderedPairs(user_product_final.product));
product_pair_flat = FOREACH product_pair GENERATE FLATTEN($0) AS prod1, FLATTEN($1) AS prod2;
-- order the unordered pairs so that (ipad,iphone) is the same as (iphone,ipad)
product_pair_ordered = FOREACH product_pair_flat GENERATE FLATTEN(($0<$1?($0,$1):($1,$0))) AS (prod1, prod2);
-- count each unordered pair
product_pair_grp = GROUP product_pair_ordered BY (prod1, prod2);
product_pair_count = FOREACH product_pair_grp GENERATE group AS productpair, COUNT(product_pair_ordered) AS total;

-- filter where count > 3000
product_pair_final = FILTER product_pair_count BY (total > 3000);
final_output = FOREACH product_pair_final GENERATE productpair, total;
DUMP final_output;