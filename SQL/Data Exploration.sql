/*
London crime data exploration
Skills used: CTE's, Windows function, Aggregate Functions
*/

--------------------------------------------------------------------------------------------------------------------------


/* Looking at Average street crimes vs borough */

WITH "borough_split" AS (
SELECT "borough", COUNT("id") AS "cnt_id","month"
FROM "street_crimes_borough"
GROUP BY "borough","month"
)
SELECT "borough", ROUND(AVG("cnt_id")) AS "average_crimes_count"
FROM "borough_split"
GROUP BY "borough"
ORDER BY "average_crimes_count" DESC


--------------------------------------------------------------------------------------------------------------------------


/* Looking at Crime category vs Crime count(in %) */

WITH "category_split" AS (
SELECT "category", COUNT(*) * 100/ SUM(COUNT(*)) OVER() AS "category_percentage"
FROM "street_crimes_borough"
GROUP BY "category"
)
SELECT "category",ROUND("category_percentage",2) AS "category_percentage"
FROM "category_split"
ORDER BY "category_percentage" DESC


--------------------------------------------------------------------------------------------------------------------------


/* Looking at latitude,longitude vs Crime count */

SELECT "location_latitude","location_longitude",COUNT("id") AS "cnt"
FROM "street_crimes_borough"
GROUP BY "location_latitude","location_longitude"
ORDER BY "cnt" DESC

--------------------------------------------------------------------------------------------------------------------------