DROP TABLE IF EXISTS records_ext;
DROP TABLE IF EXISTS labels_ext;
DROP TABLE IF EXISTS json_output;

CREATE EXTERNAL TABLE IF NOT EXISTS records_ext (
    label_id INT,
    artist_id INT,
    artist_name STRING,
    decade INT,
    albums_count INT,
    genres_list STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir3}';


CREATE EXTERNAL TABLE IF NOT EXISTS labels_ext (
    label_id INT,
    label_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir4}';


-- make label_id unique
INSERT OVERWRITE TABLE labels_ext
SELECT label_id, label_name
FROM (
    SELECT label_id, label_name,
           ROW_NUMBER() OVER (PARTITION BY label_id ORDER BY label_name) AS row_num
    FROM labels_ext
    WHERE label_id IS NOT NULL
) ranked
WHERE row_num = 1;


CREATE EXTERNAL TABLE IF NOT EXISTS json_output (
    label_name string,
    decade  int,
    artists_count int,
    releases_count int,
    genres array<string>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';

WITH ranked_labels AS (
    SELECT
        l.label_id as label_id,
        l.label_name AS label_name,
        r.decade AS decade,
        SUM(r.albums_count) AS releases_count,
        COUNT(DISTINCT r.artist_id) AS artists_count
    FROM
        records_ext r
    JOIN
        labels_ext l
    ON
        r.label_id = l.label_id
    GROUP BY
        l.label_name, l.label_id, decade
)
, ranked_labels_ranked AS (
    SELECT
        label_id,
        label_name,
        decade,
        releases_count,
        artists_count,
        ROW_NUMBER() OVER (PARTITION BY decade ORDER BY releases_count DESC) AS rank
    FROM
        ranked_labels
)
, unique_genres AS (
    SELECT
        label_id,
        decade,
        COLLECT_SET(gl) as genres
    FROM (
        SELECT label_id, decade, genre as gl FROM records_ext r LATERAL VIEW EXPLODE(SPLIT(r.genres_list, ';')) e AS genre
    ) temp
    GROUP BY label_id, decade
)

INSERT OVERWRITE TABLE json_output
SELECT
    rlr.label_name as label_name,
    rlr.decade as decade,
    rlr.artists_count as artist_count,
    rlr.releases_count as releases_count,
    ug.genres as genres
FROM
    ranked_labels_ranked rlr
JOIN
    unique_genres ug
ON
    (rlr.label_id = ug.label_id AND rlr.decade = ug.decade)
WHERE
    rlr.rank <= 3
ORDER BY
    decade desc, releases_count asc;

