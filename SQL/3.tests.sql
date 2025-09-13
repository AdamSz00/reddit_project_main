SELECT * FROM unique_authors;

SELECT * FROM reddit_posts
LIMIT 600;

SELECT * FROM toxicity_results;
LIMIT 600;

-- delete all data from reddit_posts table
DELETE FROM unique_authors;
DELETE FROM reddit_posts;
DELETE FROM toxicity_results;


SELECT 
    rp.body,
    tr.*
FROM
    reddit_posts as rp 
INNER JOIN
    toxicity_results as tr ON rp.post_id = tr.post_id
;