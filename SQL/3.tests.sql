SELECT * FROM unique_authors;

SELECT COUNT(post_id) FROM toxicity_results;

SELECT * FROM toxicity_results;
LIMIT 600;

-- delete all data from reddit_posts table
DELETE FROM unique_authors;
DELETE FROM reddit_posts;
DELETE FROM toxicity_results;

-- find top 10 submissions with the highest number of toxic comments
SELECT 
    rp.submission_id,
    COUNT(*) AS toxic_comment_count
FROM 
    reddit_posts rp
JOIN 
    toxicity_results tr ON rp.post_id = tr.post_id
WHERE 
    tr.overall_toxicity > 0.5
  AND rp.type = 'comment'
GROUP BY rp.submission_id
ORDER BY toxic_comment_count DESC
LIMIT 10;