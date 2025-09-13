--------------------------------------
--- CREATE TABLES FOR THE DATABASE ---
--------------------------------------

CREATE TABLE unique_authors (
    original_author VARCHAR(100) PRIMARY KEY,
    new_username VARCHAR(100) UNIQUE NOT NULL 
);

--- TABLE: reddit_posts
CREATE TABLE reddit_posts (
    type VARCHAR(20),
    submission_id CHAR(7),
    post_id CHAR(7) PRIMARY KEY,
    target_post_id CHAR(7),
    author VARCHAR(100) NOT NULL,
    target_author VARCHAR(100),
    community VARCHAR(100),
    title TEXT,
    body TEXT,
    score INT,
    number_of_replies DECIMAL(10,1),
    date DATE,
    time TIME,
    FOREIGN KEY (author) REFERENCES unique_authors(new_username) ON DELETE CASCADE
);

--- TABLE: toxicity_results
CREATE TABLE toxicity_results (
    post_id CHAR(7) PRIMARY KEY,
    toxic DECIMAL(3,2),
    severe_toxic DECIMAL(3,2),
    obscene DECIMAL(3,2),
    threat DECIMAL(3,2),
    insult DECIMAL(3,2),
    identity_hate DECIMAL(3,2),
    overall_toxicity DECIMAL(3,2),
    FOREIGN KEY (post_id) REFERENCES reddit_posts(post_id) ON DELETE CASCADE  
);

----------------------------------------------
-------- ALTER TABLES FOR THE DATABASE -------
----------------------------------------------;

--update table column - change column name
ALTER TABLE unique_authors
RENAME COLUMN new_name TO new_username;

----------------------------------------------
------------- Indexes ------------------------
----------------------------------------------;

-- Check if reddit_posts has index
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'reddit_posts';


-- Check if toxicity_results has index
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'toxicity_results';


--- Create indexes REDDIT_POSTS
CREATE INDEX idx_author ON reddit_posts (author);
CREATE INDEX idx_community ON reddit_posts (community);
CREATE INDEX idx_post_date ON reddit_posts (date);

--- Create indexes TOXICITY_RESULTS
CREATE INDEX idx_toxicity_post_id ON toxicity_results (post_id);
CREATE INDEX idx_overall_toxicity ON toxicity_results (overall_toxicity);

----------------------------------------------
--- Ownership of the tables ------------------
----------------------------------------------

--- Check ownership of the tables
SELECT tablename, tableowner 
FROM pg_tables 
WHERE schemaname = 'public';

-- Set ownership of the tables to the postgres user
ALTER TABLE public.unique_authors OWNER to postgres;
ALTER TABLE public.reddit_posts OWNER to postgres;
ALTER TABLE public.toxicity_results OWNER to postgres;

---------------------------------
--- drop tables -----------------
---------------------------------

DROP TABLE IF EXISTS reddit_posts, toxicity_results, unique_authors;


-- delete all data from reddit_posts table
DELETE FROM toxicity_results;

DROP TABLE IF EXISTS toxicity_results;