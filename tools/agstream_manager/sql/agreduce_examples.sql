-- ============================================================================
-- AGREDUCE UDF Examples
-- Aggregate semantic reduction for Flink SQL
-- ============================================================================

-- Register the UDAF (run this first)
CREATE TEMPORARY SYSTEM FUNCTION agreduce AS 'agreduce.agreduce' LANGUAGE PYTHON;

-- ============================================================================
-- BASIC EXAMPLES
-- ============================================================================

-- Example 1: Simple Summary
-- Aggregate all reviews into a single summary
-- Use JSON_VALUE to extract the field from the JSON result
SELECT JSON_VALUE(
    agreduce(
        customer_review,
        'summary',
        'str',
        'Summarize the overall sentiment and key themes from all reviews'
    ),
    '$.summary'
) as summary
FROM pr;

-- Example 2: Average Sentiment Score
-- Calculate aggregate sentiment score
SELECT CAST(JSON_VALUE(
    agreduce(
        customer_review,
        'avg_sentiment',
        'float',
        'Average sentiment score from 0 (very negative) to 1 (very positive)'
    ),
    '$.avg_sentiment'
) AS DOUBLE) as avg_sentiment
FROM pr;

-- Example 3: Count Total Complaints
-- Count how many complaints are mentioned
SELECT CAST(JSON_VALUE(
    agreduce(
        customer_review,
        'complaint_count',
        'int',
        'Total number of complaints or issues mentioned across all reviews'
    ),
    '$.complaint_count'
) AS INT) as complaint_count
FROM pr;

-- Example 4: Boolean Flag - Any Negative?
-- Check if there are any negative reviews
SELECT CAST(JSON_VALUE(
    agreduce(
        customer_review,
        'has_negative',
        'bool',
        'Are there any negative reviews in the dataset?'
    ),
    '$.has_negative'
) AS BOOLEAN) as has_negative
FROM pr;

-- ============================================================================
-- SUMMARIZATION EXAMPLES
-- ============================================================================

-- Example 5: Executive Summary
SELECT agreduce(
    customer_review,
    'executive_summary',
    'str',
    'Create a 2-3 sentence executive summary of all customer feedback'
) as summary
FROM pr;

-- Example 6: Top Themes
SELECT agreduce(
    customer_review,
    'top_themes',
    'str',
    'List the top 5 most common themes or topics mentioned, in order of frequency'
) as themes
FROM pr;

-- Example 7: Sentiment Breakdown
SELECT agreduce(
    customer_review,
    'sentiment_distribution',
    'str',
    'Provide percentage breakdown: X% positive, Y% neutral, Z% negative'
) as distribution
FROM pr;

-- Example 8: Key Insights
SELECT agreduce(
    customer_review,
    'key_insights',
    'str',
    'Identify 3-5 key insights or patterns from the customer feedback'
) as insights
FROM pr;

-- ============================================================================
-- FILTERED AGGREGATION
-- ============================================================================

-- Example 9: Summarize Only Negative Reviews
SELECT agreduce(
    customer_review,
    'negative_summary',
    'str',
    'Summarize the main complaints and issues from negative reviews'
) as summary
FROM pr
WHERE customer_review LIKE '%bad%'
   OR customer_review LIKE '%poor%'
   OR customer_review LIKE '%terrible%';

-- Example 10: Aggregate Recent Reviews (with LIMIT)
SELECT agreduce(
    customer_review,
    'recent_summary',
    'str',
    'Summarize the most recent customer feedback'
) as summary
FROM pr
LIMIT 100;

-- ============================================================================
-- GROUPED AGGREGATION
-- ============================================================================

-- Example 11: Summary by Category
-- Note: Assumes you have a category column or can extract it
SELECT
    category,
    agreduce(
        customer_review,
        'category_summary',
        'str',
        'Summarize customer feedback for this product category'
    ) as summary
FROM pr
GROUP BY category;

-- Example 12: Sentiment by Time Period
-- Note: Assumes you have a timestamp column
SELECT
    DATE_FORMAT(review_time, 'yyyy-MM-dd') as review_date,
    agreduce(
        customer_review,
        'daily_sentiment',
        'str',
        'Overall sentiment for this day: positive, neutral, or negative'
    ) as sentiment
FROM pr
GROUP BY DATE_FORMAT(review_time, 'yyyy-MM-dd');

-- ============================================================================
-- STATISTICAL AGGREGATION
-- ============================================================================

-- Example 13: Net Promoter Score
SELECT agreduce(
    customer_review,
    'nps_score',
    'float',
    'Calculate Net Promoter Score from 0-10 based on customer sentiment'
) as nps
FROM pr;

-- Example 14: Quality Score
SELECT agreduce(
    customer_review,
    'quality_score',
    'float',
    'Average product quality score from 1-5 based on reviews'
) as quality
FROM pr;

-- Example 15: Recommendation Rate
SELECT agreduce(
    customer_review,
    'recommendation_rate',
    'float',
    'Percentage of customers who would recommend (0.0 to 1.0)'
) as rate
FROM pr;

-- ============================================================================
-- ACTION-ORIENTED AGGREGATION
-- ============================================================================

-- Example 16: Action Items
SELECT agreduce(
    customer_review,
    'action_items',
    'str',
    'Generate 3-5 specific, actionable recommendations based on customer feedback'
) as actions
FROM pr;

-- Example 17: Priority Issues
SELECT agreduce(
    customer_review,
    'priority_issues',
    'str',
    'Identify the top 3 most critical issues that need immediate attention'
) as issues
FROM pr;

-- Example 18: Improvement Suggestions
SELECT agreduce(
    customer_review,
    'improvements',
    'str',
    'List specific product or service improvements suggested by customers'
) as suggestions
FROM pr;

-- ============================================================================
-- COMPARATIVE ANALYSIS
-- ============================================================================

-- Example 19: Compare Positive vs Negative
WITH positive_reviews AS (
    SELECT agreduce(
        customer_review,
        'positive_themes',
        'str',
        'What do customers love? List main positive themes'
    ) as themes
    FROM pr
    WHERE customer_review LIKE '%great%' OR customer_review LIKE '%excellent%'
),
negative_reviews AS (
    SELECT agreduce(
        customer_review,
        'negative_themes',
        'str',
        'What are the complaints? List main negative themes'
    ) as themes
    FROM pr
    WHERE customer_review LIKE '%bad%' OR customer_review LIKE '%poor%'
)
SELECT
    p.themes as positive_themes,
    n.themes as negative_themes
FROM positive_reviews p, negative_reviews n;

-- ============================================================================
-- REGISTRY MODE EXAMPLES
-- ============================================================================

-- Example 20: Using Pre-registered Schema
-- Assumes ReviewSummary schema is registered with fields:
-- - overall_sentiment: string
-- - positive_count: int
-- - negative_count: int
-- - neutral_count: int
-- - key_themes: string
-- - avg_rating: float

SELECT agreduce(
    customer_review,
    'ReviewSummary',
    'registry'
) as summary
FROM pr;

-- ============================================================================
-- WINDOWED AGGREGATION
-- ============================================================================

-- Example 21: Hourly Summary
-- Note: Requires timestamp column
SELECT
    TUMBLE_START(review_time, INTERVAL '1' HOUR) as window_start,
    agreduce(
        customer_review,
        'hourly_summary',
        'str',
        'Summarize customer sentiment for this hour'
    ) as summary
FROM pr
GROUP BY TUMBLE(review_time, INTERVAL '1' HOUR);

-- Example 22: Daily Trends
SELECT
    TUMBLE_START(review_time, INTERVAL '1' DAY) as day,
    agreduce(
        customer_review,
        'daily_trend',
        'str',
        'Describe the sentiment trend: improving, declining, or stable'
    ) as trend
FROM pr
GROUP BY TUMBLE(review_time, INTERVAL '1' DAY);

-- ============================================================================
-- COMPLEX QUERIES
-- ============================================================================

-- Example 23: Multi-level Aggregation
-- First aggregate by category, then aggregate the summaries
WITH category_summaries AS (
    SELECT
        category,
        agreduce(
            customer_review,
            'category_summary',
            'str',
            'Summarize feedback for this category'
        ) as summary
    FROM pr
    GROUP BY category
)
SELECT agreduce(
    summary,
    'overall_summary',
    'str',
    'Create an overall summary from all category summaries'
) as final_summary
FROM category_summaries;

-- Example 24: Combining with agmap
-- Use agmap for row-level extraction, then agreduce for aggregation
WITH extracted_sentiments AS (
    SELECT
        customer_review,
        T.sentiment
    FROM pr,
    LATERAL TABLE(agmap(customer_review, 'sentiment')) AS T(sentiment)
)
SELECT agreduce(
    sentiment,
    'sentiment_summary',
    'str',
    'Summarize the distribution of sentiments'
) as summary
FROM extracted_sentiments;

-- ============================================================================
-- PERFORMANCE TIPS
-- ============================================================================

-- Tip 1: Use LIMIT for testing
SELECT agreduce(customer_review, 'summary', 'str', 'Test summary') as summary
FROM pr
LIMIT 10;  -- Start small, then increase

-- Tip 2: Filter before aggregating
SELECT agreduce(customer_review, 'summary', 'str', 'Summarize valid reviews') as summary
FROM pr
WHERE customer_review IS NOT NULL
  AND LENGTH(customer_review) > 10;

-- Tip 3: Use specific time ranges
SELECT agreduce(customer_review, 'summary', 'str', 'Recent feedback summary') as summary
FROM pr
WHERE review_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY;

-- ============================================================================
-- TROUBLESHOOTING QUERIES
-- ============================================================================

-- Check if UDAF is registered
SHOW FUNCTIONS;

-- Count rows to be aggregated
SELECT COUNT(*) as row_count FROM pr;

-- Check for NULL values
SELECT
    COUNT(*) as total_rows,
    COUNT(customer_review) as non_null_reviews,
    COUNT(*) - COUNT(customer_review) as null_reviews
FROM pr;

-- Sample data before aggregating
SELECT customer_review FROM pr LIMIT 5;

-- ============================================================================
-- END OF EXAMPLES
-- ============================================================================

-- Made with Bob
