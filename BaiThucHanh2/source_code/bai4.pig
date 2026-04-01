-- Bài 4: Top 5 từ positive/negative theo từng category
-- 1. Top 5 từ positive xuất hiện nhiều nhất theo từng category
-- 2. Top 5 từ negative xuất hiện nhiều nhất theo từng category

raw_line = LOAD 'hotel-review.csv' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw_line GENERATE
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 1) AS id,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 2) AS review,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 3) AS aspect,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 4) AS category,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 5) AS sentiment;

data = FILTER parsed BY id IS NOT NULL;

stop_raw = LOAD 'stopwords.txt' USING PigStorage('\n') AS (stopword:chararray);
stopwords = DISTINCT (FOREACH stop_raw GENERATE TRIM(LOWER(stopword)) AS stopword);

normalized = FOREACH data GENERATE
    TRIM(REPLACE(LOWER(review), '[^\\p{L}\\p{N}\\s]', ' ')) AS cleaned_review,
    category,
    LOWER(sentiment) AS sentiment;

tokens = FOREACH normalized GENERATE
    category,
    sentiment,
    FLATTEN(TOKENIZE(cleaned_review)) AS word;

tokens_non_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';

joined = JOIN tokens_non_empty BY word LEFT OUTER, stopwords BY stopword;
clean_tokens = FOREACH (FILTER joined BY stopwords::stopword IS NULL) GENERATE
    tokens_non_empty::category AS category,
    tokens_non_empty::sentiment AS sentiment,
    tokens_non_empty::word AS word;

pos = FILTER clean_tokens BY sentiment == 'positive';
pos_word_count = FOREACH (GROUP pos BY (category, word)) GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(pos) AS freq;

pos_top5_by_category = FOREACH (GROUP pos_word_count BY category) {
    ranked = ORDER pos_word_count BY freq DESC, word ASC;
    top5 = LIMIT ranked 5;
    GENERATE FLATTEN(top5) AS (category, word, freq);
};

neg = FILTER clean_tokens BY sentiment == 'negative';
neg_word_count = FOREACH (GROUP neg BY (category, word)) GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(neg) AS freq;

neg_top5_by_category = FOREACH (GROUP neg_word_count BY category) {
    ranked = ORDER neg_word_count BY freq DESC, word ASC;
    top5 = LIMIT ranked 5;
    GENERATE FLATTEN(top5) AS (category, word, freq);
};

STORE pos_top5_by_category INTO 'output_bai4_top5_positive_words_by_category' USING PigStorage(';');
STORE neg_top5_by_category INTO 'output_bai4_top5_negative_words_by_category' USING PigStorage(';');
