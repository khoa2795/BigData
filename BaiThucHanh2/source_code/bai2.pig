-- Bài 2: Thống kê và phân tích dữ liệu
-- 1. Top 5 từ xuất hiện nhiều nhất
-- 2. Thống kê số lượng comment theo category
-- 3. Thống kê số lượng comment theo aspect

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
    id,
    TRIM(REPLACE(LOWER(review), '[^\\p{L}\\p{N}\\s]', ' ')) AS cleaned_review,
    aspect,
    category,
    LOWER(sentiment) AS sentiment;

tokens = FOREACH normalized GENERATE
    FLATTEN(TOKENIZE(cleaned_review)) AS word,
    aspect,
    category,
    sentiment;

tokens_non_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';

joined = JOIN tokens_non_empty BY word LEFT OUTER, stopwords BY stopword;
clean_tokens = FOREACH (FILTER joined BY stopwords::stopword IS NULL) GENERATE
    tokens_non_empty::word AS word,
    tokens_non_empty::aspect AS aspect,
    tokens_non_empty::category AS category,
    tokens_non_empty::sentiment AS sentiment;

word_freq = FOREACH (GROUP clean_tokens BY word) GENERATE group AS word, COUNT(clean_tokens) AS freq;
top5_words = FOREACH (GROUP word_freq ALL) {
    sorted = ORDER word_freq BY freq DESC, word ASC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5) AS (word, freq);
};

category_count = FOREACH (GROUP data BY category) GENERATE group AS category, COUNT(data) AS total_comments;
category_count_ordered = FOREACH (GROUP category_count ALL) {
    sorted = ORDER category_count BY total_comments DESC, category ASC;
    GENERATE FLATTEN(sorted) AS (category, total_comments);
};

aspect_count = FOREACH (GROUP data BY aspect) GENERATE group AS aspect, COUNT(data) AS total_comments;
aspect_count_ordered = FOREACH (GROUP aspect_count ALL) {
    sorted = ORDER aspect_count BY total_comments DESC, aspect ASC;
    GENERATE FLATTEN(sorted) AS (aspect, total_comments);
};

STORE top5_words INTO 'output_bai2_top5_words' USING PigStorage(';');
STORE category_count_ordered INTO 'output_bai2_by_category' USING PigStorage(';');
STORE aspect_count_ordered INTO 'output_bai2_by_aspect' USING PigStorage(';');
