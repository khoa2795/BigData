-- Bài 1: Xử lý dữ liệu hotel review
-- 1. Đưa tất cả ký tự về chữ thường (lowercase)
-- 2. Tách các dòng bình luận thành dãy các từ (tokenize)
-- 3. Loại bỏ các stop word

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
    id,
    FLATTEN(TOKENIZE(cleaned_review)) AS word,
    aspect,
    category,
    sentiment;

tokens_non_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';

joined = JOIN tokens_non_empty BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;

result = FOREACH filtered GENERATE
    tokens_non_empty::id AS id,
    tokens_non_empty::word AS word,
    tokens_non_empty::aspect AS aspect,
    tokens_non_empty::category AS category,
    tokens_non_empty::sentiment AS sentiment;

STORE result INTO 'output_bai1' USING PigStorage(';');
