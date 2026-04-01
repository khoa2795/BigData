-- Bài 5: Top 5 từ liên quan nhất theo từng category (dựa trên TF-IDF)
-- Tính TF-IDF score để tìm các từ đặc trưng của mỗi category

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
    category,
    TRIM(REPLACE(LOWER(review), '[^\\p{L}\\p{N}\\s]', ' ')) AS cleaned_review;

tokens = FOREACH normalized GENERATE
    category,
    FLATTEN(TOKENIZE(cleaned_review)) AS word;

tokens_non_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';

joined = JOIN tokens_non_empty BY word LEFT OUTER, stopwords BY stopword;
clean_tokens = FOREACH (FILTER joined BY stopwords::stopword IS NULL) GENERATE
    tokens_non_empty::category AS category,
    tokens_non_empty::word AS word;

tf = FOREACH (GROUP clean_tokens BY (category, word)) GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(clean_tokens) AS tf;

cat_word = DISTINCT (FOREACH clean_tokens GENERATE category, word);
df = FOREACH (GROUP cat_word BY word) GENERATE group AS word, COUNT(cat_word) AS df;

cat_distinct = DISTINCT (FOREACH clean_tokens GENERATE category);
cat_count_rel = GROUP cat_distinct ALL;
cat_count_tmp = FOREACH cat_count_rel GENERATE COUNT(cat_distinct) AS total_categories;

tf_df = JOIN tf BY word, df BY word;
tf_df_all = CROSS tf_df, cat_count_tmp;

scored = FOREACH tf_df_all GENERATE
    tf::category AS category,
    tf::word AS word,
    tf::tf AS tf,
    df::df AS df,
    ((double)tf::tf) * LOG((double)cat_count_tmp::total_categories / (double)df::df) AS score;

top5_related_by_category = FOREACH (GROUP scored BY category) {
    ranked = ORDER scored BY score DESC, word ASC;
    top5 = LIMIT ranked 5;
    GENERATE FLATTEN(top5) AS (category, word, tf, df, score);
};

STORE top5_related_by_category INTO 'output_bai5_top5_related_words_by_category' USING PigStorage(';');
