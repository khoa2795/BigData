-- Bài 3: Tìm aspect có nhiều đánh giá negative và positive nhất
-- 1. Top aspect có nhiều đánh giá negative nhất
-- 2. Top aspect có nhiều đánh giá positive nhất

raw_line = LOAD 'hotel-review.csv' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw_line GENERATE
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 1) AS id,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 2) AS review,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 3) AS aspect,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 4) AS category,
    REGEX_EXTRACT(line, '^(\\d+);(.*);([^;]+);([^;]+);([^;]+)$', 5) AS sentiment;

data = FILTER parsed BY id IS NOT NULL;

normalized = FOREACH data GENERATE
    aspect,
    LOWER(sentiment) AS sentiment;

negative = FILTER normalized BY sentiment == 'negative';
negative_count = FOREACH (GROUP negative BY aspect) GENERATE group AS aspect, COUNT(negative) AS total_negative;
top_negative_aspect = FOREACH (GROUP negative_count ALL) {
    sorted = ORDER negative_count BY total_negative DESC, aspect ASC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1) AS (aspect, total_negative);
};

positive = FILTER normalized BY sentiment == 'positive';
positive_count = FOREACH (GROUP positive BY aspect) GENERATE group AS aspect, COUNT(positive) AS total_positive;
top_positive_aspect = FOREACH (GROUP positive_count ALL) {
    sorted = ORDER positive_count BY total_positive DESC, aspect ASC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1) AS (aspect, total_positive);
};

STORE top_negative_aspect INTO 'output_bai3_top_negative_aspect' USING PigStorage(';');
STORE top_positive_aspect INTO 'output_bai3_top_positive_aspect' USING PigStorage(';');
