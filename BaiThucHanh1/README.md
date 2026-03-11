

> Tài liệu này áp dụng chung cho tất cả các bài thực hành: `BaiThucHanh1`, `BaiThucHanh2`, ..., `BaiThucHanhN`.

---

## Cấu trúc thư mục

```text
BaiThucHanhN/
├── bai_thuc_hanh/      ← Đề bài và dữ liệu đầu vào
├── source_code/        ← Mã nguồn Java (MapReduce)
└── output/             ← Kết quả đầu ra sau khi chạy
```

---

## Dữ liệu đầu vào — `bai_thuc_hanh/`

| File | Mô tả |
|---|---|
| `assignments.ipynb` | Đề bài và hướng dẫn |
| `movies.txt` | Danh sách phim `(MovieID, Title, Genres)` |
| `ratings_1.txt` | Dữ liệu đánh giá phần 1 `(UserID, MovieID, Rating, Timestamp)` |
| `ratings_2.txt` | Dữ liệu đánh giá phần 2 `(UserID, MovieID, Rating, Timestamp)` |
| `users.txt` | Thông tin người dùng `(UserID, Gender, Age, Occupation, Zip-code)` |

---

## Mã nguồn — `source_code/`

Mỗi bài được viết trong một file Java riêng, đặt tên theo quy ước `BaiX.java`:

| File | Nội dung |
|---|---|
| `Bai1.java` | Tính điểm trung bình và tổng số lượt đánh giá cho mỗi phim |
| `Bai2.java` | Phân tích đánh giá theo thể loại phim |
| `Bai3.java` | Phân tích đánh giá theo giới tính người dùng |
| `Bai4.java` | Phân tích đánh giá theo nhóm tuổi người dùng |

---

## Kết quả đầu ra — `output/`

Sau khi chạy, kết quả được lưu theo quy ước `outputX.txt`:

| File | Kết quả của |
|---|---|
| `output1.txt` | Bài 1 |
| `output2.txt` | Bài 2 |
| `output3.txt` | Bài 3 |
| `output4.txt` | Bài 4 |

---

## Áp dụng cho bài tiếp theo

Khi tạo `BaiThucHanh2`, `BaiThucHanh3`, ... chỉ cần:
1. Tạo thư mục gốc mới với tên tương ứng.
2. Giữ nguyên 3 thư mục con: `bai_thuc_hanh/`, `source_code/`, `output/`.
3. Đặt tên file nguồn theo `BaiX.java` và kết quả theo `outputX.txt`.

