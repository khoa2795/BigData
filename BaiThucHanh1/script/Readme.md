# Hướng dẫn chạy chương trình MovieRatingAnalysis trên Hadoop

Dưới đây là các lệnh bạn cần dùng nếu bạn muốn tự gõ vào Terminal để biên dịch và chạy file `MovieRatingAnalysis.java`.

### Bước 1: Khai báo lại biến môi trường (Nếu Terminal mới mở)
Mỗi khi mở một Terminal mới, bạn cần chắc chắn hệ thống biết Hadoop và Java nằm ở đâu:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

### Bước 2: Khởi động Hadoop (RẤT QUAN TRỌNG nếu máy vừa mới bật lại)
Nếu bạn nhận được lỗi `Connection refused` khi chạy lệnh `hdfs`, nghĩa là máy chủ Hadoop đang ở trạng thái TẮT. Để bật lại, bạn trỏ lệnh:
```bash
start-dfs.sh
start-yarn.sh
```
*(Để kiểm tra xem hệ thống đã lên chưa, bạn gõ lệnh `jps`. Nếu kết quả hiển thị đủ 5 dịch vụ (NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager) thì có nghĩa là Hadoop đã hoạt động bình thường).*

### Bước 3: Biên dịch file Java thành file `.class`
Vào thư mục chứa file Java:
```bash
cd ~/BigData/BaiThucHanh1/source_code
javac -classpath $(${HADOOP_HOME}/bin/hadoop classpath) MovieRatingAnalysis.java
```

### Bước 4: Đóng gói các file `.class` thành file `.jar`
Hadoop yêu cầu chương trình phải ở định dạng `.jar` để có thể chạy trên nhiều Node:
```bash
jar cf MovieRatingAnalysis.jar MovieRatingAnalysis*.class
```

### Bước 5: Đưa dữ liệu lên HDFS và Xóa thư mục cũ
Khác với Windows, Hadoop yêu cầu dữ liệu phải được đưa lên HDFS trước:
```bash
hdfs dfs -mkdir -p /user/khoa/BaiThucHanh1/bai_thuc_hanh
hdfs dfs -put -f ../bai_thuc_hanh/movies.txt ../bai_thuc_hanh/ratings_1.txt ../bai_thuc_hanh/ratings_2.txt ../bai_thuc_hanh/users.txt /user/khoa/BaiThucHanh1/bai_thuc_hanh/
hdfs dfs -rm -r -f /user/khoa/BaiThucHanh1/output
```

### Bước 6: Chạy chương trình MapReduce
Cú pháp: `hadoop jar <File_Jar> <Tên_Class_Chính> <Đường_Dẫn_Input_1> <Đường_Dẫn_Input_2> <Đường_Dẫn_Cache> <Đường_Dẫn_Output>`
```bash
hadoop jar MovieRatingAnalysis.jar MovieRatingAnalysis /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_1.txt /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_2.txt /user/khoa/BaiThucHanh1/bai_thuc_hanh/movies.txt /user/khoa/BaiThucHanh1/output
```

### Bước 7: Xem kết quả đầu ra
Xem file kết quả sinh ra bởi Reducer (thường có tên là `part-r-00000`):
```bash
hdfs dfs -cat /user/khoa/BaiThucHanh1/output/part-r-00000
```

---
*Lưu ý: Bạn cũng có thể dùng trực tiếp file script `./run_bai1.sh` để máy tính thực thi toàn bộ các Bước trên một cách tự động.*
