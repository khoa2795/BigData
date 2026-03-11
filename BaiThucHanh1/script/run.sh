#!/bin/bash

# Thiết lập biến môi trường cơ bản
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

cd /home/khoa/BigData/BaiThucHanh1/source_code

# Đưa dữ liệu lên HDFS một lần dùng chung cho cả 2 bài
echo "🧹 Đang đồng bộ hóa dữ liệu lên hệ thống HDFS..."
hdfs dfs -mkdir -p /user/khoa/BaiThucHanh1/bai_thuc_hanh
hdfs dfs -put -f ../bai_thuc_hanh/movies.txt ../bai_thuc_hanh/ratings_1.txt ../bai_thuc_hanh/ratings_2.txt ../bai_thuc_hanh/users.txt /user/khoa/BaiThucHanh1/bai_thuc_hanh/

# Hàm chạy chung cho các bài
run_job() {
    local BAI_NUM=$1
    local CLASS_NAME=$2
    local OUTPUT_HDFS="/user/khoa/BaiThucHanh1/output_bai${BAI_NUM}"
    local OUTPUT_LOCAL="/home/khoa/BigData/BaiThucHanh1/output/output${BAI_NUM}.txt"

    echo "======================================"
    echo "🚀 BẮT ĐẦU CHẠY BÀI ${BAI_NUM}: ${CLASS_NAME}"
    echo "======================================"

    # 1. Biên dịch file Java
    echo "⏳ Đang biên dịch ${CLASS_NAME}.java ..."
    javac -classpath $(${HADOOP_HOME}/bin/hadoop classpath) ${CLASS_NAME}.java
    if [ $? -ne 0 ]; then
        echo "❌ Lỗi: Không thể biên dịch file Java ${CLASS_NAME}!"
        exit 1
    fi

    # 2. Đóng gói file JAR
    echo "⏳ Đang đóng gói thành ${CLASS_NAME}.jar ..."
    jar cf ${CLASS_NAME}.jar ${CLASS_NAME}*.class

    # 3. Dọn dẹp thư mục output cũ trên HDFS
    hdfs dfs -rm -r -f ${OUTPUT_HDFS}

    # 4. Chạy MapReduce Job
    echo "🚀 Đang gửi MapReduce Job lên Hadoop..."
    echo "--------------------------------------"
    if [ "${CLASS_NAME}" = "Bai3" ] || [ "${CLASS_NAME}" = "Bai4" ]; then
        hadoop jar ${CLASS_NAME}.jar ${CLASS_NAME} \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_1.txt \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_2.txt \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/movies.txt \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/users.txt \
            ${OUTPUT_HDFS}
    else
        hadoop jar ${CLASS_NAME}.jar ${CLASS_NAME} \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_1.txt \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/ratings_2.txt \
            /user/khoa/BaiThucHanh1/bai_thuc_hanh/movies.txt \
            ${OUTPUT_HDFS}
    fi
    echo "--------------------------------------"

    # 5. Tải kết quả về local
    echo "💾 Đang trích xuất kết quả ra file 'output${BAI_NUM}.txt'..."
    rm -f "${OUTPUT_LOCAL}"
    hdfs dfs -getmerge ${OUTPUT_HDFS}/part-r-* "${OUTPUT_LOCAL}"
    rm -f "/home/khoa/BigData/BaiThucHanh1/output/.output${BAI_NUM}.txt.crc"
    
    echo "🎉 BÀI ${BAI_NUM} HOÀN THÀNH!"
    echo "✅ Kết quả tải về tại: ${OUTPUT_LOCAL}"
    echo "================================================================"
    cat "${OUTPUT_LOCAL}"
    echo "================================================================"
    echo ""
}

# Xử lý tham số truyền vào khi chạy script
case "$1" in
    1)
        run_job 1 "Bai1"
        ;;
    2)
        run_job 2 "Bai2"
        ;;
    3)
        run_job 3 "Bai3"
        ;;
    4)
        run_job 4 "Bai4"
        ;;
    "all"|"")
        run_job 1 "Bai1"
        run_job 2 "Bai2"
        run_job 3 "Bai3"
        run_job 4 "Bai4"
        ;;
    *)
        echo "Cú pháp: ./run.sh [1 | 2 | 3 | 4 | all]"
        echo "  - ./run.sh 1   : Chỉ chạy Bài 1"
        echo "  - ./run.sh 2   : Chỉ chạy Bài 2"
        echo "  - ./run.sh 3   : Chỉ chạy Bài 3"
        echo "  - ./run.sh 4   : Chỉ chạy Bài 4"
        echo "  - ./run.sh all : Chạy cả Bài 1, 2, 3, 4 (mặc định nếu không nhập)"
        exit 1
        ;;
esac
