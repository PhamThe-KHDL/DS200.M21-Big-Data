# DS200.M21 - Big Data

DS200.M21 - Phân Tích Dữ Liệu Lớn

Học kỳ 2 Năm 3 Năm học 2021-2022 

**Giảng Viên:** 
- Đỗ Trọng Hợp

## Nội Dung Môn Học

| Buổi | Ngày | Nội Dung | Tóm Tắt | LT/TH | Slide | Code | Video Record |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| 01 | 24/02/2022 | Giới Thiệu Dữ Liệu Lớn |  | LT-01 | [Slide 1 Big data Introduction](https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/L%C3%9D%20THUY%E1%BA%BET/Slide%201%20Big%20data%20Introduction.pdf) |  | [01 - Giới Thiệu Dữ Liệu Lớn](https://youtu.be/2wF4T9satZU) |
| 02 | 02/03/2022 | GFS and Hadoop |  | LT-02 | [Slide 2 GFS and Hadoop](https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/L%C3%9D%20THUY%E1%BA%BET/Slide%202%20GFS%20and%20Hadoop.pdf) |  | [02 - GFS and Hadoop](https://youtu.be/m9rWekyOdUs) |
| 03 | 09/03/2022 |  GFS and Hadoop, Hadoop MapReduce Tutorial |  | LT-03 | [Slide 2 GFS and Hadoop](https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/L%C3%9D%20THUY%E1%BA%BET/Slide%202%20GFS%20and%20Hadoop.pdf) <br /> [Slide 3 Hadoop MapReduce Tutorial](https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/L%C3%9D%20THUY%E1%BA%BET/Slide%203%20Hadoop%20MapReduce%20Tutorial.pdf) |  | [03 -  GFS and Hadoop, Hadoop MapReduce Tutorial](https://youtu.be/h5kh2mJ4XmM) |
| 04 | 10/03/2022 | Hadoop MapReduce Tutorial |  | TH-01 | [Slide 3 Hadoop MapReduce Tutorial](https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/L%C3%9D%20THUY%E1%BA%BET/Slide%203%20Hadoop%20MapReduce%20Tutorial.pdf) |  | [LAB01 - ]() |
| 05 |  |  |  |  |  |  |  |
| 06 |  |  |  |  |  |  |  |
| 07 |  |  |  |  |  |  |  |
| 08 |  |  |  |  |  |  |  |
| 09 |  |  |  |  |  |  |  |
| 10 |  |  |  |  |  |  |  |
| 11 |  |  |  |  |  |  |  |
| 12 |  |  |  |  |  |  |  |
| 13 |  |  |  |  |  |  |  |
| 14 |  |  |  |  |  |  |  |
| 15 |  |  |  |  |  |  |  |
| 16 |  |  |  |  |  |  |  |
| 17 |  |  |  |  |  |  |  |
| 18 |  |  |  |  |  |  |  |
| 19 |  |  |  |  |  |  |  |
| 20 |  |  |  |  |  |  |  |




## Môi Trường/Công Cụ Thực Hành

### Cài đặt máy ảo: [Download VirtualBox](https://www.virtualbox.org/wiki/Downloads)


### Cài đặt Cloudera: [Download Cloudera Quickstart VM for VirtualBox](https://downloads.cloudera.com/demo_vm/virtualbox/cloudera-quickstart-vm-5.13.0-0-virtualbox.zip) [[Drive](https://drive.google.com/file/d/1x8MqHsm6zYJ6lQZqw22ajRpktyPqn24n/view?usp=sharing)]

Sau khi đã tải đầy đủ các file cần thiết, chúng ta sẽ tiến hành cài đặt máy ảo Cloudera:

- **Bước 1**: Từ giao diện VirtualBox ta chọn vào **Import** (Tools -> Import)

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/0%20-%20GiaoDienVirtualBox.PNG" width="800" alt="GiaoDienVirtualBox" />
</center>

- **Bước 2**: Màn hình sẽ xuất hiện như hình dưới. Tiếp theo, chúng ta sẽ chọn file máy ảo.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/1%20-%20Import.PNG" width="800" alt="Import" />
</center>

- **Bước 3**: Tìm đến đường dẫn chứa file máy ảo Cloudera, nhấn chọn vào file và nhấn **Open**.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/2%20-%20TimFileImport.PNG" width="800" alt="TimFileImport" />
</center>

- **Bước 4**: Sau khi đã chọn được file, ta nhấn **Next**.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/3%20-%20CaiDat.PNG" width="800" alt="CaiDat" />
</center>

- **Bước 5**: VirtualBox sẽ hiện ra các thông tin máy ảo như hình dưới và chúng ta tiến hành cài đặt bằng cách nhấn vào **Import**.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/4%20-%20TongTinMayAo.PNG" width="800" alt="TongTinMayAo" />
</center>

- **Bước 6**: Chờ 1 khoảng thời gian để máy ảo cài đặt. Sau khi đã cài đặt xong, ta tiến hành mở máy ảo lên để chạy thử.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/5%20-%20Finish.PNG" width="800" alt="Finish" />
</center>

- **Bước 7**: Giao diện của máy ảo Cloudera như hình dưới. Đến đây là chúng ta đã cài đặt xong máy ảo Cloudera để thực hành **Phân Tích Dữ Liệu Lớn**.

<center>
    <img src="https://github.com/PhamThe-KHDL/DS200.M21-Big-Data/blob/main/Image/6%20-%20Done.PNG" width="800" alt="Done" />
</center>


## Tham Khảo Thêm

- [Learn Hadoop](https://www.tutorialspoint.com/hadoop/index.htm)
- [Learn Sqoop](https://www.tutorialspoint.com/sqoop/index.htm)
- [Learn Hive](https://www.tutorialspoint.com/hive/index.htm)
- [Learn Apache Pig](https://www.tutorialspoint.com/apache_pig/index.htm)
- [Hadoop Tutorial](https://data-flair.training/blogs/hadoop-tutorial/)
- [Apache Sqoop Tutorial](https://data-flair.training/blogs/apache-sqoop-tutorial/)

## Thực hiện

```
Phạm Đức Thể

Thể ~/~
```


