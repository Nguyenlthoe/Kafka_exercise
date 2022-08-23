# Kafka_exercise in Vccorp intern
## Đề bài:
Xử lý lưu trữ dữ liệu theo CampainID vào hdfs đảm bảo các truy vấn sau
    1. Xác định số lượng click and view từng campain với cov = 1 là log click, cov = 0 là view <br>
    2. Xác định số lượng location <br>
    3. Xác định lượng user từng campain <br>
    4. Xác định số lượng user vào nhiều campain <br>
Các trường dữ liệu <br>
{  <br>
timeNow: thời gian xuất hiện log( thời gian lên quảng cáo)  <br>
ip:  <br>
userAgent:  <br>
guidInfo.GUIDTime:   <br>
bannerId:  id của banner quảng cáo. <br> 
viewCount:  <br>
guidInfo.GUID: Định danh người dùng duy nhất.<br>  
admDomain:  <br>
tp:  <br>
cov:  <br>
zoneID:vị trí quảng cáo lên website.<br>  
campaign: một id thể hiện chiến dịch quảng cáo gồm 1 list các banner. <br> 
channelID:  <br>
isNew:  <br>
referer:  <br>
regionInfo.Value <br>  
tid  <br>
price: giá lên quảng cáo tại vị trí zoneId.  <br>
}<br>
## Submit job spark 

### Đóng gói file jar
Chạy <b><i>cd kafkatask</b></i> và <b><i>mvn package</i></b>

## Ghi dữ liệu vào hdfs 
Chạy <b><i>bin/spark-submit  --deploy-mode client  --master yarn --class io.WriteKafka [link to file kafkatask-1.0-jar-with-dependencies.jar] 
### 1. Xác định số lượng click and view từng campain với cov = 1 là log click, cov = 0 là view 

Chạy <b><i>bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountClickAndView [link to file kafkatask-1.0-jar-with-dependencies.jar] </i></b> <br>
Ví dụ: bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountClickAndView /home/nguyenlt/kafka/Kafka_exercise/kafkatask/target/kafkatask-1.0-jar-with-dependencies.jar
### Xác định số lượng location
Chạy <b><i>bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountLocation [link to file kafkatask-1.0-jar-with-dependencies.jar] </i></b> <br>
### Xác định user từng campain
Chạy <b><i>bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountUser [link to file kafkatask-1.0-jar-with-dependencies.jar] </i></b> <br>
### Xác định số user vào nhiều campain
Chạy <b><i>bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountUserOfCampains [link to file kafkatask-1.0-jar-with-dependencies.jar] [list campainID] </i></b> <br>
Ví dụ: bin/spark-submit  --deploy-mode client  --master yarn --class todo.CountUserOfCampains /home/nguyenlt/kafka/Kafka_exercise/kafkatask/target/kafkatask-1.0-jar-with-dependencies.jar 203611 206222
