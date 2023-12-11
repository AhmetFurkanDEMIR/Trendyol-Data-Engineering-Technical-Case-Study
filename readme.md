![](https://img.shields.io/badge/Debian-A81D33?style=for-the-badge&logo=debian&logoColor=white) ![](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white) ![](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16) ![](https://img.shields.io/badge/Scala-DC322F?style=for-the-badge&logo=scala&logoColor=white)

# Trendyol Data Engineering Technical Case

Bu Case çalışmasında iki ayrı görev için iki ayrı Job yazılmıştır. Kodlar **Scala** ile geliştirilmiş ve **SQL** kullanılmamıştır. Spark, bir master iki worker olacak şekilde docker ile deploy edilmiştir.

### **Veri**

```
orders
 |-- customer_id: string
 |-- location:    string
 |-- seller_id:   string
 |-- order_date:  string
 |-- order_id:    string
 |-- price:       double
 |-- product_id:  string
 |-- status:      string

products
 |-- brandname:    string
 |-- categoryname: string
 |-- productid:    string
 |-- productname:  string
```

#### *Problem 1 - Job1*

Her ürün için aşağıdakileri hesaplayın ve dosya sistemine sonuçları yazın:
- net satış adedi ve tutarı (toplam satış adetleri - iptal / iade adetleri)
- brüt satış adedi ve tutarı (toplam satış adetleri)
- **satıldığı** son 5 gündeki satış sayısı ortalamaları
- en çok sattığı yer

#### *Problem 2 - Job2*

Ürün bazlı fiyat değişimlerini tarihleriyle birlikte çıkarıp dosya sistemine yazın.

Burada `orders` verisi üzerinde aynı `product_id`'nin sonraki order'da fiyatı arttıysa `rise`, azaldıysa `fall` gibi bir çıktı üretebilirsiniz.


## **Çözüm**

* [Start.sh](/start.sh)

* [docker-compose.yml](/docker-compose.yml)

* [raw-data](/raw-data)

    * [Orders.json](/raw-data/orders.json)

    * [Products.json](/raw-data/products.json)

* [out_data](/out_data/)

    * [Job1 Out](/out_data/job1/_temporary/0/task_202312110730164091740487195813030_0050_m_000000/part-00000-7d5b6f8e-db19-4066-aec9-f332a8026744-c000.csv)

    * [Job2 Out](/out_data/job2/_temporary/0/task_202312110730004615437569356012626_0012_m_000000/part-00000-9b4f1c2a-04d3-4ca4-987b-3d1b70934676-c000.csv)

    * [Output Join](/out_data/output_join/_temporary/0/task_202312110729476044888576980888494_0005_m_000000/part-00000-0ae58b24-83d3-44c6-a465-36fe44908fa2-c000.csv)

* [spark-submitter](/spark-submitter)

    * [Dockerfile](/spark-submitter/Dockerfile)

    * [spark-submit.sh](/spark-submitter/spark-submit.sh)
    
    * [Job1.jar](/spark-submitter/Job1-1.0-SNAPSHOT-jar-with-dependencies.jar)

    * [Job2.jar](/spark-submitter/Job2-1.0-SNAPSHOT-jar-with-dependencies.jar)

* [spark-scala-source-code](/spark-scala-source-code)

    * [Job1](/spark-scala-source-code/Job1/)

        * [Job1.scala](/spark-scala-source-code/Job1/src/main/scala/myPackage/Job1.scala)

        * [Transactions.scala](/spark-scala-source-code/Job1/src/main/scala/myPackage/Transactions.scala)

    * [Job2](/spark-scala-source-code/Job2/)

        * [Job2.scala](/spark-scala-source-code/Job2/src/main/scala/myPackage/Job2.scala)

        * [Transactions.scala](/spark-scala-source-code/Job2/src/main/scala/myPackage/Transactions.scala)



#### **İşlemleri başlatma | Çıktıları hesaplama**

![](/images/schema.png)

Bu bölümde, Docker imajlarını otomatik olarak başlatarak iki görevin de tamamlanmasını ve çıktıların dosya sistemine yazılmasını sağlayacağız.

**Gereklilikler**

* Bilgisayarınızda [Docker](https://docs.docker.com/engine/install/)'ın kurulu olduğundan emin olun.

* Bilgisayarınızda 8080 ve 7077 portlarının boşta ve kullanılabilir olduğundan emin olun.

İşlemlerin başlayabilmesi için [start.sh](/start.sh) dosyasını çalıştırmanız gerekiyor. Bu dosyadaki komutlar Linux komutları içerdiği için, Debian(Ubuntu) tabanlı bir işletim sistemini tercih etmeniz önerilir. Eğer Windows kullanıyorsanız, lütfen dosya içindeki komutları tek tek elle çalıştırın.


```terminal
# işlemlerin başlaması

sudo bash ./start.sh
```

Bu dosyanın çalıştırılmasıyla birlikte, sırasıyla şu adımlar gerçekleştirilecek:

    1. Eğer mevcut bir imaj çalışıyorsa, bu imaj durdurulacak.
    
    2. out-data klasörü temizlenecek, yani içindeki dosyalar silinecek.
    
    3. spark-network adında bir ağ oluşturulacak.
    
    4. İmajlar yeniden oluşturulacak (build) ve ayağa kaldırılacak.

**konteynerların ayağa kalkması**

![](/images/docker_run.png)

Ardından, Spark master ve iki worker ayağa kaldırılacak. Spark işlemleri tamamlandıktan sonra, spark-submit adlı konteyner, joblarımızın jar dosyalarını çalıştıracak ve jobların başarıyla tamamlanmasını bekleyecektir. Spark UI'ı görüntülemek için tarayıcınızı açın ve [0.0.0.0:8080](http://0.0.0.0:8080/) adresine gidin.

**Spark kaynak yönetimi**

* **Spark Master** CPU:8, RAM: 16

* **Spark Worker-1** CPU:8, RAM: 8

* **SPARK Worker-2** CPU:8, RAM: 8

**Spark UI [0.0.0.0:8080](http://0.0.0.0:8080/)**

![](/images/spark_ui.png)

Joblar başarıyla tamamlandıktan sonra, *worker1* ve *worker2* üzerinden elde edilen çıktılar alınacak ve bu çıktılar [out_data](/out_data/) klasörüne taşınacaktır.

Tüm bu işlemler, genellikle 2-3 dakika içerisinde tamamlanır ve çıktı dosyalarımız kullanılabilir hale gelir.

**Çıktılar**

![](/images/out_data.png)

*Çıktı isimleri değişiklik gösterebilir*

#### **Spark&Scala Kod Çözümü**

Çözümde **Spark'ın 3.5.0** sürümü, **Java'nın 8** sürümü ve **Scala'nın 2.12.8** sürümü kullanılmıştır. Spark kütüphanesini bağımlılıklar için çözüm sürecinde Maven kullanılmıştır.

**Job1**

Spark, *myPackage.Job1* sınıfını çalıştırır. Bu sınıf içinde, iki dosya okunduktan sonra *Transactions* sınıfında dosyalar birleştirilir, işlemler tamamlanır ve elde edilen yeni çıktı dosya sistemine yazılır.

*İşlemler ve metodları*

* dosyaları joinleme **:** Transactions.joinDataFrames

* net satış adedi ve tutarı (toplam satış adetleri - iptal / iade adetleri) **:** Transactions.calculateNetSales

* brüt satış adedi ve tutarı (toplam satış adetleri) **:** Transactions.calculateGrossSales

* satıldığı son 5 gündeki satış sayısı ortalamaları **:** Transactions.calculateAverageSalesPerProduct

* en çok sattığı yer **:** Transactions.calculateMostSoldLocationForAllProducts

* verilerin joinlenmesi **:** Transactions.joinFinalData

* verilerin dosya sistemine yazılması **:** Transactions.writeDataFrame


**Job2**

Spark, *myPackage.Job2* sınıfını çalıştırır. Bu sınıf içinde, iki dosya okunduktan sonra *Transactions* sınıfında dosyalar birleştirilir, işlemler tamamlanır ve elde edilen iki yeni çıktı dosya sistemine yazılır.

* dosyaları joinleme **:** Transactions.joinDataFrames

* Joinlenmiş datayı dosya sistemine yazma **:** Transactions.writeJoinData

* Ürün bazlı fiyat değişimleri tarihleriyle birlikte **:** Transactions.calculatePriceChanges

* oluşan çıktıyı dosya sistemine yazma **:** Transactions.writeDataFrame

#### **Oluşabilecek Hatalar**

* **Port Çakışması** 8080 ve 7077 portlarının boş olduğundan emin olun.

* **Docker'ın kurulu olduğundan emin olunuz**

* **Docker sürümü** bazı sürümlerde *docker-compose up* kullanılırken bazı sürümlerde ise *docker compose up* kullanılır. Hata alırsanız bu iki komuttan birini kullanarak çözünüz.

**[Ahmet Furkan Demir](https://ahmetfurkandemir.com/)**