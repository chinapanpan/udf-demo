# Hive UDF Demo on EMR Spark

本项目演示如何在 AWS EMR 7.12 (Spark 3.5.6) 上，通过 Java 编写自定义 Hive UDF，并使用 `CREATE FUNCTION` 将其注册为永久函数。

## 项目结构

```
udf-demo/
├── pom.xml                                          # Maven 配置
├── src/main/java/com/example/udf/ToUpperUDF.java    # UDF 源码
├── verify_udf.py                                    # Spark SQL 验证脚本
└── README.md                                        # 本文档
```

## 1. UDF 源码

`src/main/java/com/example/udf/ToUpperUDF.java`：

```java
package com.example.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ToUpperUDF extends UDF {
    public Text evaluate(Text input) {
        if (input == null) return null;
        return new Text(input.toString().toUpperCase());
    }
}
```

- 继承 `org.apache.hadoop.hive.ql.exec.UDF`
- 实现 `evaluate()` 方法，接收 `Text` 类型输入，返回大写结果

## 2. 编译打包

```bash
mvn clean package
```

生成 `target/udf-demo-1.0.0.jar`。

将 JAR 上传到 S3：

```bash
aws s3 cp target/udf-demo-1.0.0.jar s3://<your-bucket>/udf-demo/udf-demo-1.0.0.jar
```

## 3. 启动 EMR 集群

启动 EMR 7.12 集群，安装 Spark 和 Hive，使用 Glue Catalog 作为 Metastore：

```bash
aws emr create-cluster \
  --name "udf-demo" \
  --release-label emr-7.12.0 \
  --region us-west-2 \
  --applications Name=Spark Name=Hive \
  --ec2-attributes KeyName=<key>,SubnetId=<subnet>,InstanceProfile=EMR_EC2_DefaultRole \
  --instance-groups '[
    {"InstanceGroupType":"MASTER","InstanceCount":1,"InstanceType":"m5.xlarge"},
    {"InstanceGroupType":"CORE","InstanceCount":1,"InstanceType":"m5.xlarge"}
  ]' \
  --service-role EMR_DefaultRole \
  --configurations '[
    {
      "Classification": "spark-hive-site",
      "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
    {
      "Classification": "hive-site",
      "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ]' \
  --log-uri s3://<your-bucket>/emr-logs/
```

## 4. 创建永久 UDF

在 Spark SQL 中执行：

```sql
CREATE DATABASE IF NOT EXISTS udf_demo;

CREATE FUNCTION udf_demo.to_upper
  AS 'com.example.udf.ToUpperUDF'
  USING JAR 's3://<your-bucket>/udf-demo/udf-demo-1.0.0.jar';
```

## 5. 验证 UDF

### 简单查询

```sql
SELECT udf_demo.to_upper('hello world') AS result;
```

输出：

```
+-----------+
|result     |
+-----------+
|HELLO WORLD|
+-----------+
```

### 表数据查询

```sql
CREATE TABLE udf_demo.test_names (name STRING, city STRING) USING parquet;

INSERT INTO udf_demo.test_names VALUES
  ('alice', 'seattle'),
  ('bob', 'portland'),
  ('charlie', 'san francisco');

SELECT
    name,
    udf_demo.to_upper(name) AS upper_name,
    city,
    udf_demo.to_upper(city) AS upper_city
FROM udf_demo.test_names;
```

输出：

```
+-------+----------+-------------+-------------+
|name   |upper_name|city         |upper_city   |
+-------+----------+-------------+-------------+
|charlie|CHARLIE   |san francisco|SAN FRANCISCO|
|alice  |ALICE     |seattle      |SEATTLE      |
|bob    |BOB       |portland     |PORTLAND     |
+-------+----------+-------------+-------------+
```

### 与内置 upper() 对比

```sql
SELECT
    name,
    upper(name) AS builtin_upper,
    udf_demo.to_upper(name) AS custom_udf_upper
FROM udf_demo.test_names;
```

输出：

```
+-------+-------------+----------------+
|name   |builtin_upper|custom_udf_upper|
+-------+-------------+----------------+
|charlie|CHARLIE      |CHARLIE         |
|alice  |ALICE        |ALICE           |
|bob    |BOB          |BOB             |
+-------+-------------+----------------+
```

## 6. 通过 spark-submit 提交验证脚本

也可以通过 `verify_udf.py` 一键完成创建和验证：

```bash
spark-submit --deploy-mode client verify_udf.py
```

## 验证环境

| 组件 | 版本 |
|------|------|
| EMR | 7.12.0 |
| Spark | 3.5.6-amzn-1 |
| Hive Metastore | AWS Glue Catalog |
| Java | JDK 8+ |
| Region | us-west-2 |
