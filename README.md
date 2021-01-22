# 目的

支持hive读取自定义的CSV格式文件，目前已支持：

- 自定义行分隔符，支持多字符
- 列分隔符separator char支持多字符
- 引用符quote char支持多字符

暂未支持：

- 不支持写入

## 背景

Hive默认的InputFormat中行分隔符不支持自定义，只支持使用\n做为换行符，对于字段内容中有\n但又不能去掉和修改的情况就无法直接通过hive读取。

Hive中的OpenCSVSerde支持自定义separatorChar和quoteChar，但仅支持单字符，对于一些特定使用场景来说，无法使用，所以对OpenCSVSerde进行了改造，提供多字符的支持。

本方案推荐的使用方法：

1. 创建临时表，使用自定义的行列分隔符来读取CSV文件
2. 创建目标表，使用ORC格式
3. insert into 目标表 select * from 临时表;

# 使用

## 部署

1. 永久使用，将jar放到以下位置中后重启相应服务

- hive的lib目录
- tez的lib目录（如果用了tez）
- tez在yarn的缓存目录（或者让它重建缓存也可以）

2. 用的时候add进来

## 使用

### 使用场景：自定义行分隔符、列分隔符(单字符)、引用符(单字符)

步骤一、InputFormat使用`cn.licb.hive.ext.InputFormat.InputFormat`，Serde使用Hive自带的OpenCSVSerde即可，例：

```sql
CREATE EXTERNAL TABLE `test` (
  `id` string,
  `name` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS 
InputFormat 'cn.licb.hive.ext.InputFormat.InputFormat'
OutputFormat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '...你的csv文件路径';
```

步骤二、创建完成后，执行查询前，须使用`set textinputformat.record.line.delimiter.bytes=0d 0a;`设置分隔符。由于set里无法接受转义符设置，如无法设置=\n，所以这里只接受十六进制的设置，多个十六进制之间用空格分隔。

步骤三、执行查询`select * from test;`

注：文件编码默认以UTF-8进行读取，如需更改文件的编码，请执行查询前使用`set textinputformat.record.encoding=UTF-8;`进行更改

关于OpenCSVSerde的用法详见Hive文档，这里不做过多介绍。

### 使用场景：自定义列分隔符(多字符)、引用符(多字符)

步骤一、Serde使用`cn.licb.hive.ext.Serde.csv.CSVSerde`即可，例：

```sql
CREATE TABLE `test` (
  `id` string,
  `name` string)
ROW FORMAT SERDE 'cn.licb.hive.ext.Serde.csv.CSVSerde'
WITH SERDEPROPERTIES 
(
    'separatorChar' = '|~',
    'quoteChar'     = '#~'
)
...;
```

步骤二、执行查询`select * from test;`

### 使用场景：自定义行分隔符、列分隔符(多字符)、引用符(多字符)

步骤一、InputFormat使用`cn.licb.hive.ext.InputFormat.InputFormat`，Serde使用`cn.licb.hive.ext.Serde.csv.CSVSerde`即可，例：

```sql
CREATE TABLE `test` (
  `id` string,
  `name` string)
ROW FORMAT SERDE 'cn.licb.hive.ext.Serde.csv.CSVSerde'
WITH SERDEPROPERTIES 
(
    'separatorChar' = '|~',
    'quoteChar'     = '#~'
)
STORED AS 
InputFormat 'cn.licb.hive.ext.InputFormat.InputFormat'
OutputFormat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '...你的csv文件路径';
```

步骤二、创建完成后，执行查询前，须使用`set textinputformat.record.line.delimiter.bytes=0d 0a;`设置分隔符。由于set里无法接受转义符设置，如设置=\n在代码中收到的是"\\n"所以这里只接受十六进制的设置，多个十六进制之间用空格分隔。

步骤三、执行查询`select * from test;`

注：文件编码默认以UTF-8进行读取，如需更改文件的编码，请执行查询前使用`set textinputformat.record.encoding=UTF-8;`进行更改

# 开发

欢迎参与完善，或者有需求也可以在issues里提