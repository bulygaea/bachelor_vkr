from pyspark import SparkConf
from pyspark.sql import SparkSession
import os


# Конфиги PostgreSQL
class PostgreSQLConfigs:
    def __init__(self):
        self.host = os.environ['HOST']
        self.port = os.environ['PORT']
        self.database = os.environ['DBNAME']
        self.user = os.environ['USERNAME']
        self.password = os.environ['PWD']


# Конфиги Spark
class SparkConfigs:
    def __init__(self):
        self.conf = SparkConf()
        # conf.set('spark.dynamicAllocation.maxExecutors', '1')  # Максимум executor для динамического выделения
        # conf.set('spark.executor.memory', '1g')  # Память для каждого executor
        # conf.set('spark.driver.memory', '1g')  # Память для драйвера
        # conf.set('spark.executor.memoryOverhead', '1g')  # Дополнительная память для executor
        # conf.set('spark.driver.memoryOverhead', '1g')  # Дополнительная память для драйвера
        # conf.set('spark.executor.cores', '1')  # Кол-во используемых ядер
        self.conf.set("spark.jars", "postgresql-42.2.23.jar")
        self.conf.set('spark.app.name', 'SparkPostgresToHDFSRawLoader')


# Класс для переноса из PostgreSQL в HDFS(raw)
class PostgresToHDFSRawLoader(SparkConfigs, PostgreSQLConfigs):
    def __init__(self):
        SparkConfigs.__init__(self)
        PostgreSQLConfigs.__init__(self)

        self.spark = (SparkSession.builder.config(conf=self.conf).getOrCreate())

    def execute(self):
        # Данные и драйвер для БД
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }

        # Запрос для извлечения всех названий таблиц
        query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'airport') AS t"

        # Чтения данных в df_tables
        df_tables = self.spark.read.jdbc(url=url, table=query, properties=properties)

        # Запись названий таблиц в переменную
        table_list = [row.table_name for row in df_tables.collect()]

        # Цикл для прохождения по всем таблицам
        for table_name in table_list:
            # Чтение таблицы
            df = self.spark.read.jdbc(
                url=url,
                table=f'"airport"."{table_name}"',
                properties=properties
            )

            # Запись в HDFS
            df.write.mode('overwrite').parquet(f"hdfs:///user/data/raw/{table_name}")

        self.spark.stop()


PostgresToHDFSRawLoader().execute()
