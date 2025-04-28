# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark.sql import SparkSession


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
        self.conf.set('spark.app.name', 'RawToDstNPLoader')  # Имя


# Класс для переноса из HDFS(raw) в HDFS(dst/nonpartitioned)
class RawToDstNPLoader(SparkConfigs):
    def __init__(self):
        SparkConfigs.__init__(self)
        self.spark = (SparkSession.builder.config(conf=self.conf).getOrCreate())

        # Создаем объект FileSystem (подключение)
        self.fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            self.spark.sparkContext._jsc.hadoopConfiguration()
        )

    def execute(self):
        # Получение списка всех таблиц (директорий) в исходной папке с помощью Spark
        status = self.fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path("hdfs:///user/data/raw"))

        # Перебор всех объектов в директории
        for file in status:
            file_path = file.getPath().toString()  # Абсолютный путь

            # Загрузка данных из исходной папки для каждой таблицы
            df = self.spark.read.parquet(file_path)

            # Запись данных в целевую папку с названием таблицы
            df.write.mode("overwrite").parquet(f"hdfs:///user/data/dst/nonpartitioned/{file.getLocalName()}")

        self.spark.stop()


RawToDstNPLoader().execute()
