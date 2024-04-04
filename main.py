
from pyspark.sql import SparkSession
from random import randint


# функция возвращает датафрейм с парами для связаных продуктов-категорий
def get_connections(df_prod, df_cat, df_conn):
    df_result = (
          df_prod.join(df_conn, df_prod['id']==df_conn['id_product'], how='left')
                 .join(df_cat, df_conn['id_category']==df_cat['id'], how='left')
                 .select(['category_name','product_name'])
    )
    df_result.orderBy("id_category", "id_product", )
    return df_result



#def main():
spark = SparkSession.builder.getOrCreate()

# создание случайных датасетов
data_category = [(i, f'category_{i}') for i in range(100)]
data_product = [(i, f'product_{i}') for i in range(1000)]
data_connections = [
        (
            randint(0, len(data_product)), 
            randint(0, len(data_category))
        ) 
        for i in range(len(data_product)//2)
    ]

df_categories = spark.createDataFrame(data_category, ['id', 'category_name'])
df_product = spark.createDataFrame(data_product, ['id', 'product_name'])
df_connections = spark.createDataFrame(data_connections, ['id_product', 'id_category'])

# функция возвращает датафрейм с парами для связаных продуктов-категорий
df_report = get_connections(df_product, df_categories, df_connections)
df_report.show()