from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.types import DecimalType

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# Завантажуємо датасет
users_df = spark.read.csv('/mnt/d/user/goit/git/goit-de-hw-03/users.csv', header=True)

# Виводимо на екран перші 5 записів
users_df.show(5)

# Створюємо тимчасове представлення для виконання SQL-запитів
users_df.createTempView("users_view")

users_df.printSchema()

spark.sql("""select count(*) from users_view""").show()

users01_df=users_df.dropna()

print(users01_df.count())


#--------------------------------  purchases.csv
purchases_df = spark.read.csv('/mnt/d/user/goit/git/goit-de-hw-03/purchases.csv', header=True)

# Виводимо на екран перші 5 записів
purchases_df.show(5)

# Створюємо тимчасове представлення для виконання SQL-запитів
purchases_df.createTempView("purchases_view")

purchases_df.printSchema()

spark.sql("""select count(*) from purchases_view""").show()

purchases01_df=purchases_df.dropna()

print(purchases01_df.count())

#--------------------------------  products.csv
products_df = spark.read.csv('/mnt/d/user/goit/git/goit-de-hw-03/products.csv', header=True)

# Виводимо на екран перші 5 записів
products_df.show(5)

# Створюємо тимчасове представлення для виконання SQL-запитів
products_df.createTempView("products_view")

products_df.printSchema()

spark.sql("""select count(*) from products_view""").show()

products01_df=products_df.dropna()

print(products01_df.count())


purchases1_df =purchases_df.join(products01_df, purchases01_df.product_id == products01_df.product_id, 'inner') \
      .drop(products_df.product_id)

#purchases1_df.show()

purchases15_df=purchases1_df\
    .withColumn("cost",col("quantity").cast("float")*col("price").cast("float"))\
        .groupBy("category").agg(sum("cost").alias('total_cost'))\
      .orderBy(desc('total_cost')) \
      .select('category', 'total_cost') 


print("task3")       
purchases15_df.show()
# task4
purchases2_df =purchases1_df.join(users01_df, purchases1_df.user_id == users01_df.user_id, 'inner') \
      .drop(products_df.product_id)

#purchases2_df.show()

purchases3_df=purchases2_df\
    .where(col("age").between(18, 25))\
        .withColumn("cost",col("quantity").cast("float")*col("price").cast("float"))\
            .groupBy("category").agg(sum("cost").alias('total_cost_1825'))\
      .orderBy(desc('total_cost_1825'))    

print("task4")        
purchases3_df.show()

#task5
df15 = purchases15_df.alias("a15")
df3  = purchases3_df.alias("a3")

purchases4_df=df15.join(df3,col("a15.category")==col("a3.category"),"inner" )\
.drop(col("a3.category"))\
    .withColumn("ps",(col("a3.total_cost_1825")/col("a15.total_cost")*100).cast(DecimalType(5, 2)))

print("task5")  
purchases4_df.show()

#task6
print("task6")       
purchases4_df.orderBy(desc('ps'))\
    .limit(3).show()