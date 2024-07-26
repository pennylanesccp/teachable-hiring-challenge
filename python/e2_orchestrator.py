from modules import *
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, sum as _sum, current_date, lit # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Definindo a variável de ambiente HADOOP_HOME no script Python

os.environ['HADOOP_HOME'] = r'C:\hadoop'

os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['PATH']


# Inicialização da sessão Spark

spark = SparkSession.builder \
    .appName("exercise_2") \
    .config("spark.local.dir", "spark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Main function to orchestrate the database setup

def main():
    
    # Set up logging
    
    setup_logging("logs/exercise_two.log")

    # Set up SQLite database connection
    
    conn, cursor, db_path = setup_sqlite()
    
    try:

        # Create tables and insert data
        
        create_tables(cursor, "e2")
        
        insert_data(cursor, "e2")

        # print_table_contents(cursor, ["purchase", "product_item", "purchase_extra_info"])

        # Leitura da tabela 'purchase' do banco de dados SQLite

        cursor.execute(f"SELECT * FROM purchase")

        purchase_data = cursor.fetchall()

        # Definir o esquema
        schema = StructType([
            StructField("transaction_datetime", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("purchase_id", IntegerType(), True),
            StructField("buyer_id", IntegerType(), True),
            StructField("prod_item_id", IntegerType(), True),
            StructField("order_date", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("producer_id", IntegerType(), True)
        ])

        df_purchase = spark.createDataFrame(purchase_data, schema)

        df_purchase.show()
        
    except Exception as e:
        
        logging.error(f"An error occurred: {e}")
        
    finally:
        
        # Close the connection
        
        conn.close()
        
        logging.info("Database connection closed.")

if __name__ == "__main__":
    
    main()
