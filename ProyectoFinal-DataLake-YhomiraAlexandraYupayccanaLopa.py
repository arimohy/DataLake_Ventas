# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto Final: DataLake en Databricks
# MAGIC ### Alumna: Yhomira Alexandra Yupayccana Lopa
# MAGIC
# MAGIC Este proyecto tiene como objetivo implementar un DataLake utilizando la arquitectura **Medallón**. 
# MAGIC La arquitectura medallón organiza los datos en tres capas:
# MAGIC - **Bronze**: Guardar los archivos en el mismo formato de Origen.
# MAGIC - **Silver**: Crear tablas deltas a partir de los archivos guardados en la capa Bronze.
# MAGIC - **Golden**:Implementar los reportes finales en tablas Delta..
# MAGIC
# MAGIC ### Tecnologías Utilizadas
# MAGIC - **Databricks**: Para el procesamiento y almacenamiento de datos.
# MAGIC - **PySpark**: Para transformar y analizar los datos.
# MAGIC
# MAGIC ### Estructura del Notebook
# MAGIC 1. Configuración inicial del entorno.
# MAGIC 2. Transformaciones Capa Bronce.
# MAGIC 3. Transformaciones Capa Silver.
# MAGIC 4. Transformaciones Capa Golden.
# MAGIC
# MAGIC
# MAGIC A continuación, se detallan los pasos realizados.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración inicial del entorno.

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/DATALAKE-PROYECTOFINAL")

# COMMAND ----------

# Lista de nombres de carpetas SEGUN LA ARQUITECTURA MEDALLON
carpetas = ["dbfs:/DATALAKE-PROYECTOFINAL/bronze", "dbfs:/DATALAKE-PROYECTOFINAL/silver", "dbfs:/DATALAKE-PROYECTOFINAL/golden"]
# Crear las carpetas usando un bucle
for carpeta in carpetas:
    dbutils.fs.mkdirs(carpeta)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformaciones Capa Bronce
# MAGIC
# MAGIC En esta sección, se cargan los datos iniciales y se almacenan en la capa **Bronze** del DataLake.
# MAGIC Los datos se encuentran en formato CSV .
# MAGIC

# COMMAND ----------

#Copiar los archivos de /temp a bronze:
dbutils.fs.cp("dbfs:/FileStore/temp/clientes.csv", "dbfs:/DATALAKE-PROYECTOFINAL/bronze")
dbutils.fs.cp("dbfs:/FileStore/temp/productos.csv", "dbfs:/DATALAKE-PROYECTOFINAL/bronze")
dbutils.fs.cp("dbfs:/FileStore/temp/ventas.csv", "dbfs:/DATALAKE-PROYECTOFINAL/bronze")

# COMMAND ----------

#Lectura de los csv en dataframes
clientes_df = spark.read.csv("dbfs:/DATALAKE-PROYECTOFINAL/bronze/clientes.csv", header=True)
productos_df = spark.read.csv("dbfs:/DATALAKE-PROYECTOFINAL/bronze/productos.csv", header=True)
ventas_df = spark.read.csv("dbfs:/DATALAKE-PROYECTOFINAL/bronze/ventas.csv", header=True)

# COMMAND ----------

clientes_df.show()

# COMMAND ----------

productos_df.show()

# COMMAND ----------

ventas_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformaciones Capa Silver
# MAGIC En esta sección, se crea el esquema para guardar las tablas delta tomando los datos de la capa bronce y observamos las respectivas tablas para modificar la estructura si es necesario.

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists proyectofinal;

# COMMAND ----------

clientes_df.printSchema()

# COMMAND ----------

clientes_df = clientes_df.withColumnRenamed("teléfono", "telefono")

# COMMAND ----------

dbutils.fs.rm("dbfs:/DATALAKE-PROYECTOFINAL/silver/data/CLIENTES_DELTA", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creamos las tablas input
# MAGIC --Clientes
# MAGIC DROP TABLE IF EXISTS proyectofinal.clientes_delta;
# MAGIC CREATE OR REPLACE TABLE proyectofinal.clientes_delta
# MAGIC (
# MAGIC     id_cliente STRING,
# MAGIC     nombre STRING,
# MAGIC     correo STRING,
# MAGIC     ciudad STRING,
# MAGIC     telefono STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/silver/data/CLIENTES_DELTA';

# COMMAND ----------

productos_df.printSchema()

# COMMAND ----------

productos_df = productos_df.withColumnRenamed("categoría", "categoria")

# COMMAND ----------

dbutils.fs.rm("dbfs:/DATALAKE-PROYECTOFINAL/silver/data/PRODUCTOS_DELTA", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creamos las tablas input
# MAGIC --Productos
# MAGIC DROP TABLE IF EXISTS proyectofinal.productos_delta;
# MAGIC CREATE OR REPLACE TABLE proyectofinal.productos_delta
# MAGIC (
# MAGIC     id_producto STRING,
# MAGIC     nombre_producto STRING,
# MAGIC     categoria STRING,
# MAGIC     precio STRING,
# MAGIC     cantidad_stock STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/silver/data/PRODUCTOS_DELTA';

# COMMAND ----------

ventas_df.printSchema()

# COMMAND ----------

dbutils.fs.rm("dbfs:/DATALAKE-PROYECTOFINAL/silver/data/VENTAS_DELTA", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creamos las tablas input
# MAGIC --Ventas
# MAGIC DROP TABLE IF EXISTS proyectofinal.ventas_delta;
# MAGIC CREATE OR REPLACE TABLE proyectofinal.ventas_delta
# MAGIC (
# MAGIC     id_venta STRING,
# MAGIC     id_cliente STRING,
# MAGIC     id_producto STRING,
# MAGIC     fecha_venta STRING,
# MAGIC     cantidad_vendida STRING,
# MAGIC     total_venta STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/silver/data/VENTAS_DELTA';

# COMMAND ----------

# MAGIC %sql
# MAGIC --verificamos si se creo correctamente
# MAGIC SHOW TABLES IN proyectofinal;
# MAGIC

# COMMAND ----------

#Escritura en las tablas delta
clientes_df.write.mode("overwrite").saveAsTable("proyectofinal.clientes_delta")
productos_df.write.mode("overwrite").saveAsTable("proyectofinal.productos_delta")
ventas_df.write.mode("overwrite").saveAsTable("proyectofinal.ventas_delta")

# COMMAND ----------

df_clientes = spark.table("proyectofinal.clientes_delta")
df_productos = spark.table("proyectofinal.productos_delta")
df_ventas = spark.table("proyectofinal.ventas_delta")

df_clientes.show(5)
df_productos.show(5)
df_ventas.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformaciones Capa Golden
# MAGIC
# MAGIC Creamos Tablas para llenar con la logica final pedida

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ventas Totales por Producto
# MAGIC 2. Productos y sus Precios con Ventas Realizadas
# MAGIC 3. Clientes con la Mayor Cantidad de Compras
# MAGIC 4. Productos con Baja Venta

# COMMAND ----------

# MAGIC %sql
# MAGIC --Tabla 1 :Ventas Totales por Producto
# MAGIC CREATE or REPLACE TABLE proyectofinal.ventas_totales_por_producto
# MAGIC (
# MAGIC   nombre_producto STRING,
# MAGIC   categoria STRING,
# MAGIC   total_cantidad_vendida BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/golden/data/VENTAS_TOTALES_POR_PRODUCTO'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Tabla 2: Productos y sus Precios con Ventas Realizadas
# MAGIC CREATE or REPLACE TABLE proyectofinal.productos_precios_ventas
# MAGIC (
# MAGIC   nombre_producto STRING,
# MAGIC   precio_producto decimal(20,2),
# MAGIC   cantidad_vendida BIGINT,
# MAGIC   ingresos_totales decimal(20,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/golden/data/VENTAS_PRECIOS_VENTAS'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla 3:  Clientes con la Mayor Cantidad de Compras
# MAGIC CREATE or REPLACE TABLE proyectofinal.clientes_mayor_compras
# MAGIC (
# MAGIC   nombre STRING,
# MAGIC   cantidad_compras BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/golden/data/CLIENTES_MAYOR_COMPRAS'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla 4:  Productos con Baja Venta
# MAGIC CREATE or REPLACE TABLE proyectofinal.productos_baja_venta
# MAGIC (
# MAGIC   nombre_producto STRING,
# MAGIC   categoria STRING,
# MAGIC   cantidad_vendida BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/DATALAKE-PROYECTOFINAL/golden/data/PRODUCTOS_BAJA_VENTA'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ventas Totales por Producto
# MAGIC
# MAGIC ### Enunciado:
# MAGIC Se requiere generar un reporte que detalle el total de ventas realizadas para cada producto. El reporte debe incluir los siguientes campos:
# MAGIC
# MAGIC - **Nombre del Producto**  
# MAGIC - **Categoría**  
# MAGIC - **Total de Cantidad Vendida**  
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import  col ,sum ,count

# COMMAND ----------

ventas_totales_por_producto = df_ventas.join(df_productos,df_ventas["id_producto"]==df_productos["id_producto"])\
                                .groupBy('nombre_producto','categoria')\
                                .agg(sum(col("cantidad_vendida").cast("int")).alias("total_cantidad_vendida"))

# COMMAND ----------

display(ventas_totales_por_producto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Productos y sus Precios con Ventas Realizadas
# MAGIC
# MAGIC ### Enunciado:
# MAGIC Se requiere un reporte que detalle los productos junto con su precio, la cantidad vendida y los ingresos totales generados por cada producto. El reporte debe incluir los siguientes campos:
# MAGIC
# MAGIC - **Nombre del Producto**  
# MAGIC - **Precio del Producto**  
# MAGIC - **Cantidad Vendida**  
# MAGIC - **Total de Ingresos Generados** (cantidad vendida * precio)

# COMMAND ----------

productos_precios_ventas = df_productos.join(df_ventas, df_productos["id_producto"] == df_ventas["id_producto"])\
                            .groupBy("Nombre_producto", "precio")\
                            .agg(sum(col("cantidad_vendida").cast("int")).alias("cantidad_vendida"))\
                            .withColumn("total_ingresos", col("precio").cast("decimal(10,2)") * col("cantidad_vendida"))

# COMMAND ----------

display(productos_precios_ventas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clientes con la Mayor Cantidad de Compras
# MAGIC
# MAGIC ### Enunciado:
# MAGIC Generar un reporte que identifique a los clientes que han realizado el mayor número de compras. El reporte debe incluir los siguientes campos:
# MAGIC
# MAGIC - **Nombre del Cliente**  
# MAGIC - **Cantidad de Compras Realizadas** 

# COMMAND ----------

clientes_mayor_compras = df_clientes.join(df_ventas, df_clientes['id_cliente'] == df_ventas['id_cliente'])\
                            .groupBy('nombre')\
                            .agg(count('nombre').alias('cantidad_compras'))

# COMMAND ----------

display(clientes_mayor_compras)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Productos con Baja Venta
# MAGIC
# MAGIC ### Enunciado:
# MAGIC Se requiere un reporte que identifique los productos con baja venta, es decir, aquellos con una cantidad total de ventas menor a 15. El reporte debe incluir los siguientes campos:
# MAGIC
# MAGIC - **Nombre del Producto**  
# MAGIC - **Categoría**  
# MAGIC - **Cantidad Vendida**  

# COMMAND ----------

productos_baja_venta = ventas_totales_por_producto.withColumnRenamed("total_cantidad_vendida", "cantidad_vendida")\
    .filter(col("cantidad_vendida") < 15) 


# COMMAND ----------

display(productos_baja_venta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura de Tablas Resultantes
# MAGIC

# COMMAND ----------

ventas_totales_por_producto.write.mode("overwrite").saveAsTable("proyectofinal.ventas_totales_por_producto")
productos_precios_ventas.write.mode("overwrite").saveAsTable("proyectofinal.productos_precios_ventaso")
clientes_mayor_compras.write.mode("overwrite").saveAsTable("proyectofinal.clientes_mayor_compras")
productos_baja_venta.write.mode("overwrite").saveAsTable("proyectofinal.productos_baja_venta")

# COMMAND ----------

# MAGIC %md
# MAGIC Espero que el proyecto sea de su agrado y cualquier recomendacion sera agradecido. Gracias
# MAGIC
# MAGIC **Desarrollado por: Yhomira Alexandra Yupayccana Lopa**  
# MAGIC
