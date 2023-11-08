# Databricks notebook source
# Define the configuration for Azure Blob Storage
storage_account_name = "groupe2stockage"
container_name = "groupe2conteneur"

# Mount the Azure Blob Storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = "/mnt/mymount", # Provide the mount point path
    extra_configs = {"fs.azure.account.key.groupe2stockage.blob.core.windows.net": dbutils.secrets.get(scope="groupe2", key='secret2')}
)

# COMMAND ----------

file_path = "/mnt/mymount/train.csv"
train = spark.read.option("header", "true").csv(file_path)


# COMMAND ----------

train.show()

# COMMAND ----------

# Sélectionnez les trois premières colonnes
train.select(train.columns[:3]).show()


# COMMAND ----------

total_rows = train.count()

threshold = 0.7

selected_columns = []

# Parcourez chaque colonne et vérifiez le pourcentage de valeurs nulles
for col_name in train.columns:
    null_count = train.where(col(col_name).isNull()).count()
    null_percentage = null_count / total_rows
    if null_percentage < threshold:
        selected_columns.append(col_name)

# Limitez le DataFrame train aux colonnes sélectionnées
limited_train = train.select(selected_columns)

# Limitez le DataFrame aux 2000 premières lignes
new_df = limited_train.limit(2000)


# COMMAND ----------

new_df.select(new_df.columns[:3]).show()
