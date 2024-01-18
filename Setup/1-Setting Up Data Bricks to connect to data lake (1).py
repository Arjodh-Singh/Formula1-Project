# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

def mountdatalake(storageaccount,container):
    application_id=dbutils.secrets.get(scope="formula1-adb-secret",key="formula1-adls-application-id")
    secretvalue=dbutils.secrets.get(scope="formula1-adb-secret",key="formula1-adls-secretvalue")
    tenant_id=dbutils.secrets.get(scope="formula1-adb-secret",key="formula1-adls-tenant-id")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secretvalue,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint==f"/mnt/{storageaccount}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storageaccount}/{container}")


# Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
    source = f"abfss://{container}@{storageaccount}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storageaccount}/{container}",
    extra_configs = configs)

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

mountdatalake("devarjsa","gold")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/demo"))

# COMMAND ----------

mountdatalake("devarjsa","bronze")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/circuits/

# COMMAND ----------


