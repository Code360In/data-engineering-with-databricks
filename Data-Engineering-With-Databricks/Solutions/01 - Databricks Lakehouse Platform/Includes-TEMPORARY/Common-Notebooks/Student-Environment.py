# Databricks notebook source
#############################################
# TAG API FUNCTIONS
#############################################

# Get all tags
def getTags() -> dict:
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if len(values) > 0:
      return values
  except:
    return defaultValue

#############################################
# USER, USERNAME, AND USERHOME FUNCTIONS
#############################################

# Get the user's username
def getUsername() -> str:
  import uuid
  try:
    return dbutils.widgets.get("databricksUsername")
  except:
    return getTag("user", str(uuid.uuid1()).replace("-", ""))

# Get the user's userhome
def getUserhome() -> str:
  username = getUsername()
  return "dbfs:/user/{}".format(username)

def getModuleName() -> str:
  return "aspwd"

def getLessonName() -> str:
  # If not specified, use the notebook's name.
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

def getWorkingDir() -> str:
  import re
  lessonName = re.sub("[^a-zA-Z0-9]", "_", getLessonName())
  moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName())
  userhome = getUserhome()
  return f"{userhome}/dbacademy/{moduleName}/{lessonName}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()

def getRootDir() -> str:
  import re
  moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName())
  userhome = getUserhome()
  return f"{userhome}/dbacademy/{moduleName}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()

############################################
# USER DATABASE FUNCTIONS
############################################

def getDatabaseName(username:str, moduleName:str, lessonName:str) -> str:
  import re
  user = re.sub("[^a-zA-Z0-9]", "_", username)
  module = re.sub("[^a-zA-Z0-9]", "_", moduleName)
  lesson = re.sub("[^a-zA-Z0-9]", "_", lessonName)
  databaseName = f"dbacademy_{user}_{module}_{lesson}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()
  return databaseName


# Create a user-specific database
def createUserDatabase(username:str, moduleName:str, lessonName:str) -> str:
  databaseName = getDatabaseName(username, moduleName, lessonName)

  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
  spark.sql("USE {}".format(databaseName))

  return databaseName

# ****************************************************************************
# Utility method to determine whether a path exists
# ****************************************************************************

def pathExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

# ****************************************************************************
# Utility method for recursive deletes
# Note: dbutils.fs.rm() does not appear to be truely recursive
# ****************************************************************************

def deletePath(path):
  files = dbutils.fs.ls(path)

  for file in files:
    deleted = dbutils.fs.rm(file.path, True)

    if deleted == False:
      if file.is_dir:
        deletePath(file.path)
      else:
        raise IOError("Unable to delete file: " + file.path)

  if dbutils.fs.rm(path, True) == False:
    raise IOError("Unable to delete directory: " + path)

# ****************************************************************************
# Utility method to clean up the workspace at the end of a lesson
# ****************************************************************************

def classroomCleanup(username:str, moduleName:str, lessonName:str, dropDatabase:bool):
  import time

  # Stop any active streams
  if len(spark.streams.active) > 0:
    print(f"Stopping {len(spark.streams.active)} streams")

  for stream in spark.streams.active:
    try: 
      stream.stop()
      stream.awaitTermination()
    except: 
      pass # Bury any exceptions arising from stopping

  # Drop all tables from the specified database
  database = getDatabaseName(username, moduleName, lessonName)
  try:
    tables = spark.sql("show tables from {}".format(database)).select("tableName").collect()
    for row in tables:
      tableName = row["tableName"]
      spark.sql("drop table if exists {}.{}".format(database, tableName))

      # In some rare cases the files don't actually get removed.
      time.sleep(1) # Give it just a second...
      hivePath = "dbfs:/user/hive/warehouse/{}.db/{}".format(database, tableName)
      dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure
  except:
    pass # ignored

  # Remove any files that may have been created from previous runs
  path = getWorkingDir()
  if pathExists(path):
    deletePath(path)

  # The database should only be dropped in a "cleanup" notebook, not "setup"
  if dropDatabase:
    spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))

    # In some rare cases the files don't actually get removed.
    time.sleep(1) # Give it just a second...
    hivePath = "dbfs:/user/hive/warehouse/{}.db".format(database)
    dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure

    displayHTML("Dropped database and removed files in working directory")


# Utility method to delete a database
def deleteTables(database):
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))

# ****************************************************************************
# Placeholder variables for coding challenge type specification
# ****************************************************************************
class FILL_IN:
  from pyspark.sql.types import Row, StructType
  VALUE = None
  LIST = []
  SCHEMA = StructType([])
  ROW = Row()
  INT = 0
  DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))

############################################
# Set up student environment
############################################

moduleName = getModuleName()
username = getUsername()
lessonName = getLessonName()
userhome = getUserhome()

workingDir = getWorkingDir()
dbutils.fs.mkdirs(workingDir)

workingDirRoot = getRootDir()
datasetsDir = f"{workingDirRoot}/datasets"
spark.conf.set("com.databricks.training.aspwd.datasetsDir", datasetsDir)
databaseName = createUserDatabase(username, moduleName, lessonName)

classroomCleanup(username, moduleName, lessonName, False)

# COMMAND ----------

# MAGIC %scala
# MAGIC val datasetsDir = spark.conf.get("com.databricks.training.aspwd.datasetsDir")
