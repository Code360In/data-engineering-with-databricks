# Databricks notebook source
# ****************************************************************************
# Utility method to count & print the number of records in each partition.
# ****************************************************************************

def printRecordsPerPartition(df):
  def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
  results = (df.rdd                   # Convert to an RDD
    .mapPartitions(countInPartition)  # For each partition, count
    .collect()                        # Return the counts to the driver
  )
  
  print("Per-Partition Counts")
  i = 0
  for result in results: 
    i = i + 1
    print("#{}: {:,}".format(i, result))
  
# ****************************************************************************
# Utility to count the number of files in and size of a directory
# ****************************************************************************

def computeFileStats(path):
  bytes = 0
  count = 0

  files = dbutils.fs.ls(path)
  
  while (len(files) > 0):
    fileInfo = files.pop(0)
    if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
      count += 1
      bytes += fileInfo.size                      # size is a parameter on the fileInfo object
    else:
      files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
      
  return (count, bytes)

# ****************************************************************************
# Utility method to cache a table with a specific name
# ****************************************************************************

def cacheAs(df, name, level = "MEMORY-ONLY"):
  from pyspark.sql.utils import AnalysisException
  if level != "MEMORY-ONLY":
    print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")  
    
  try: spark.catalog.uncacheTable(name)
  except AnalysisException: None
  
  df.createOrReplaceTempView(name)
  spark.catalog.cacheTable(name)
  
  return df


# ****************************************************************************
# Simplified benchmark of count()
# ****************************************************************************

def benchmarkCount(func):
  import time
  start = float(time.time() * 1000)                    # Start the clock
  df = func()
  total = df.count()                                   # Count the records
  duration = float(time.time() * 1000) - start         # Stop the clock
  return (df, total, duration)

# ****************************************************************************
# Utility methods to terminate streams
# ****************************************************************************

def getActiveStreams():
  try:
    return spark.streams.active
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("Unable to iterate over all active streams - using an empty set instead.")
    return []

def stopStream(s):
  try:
    print("Stopping the stream {}.".format(s.name))
    s.stop()
    print("The stream {} was stopped.".format(s.name))
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("An [ignorable] error has occured while stoping the stream.")

def stopAllStreams():
  streams = getActiveStreams()
  while len(streams) > 0:
    stopStream(streams[0])
    streams = getActiveStreams()
    
# ****************************************************************************
# Utility method to wait until the stream is read
# ****************************************************************************

def untilStreamIsReady(name, progressions=3):
  import time
  queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))

  while (len(queries) == 0 or len(queries[0].recentProgress) < progressions):
    time.sleep(5) # Give it a couple of seconds
    queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))

  print("The stream {} is active and ready.".format(name))

