# Databricks notebook source
class Paths():
    def __init__(self, working_dir, clean_lesson):
        self.working_dir = working_dir

        if clean_lesson: self.user_db = f"{working_dir}/{clean_lesson}.db"
        else:            self.user_db = f"{working_dir}/database.db"

        self.checkpoints = f"{working_dir}/_checkpoints"    
            
    def exists(self, path):
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        max_key_len = 0
        for key in self.__dict__: max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}DA.paths.{key}:"
            print(label.ljust(max_key_len+13) + DA.paths.__dict__[key])

class DBAcademyHelper():
    def __init__(self, lesson=None):
        import re, time

        self.start = int(time.time())
        
        self.course_name = "dewd"
        self.lesson = lesson.lower()
        self.data_source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/data-engineering-with-databricks/v02"

        # Define username
        self.username = spark.sql("SELECT current_user()").first()[0]
        self.clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)

        self.db_name_prefix = f"dbacademy_{self.clean_username}_{self.course_name}"
        self.source_db_name = None

        self.working_dir_prefix = f"dbfs:/user/{self.username}/dbacademy/{self.course_name}"
        
        if self.lesson:
            clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson)
            working_dir = f"{self.working_dir_prefix}/{self.lesson}"
            self.paths = Paths(working_dir, clean_lesson)
            self.hidden = Paths(working_dir, clean_lesson)
            self.db_name = f"{self.db_name_prefix}_{clean_lesson}"
        else:
            working_dir = self.working_dir_prefix
            self.paths = Paths(working_dir, None)
            self.hidden = Paths(working_dir, None)
            self.db_name = self.db_name_prefix

    def init(self, create_db=True):
        spark.catalog.clearCache()
        self.create_db = create_db
        
        if create_db:
            print(f"\nCreating the database \"{self.db_name}\"")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
            spark.sql(f"USE {self.db_name}")

    def cleanup(self):
        for stream in spark.streams.active:
            print(f"Stopping the stream \"{stream.name}\"")
            stream.stop()
            try: stream.awaitTermination()
            except: pass # Bury any exceptions

        if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database \"{self.db_name}\"")
            spark.sql(f"DROP DATABASE {self.db_name} CASCADE")
            
        if self.paths.exists(self.paths.working_dir):
            print(f"Removing the working directory \"{self.paths.working_dir}\"")
            dbutils.fs.rm(self.paths.working_dir, True)

    def conclude_setup(self):
        import time

        spark.conf.set("da.username", self.username)
        spark.conf.set("da.db_name", self.db_name)
        for key in self.paths.__dict__:
            spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])

        print("\nPredefined Paths:")
        DA.paths.print()

        if self.source_db_name:
            print(f"\nPredefined tables in {self.source_db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.source_db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")

        if self.create_db:
            print(f"\nPredefined tables in {self.db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")
                
        print(f"\nSetup completed in {int(time.time())-self.start} seconds")
        
    def block_until_stream_is_ready(self, query, min_batches=2):
        import time
        while len(query.recentProgress) < min_batches:
            time.sleep(5) # Give it a couple of seconds

        print(f"The stream has processed {len(query.recentProgress)} batchs")
        
dbutils.widgets.text("lesson", "missing")
lesson = dbutils.widgets.get("lesson")
if lesson == "none": lesson = None
assert lesson != "missing", f"The lesson must be passed to the DBAcademyHelper"

DA = DBAcademyHelper(lesson)

