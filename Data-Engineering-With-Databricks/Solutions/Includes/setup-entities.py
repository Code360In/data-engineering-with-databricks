# Databricks notebook source
import sys, subprocess, os
subprocess.check_call([sys.executable, "-m", "pip", "install", "git+https://github.com/databricks-academy/user-setup"])

from dbacademy import LessonConfig
LessonConfig.configure(course_name="Databases Tables and Views on Databricks", use_db=False)
LessonConfig.install_datasets(silent=True)

