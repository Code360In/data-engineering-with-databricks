# Databricks notebook source
# MAGIC %scala
# MAGIC def cloudAndRegion = {
# MAGIC   import com.databricks.backend.common.util.Project
# MAGIC   import com.databricks.conf.trusted.ProjectConf
# MAGIC   import com.databricks.backend.daemon.driver.DriverConf
# MAGIC   val conf = new DriverConf(ProjectConf.loadLocalConfig(Project.Driver))
# MAGIC   (conf.cloudProvider.getOrElse("Unknown"), conf.region)
# MAGIC }
# MAGIC 
# MAGIC // These keys are read-only so they're okay to have here
# MAGIC val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
# MAGIC val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
# MAGIC val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
# MAGIC 
# MAGIC def getAwsMapping(region:String):(String,Map[String,String]) = {
# MAGIC 
# MAGIC   val MAPPINGS = Map(
# MAGIC     "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
# MAGIC     "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
# MAGIC     "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
# MAGIC     "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
# MAGIC     "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
# MAGIC     "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
# MAGIC     "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
# MAGIC     "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
# MAGIC     "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),
# MAGIC     "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
# MAGIC     
# MAGIC     "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
# MAGIC     "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
# MAGIC     "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
# MAGIC     "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
# MAGIC     "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
# MAGIC   )
# MAGIC 
# MAGIC   MAPPINGS.getOrElse(region, MAPPINGS("_default"))
# MAGIC }
# MAGIC 
# MAGIC def getAzureMapping(region:String):(String,Map[String,String]) = {
# MAGIC 
# MAGIC   var MAPPINGS = Map(
# MAGIC     "australiacentral"    -> ("dbtrainaustraliasoutheas",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "australiacentral2"   -> ("dbtrainaustraliasoutheas",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "australiaeast"       -> ("dbtrainaustraliaeast",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=FM6dy59nmw3f4cfN%2BvB1cJXVIVz5069zHmrda5gZGtU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "australiasoutheast"  -> ("dbtrainaustraliasoutheas",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "canadacentral"       -> ("dbtraincanadacentral",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=dwAT0CusWjvkzcKIukVnmFPTmi4JKlHuGh9GEx3OmXI%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "canadaeast"          -> ("dbtraincanadaeast",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SYmfKBkbjX7uNDnbSNZzxeoj%2B47PPa8rnxIuPjxbmgk%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "centralindia"        -> ("dbtraincentralindia",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=afrYm3P5%2BB4gMg%2BKeNZf9uvUQ8Apc3T%2Bi91fo/WOZ7E%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "centralus"           -> ("dbtraincentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=As9fvIlVMohuIV8BjlBVAKPv3C/xzMRYR1JAOB%2Bbq%2BQ%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "eastasia"            -> ("dbtraineastasia",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=sK7g5pki8bE88gEEsrh02VGnm9UDlm55zTfjZ5YXVMc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "eastus"              -> ("dbtraineastus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "eastus2"             -> ("dbtraineastus2",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=Y6nGRjkVj6DnX5xWfevI6%2BUtt9dH/tKPNYxk3CNCb5A%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "japaneast"           -> ("dbtrainjapaneast",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=q6r9MS/PC9KLZ3SMFVYO94%2BfM5lDbAyVsIsbBKEnW6Y%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "japanwest"           -> ("dbtrainjapanwest",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=M7ic7/jOsg/oiaXfo8301Q3pt9OyTMYLO8wZ4q8bko8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "northcentralus"      -> ("dbtrainnorthcentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "northcentralus"      -> ("dbtrainnorthcentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "northeurope"         -> ("dbtrainnortheurope",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=35yfsQBGeddr%2BcruYlQfSasXdGqJT3KrjiirN/a3dM8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "southcentralus"      -> ("dbtrainsouthcentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "southcentralus"      -> ("dbtrainsouthcentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "southindia"          -> ("dbtrainsouthindia",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=0X0Ha9nFBq8qkXEO0%2BXd%2B2IwPpCGZrS97U4NrYctEC4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "southeastasia"       -> ("dbtrainsoutheastasia",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=H7Dxi1yqU776htlJHbXd9pdnI35NrFFsPVA50yRC9U0%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "uksouth"             -> ("dbtrainuksouth",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SPAI6IZXmm%2By/WMSiiFVxp1nJWzKjbBxNc5JHUz1d1g%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "ukwest"              -> ("dbtrainukwest",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=olF4rjQ7V41NqWRoK36jZUqzDBz3EsyC6Zgw0QWo0A8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "westcentralus"       -> ("dbtrainwestcentralus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=UP0uTNZKMCG17IJgJURmL9Fttj2ujegj%2BrFN%2B0OszUE%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "westeurope"          -> ("dbtrainwesteurope",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=csG7jGsNFTwCArDlsaEcU4ZUJFNLgr//VZl%2BhdSgEuU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "westindia"           -> ("dbtrainwestindia",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=fI6PNZ7YvDGKjArs1Et2rAM2zgg6r/bsKEjnzQxgGfA%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "westus"              -> ("dbtrainwestus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=%2B1XZDXbZqnL8tOVsmRtWTH/vbDAKzih5ThvFSZMa3Tc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "westus2"             -> ("dbtrainwestus2",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
# MAGIC     "_default"            -> ("dbtrainwestus2",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z")
# MAGIC   )
# MAGIC 
# MAGIC   val (account: String, sasKey: String) = MAPPINGS.getOrElse(region, MAPPINGS("_default"))
# MAGIC 
# MAGIC   val blob = "training"
# MAGIC   val source = s"wasbs://$blob@$account.blob.core.windows.net/"
# MAGIC   val configMap = Map(
# MAGIC     s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
# MAGIC   )
# MAGIC 
# MAGIC   (source, configMap)
# MAGIC }
# MAGIC 
# MAGIC def retryMount(source: String, mountPoint: String): Unit = {
# MAGIC   try { 
# MAGIC     // Mount with IAM roles instead of keys for PVC
# MAGIC     dbutils.fs.mount(source, mountPoint)
# MAGIC     dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
# MAGIC   } catch {
# MAGIC     case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
# MAGIC   try {
# MAGIC     dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
# MAGIC     dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
# MAGIC   } catch {
# MAGIC     case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
# MAGIC     case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def autoMount(fix:Boolean = false, failFast:Boolean = false, mountPoint:String = "/mnt/training"): Unit = {
# MAGIC   val (cloud, region) = cloudAndRegion
# MAGIC   spark.conf.set("com.databricks.training.cloud.name", cloud)
# MAGIC   spark.conf.set("com.databricks.training.region.name", region)
# MAGIC   if (cloud=="AWS")  {
# MAGIC     val (source, extraConfigs) = getAwsMapping(region)
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     displayHTML(s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
# MAGIC   } else if (cloud=="Azure") {
# MAGIC     val (source, extraConfigs) = initAzureDataSource(region)
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     displayHTML(s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
# MAGIC   } else {
# MAGIC     val (source, extraConfigs) = ("s3a://databricks-corp-training/common", Map[String,String]())
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     displayHTML(s"Mounted course-specific datasets to <b>$mountPoint</b>.")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
# MAGIC   val mapping = getAzureMapping(azureRegion)
# MAGIC   val (source, config) = mapping
# MAGIC   val (sasEntity, sasToken) = config.head
# MAGIC 
# MAGIC   val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
# MAGIC   spark.conf.set("com.databricks.training.azure.datasource", datasource)
# MAGIC 
# MAGIC   return mapping
# MAGIC }
# MAGIC 
# MAGIC def mountSource(fix:Boolean, failFast:Boolean, mountPoint:String, source:String, extraConfigs:Map[String,String]): String = {
# MAGIC   val mntSource = source.replace(awsAuth+"@", "")
# MAGIC 
# MAGIC   if (dbutils.fs.mounts().map(_.mountPoint).contains(mountPoint)) {
# MAGIC     val mount = dbutils.fs.mounts().filter(_.mountPoint == mountPoint).head
# MAGIC     if (mount.source == mntSource) {
# MAGIC       return s"""Datasets are already mounted to <b>$mountPoint</b>."""
# MAGIC       
# MAGIC     } else if (failFast) {
# MAGIC       throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
# MAGIC       
# MAGIC     } else if (fix) {
# MAGIC       println(s"Unmounting existing datasets ($mountPoint from ${mount.source}).")
# MAGIC       dbutils.fs.unmount(mountPoint)
# MAGIC       mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     } else {
# MAGIC       return s"""<b style="color:red">Invalid Mounts!</b></br>
# MAGIC                       <ul>
# MAGIC                       <li>The training datasets you are using are from an unexpected source</li>
# MAGIC                       <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
# MAGIC                       <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
# MAGIC                       <ol>
# MAGIC                         <li>Insert a new cell after this one</li>
# MAGIC                         <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
# MAGIC                         <li>Verify that the problem has been resolved.</li>
# MAGIC                       </ol>"""
# MAGIC     }
# MAGIC   } else {
# MAGIC     println(s"""Mounting datasets to $mountPoint.""")
# MAGIC     mount(source, extraConfigs, mountPoint)
# MAGIC     return s"""Mounted datasets to <b>$mountPoint</b> from <b>$mntSource<b>."""
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def fixMounts(): Unit = {
# MAGIC   autoMount(true)
# MAGIC }
# MAGIC 
# MAGIC autoMount(true)
