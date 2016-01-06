// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.

//the path to the data file
//if you want to test code, please change path here
val path = "company.json"

//read json file by SparkSQL
val demodata = sqlContext.read.json(path)


//demodata.show()

// The inferred schema can be visualized using the printSchema() method.
//demodata.printSchema()
// root
//  |-- age: integer (nullable = true)
//  |-- name: string (nullable = true)

// Register this DataFrame as a table.

demodata.registerTempTable("demodata")
/*
val start = System.currentTimeMillis()
demodata.show()
val time = System.currentTimeMillis() - start
println("execution time: " + time + "ms")

val start = System.currentTimeMillis()
demodata.groupBy("_id").count().show()
val time = System.currentTimeMillis() - start
println("execution time: " + time + "ms")

val start = System.currentTimeMillis()
demodata.groupBy("SHA-256").count().show()
val time = System.currentTimeMillis() - start
println("execution time: " + time + "ms")

val start = System.currentTimeMillis()
//demodata.groupBy("result.AVware").count().show(1000)
//println("print %s". result.AVware)
//val result = demodata.groupBy("result.AVware").count()
val time = System.currentTimeMillis() - start
println("Execution time: " + time + "ms")
*/
 
val start = System.currentTimeMillis()
//output AntiVir number 
val count_adaware = sqlContext.sql("SELECT count(*) AS AntiVir_NUM FROM demodata WHERE result.AntiVir!='null'")
count_adaware.show()

//output AVG number
val count = sqlContext.sql("SELECT count(*) AS AVG_NUM FROM demodata WHERE result.AVG!='null'")
count.show()

//output Agnitum number
val count_Agnitum = sqlContext.sql("SELECT count(*) AS Agnitum_NUM FROM demodata WHERE result.Agnitum!='null'")
count_Agnitum.show()

//output Avast number
val count_Avast = sqlContext.sql("SELECT count(*) AS Avast_NUM FROM demodata WHERE result.Avast!='null'")
count_Avast.show()

//output BitDefender number
val count_BitDefender = sqlContext.sql("SELECT count(*) AS BitDefender_NUM FROM demodata WHERE result.BitDefender!='null'")
count_BitDefender.show()

//output Bkav number
val count_Bkav = sqlContext.sql("SELECT count(*) AS Bkav_NUM FROM demodata WHERE result.Bkav!='null'")
count_Bkav.show()

//output ByteHero number
val count_ByteHero = sqlContext.sql("SELECT count(*) AS ByteHero_NUM FROM demodata WHERE result.ByteHero!='null'")
count_ByteHero.show()

//output ClamAV number
val count_ClamAV = sqlContext.sql("SELECT count(*) AS ClamAV_NUM FROM demodata WHERE result.ClamAV!='null'")
count_ClamAV.show()

//output Commtouch number
val count_Commtouch = sqlContext.sql("SELECT count(*) AS Commtouch_NUM FROM demodata WHERE result.Commtouch!='null'")
count_Commtouch.show()

//output Comodo number
val count_Comodo = sqlContext.sql("SELECT count(*) AS Comodo_NUM FROM demodata WHERE result.Comodo!='null'")
count_Comodo.show()

//output DrWeb number
val count_DrWeb = sqlContext.sql("SELECT count(*) AS DrWeb_NUM FROM demodata WHERE result.DrWeb!='null'")
count_DrWeb.show()

//output Emsisoft number
val count_Emsisoft = sqlContext.sql("SELECT count(*) AS Emsisoft_NUM FROM demodata WHERE result.Emsisoft!='null'")
count_Emsisoft.show()

//output Fortinet number
val count_Fortinet = sqlContext.sql("SELECT count(*) AS Fortinet_NUM FROM demodata WHERE result.Fortinet!='null'")
count_Fortinet.show()

//output GData number
val count_GData = sqlContext.sql("SELECT count(*) AS GData_NUM FROM demodata WHERE result.GData!='null'")
count_GData.show()

//output Ikarus number
val count_Ikarus = sqlContext.sql("SELECT count(*) AS Ikarus_NUM FROM demodata WHERE result.Ikarus!='null'")
count_Ikarus.show()

//output Jiangmin number
val count_Jiangmin = sqlContext.sql("SELECT count(*) AS Jiangmin_NUM FROM demodata WHERE result.Jiangmin!='null'")
count_Jiangmin.show()

//output K7AntiVirus number
val count_K7AntiVirus = sqlContext.sql("SELECT count(*) AS K7AntiVirus_NUM FROM demodata WHERE result.K7AntiVirus!='null'")
count_K7AntiVirus.show()

//output K7GW number
val count_K7GW = sqlContext.sql("SELECT count(*) AS K7GW_NUM FROM demodata WHERE result.K7GW!='null'")
count_K7GW.show()

//output Kaspersky number
val count_Kaspersky = sqlContext.sql("SELECT count(*) AS Kaspersky_NUM FROM demodata WHERE result.Kaspersky!='null'")
count_Kaspersky.show()

//output Kingsoft number
val count_Kingsoft = sqlContext.sql("SELECT count(*) AS Kingsoft_NUM FROM demodata WHERE result.Kingsoft!='null'")
count_Kingsoft.show()

//output Malwarebytes number
val count_Malwarebytes = sqlContext.sql("SELECT count(*) AS Malwarebytes_NUM FROM demodata WHERE result.Malwarebytes!='null'")
count_Malwarebytes.show()

//output McAfee number
val count_McAfee = sqlContext.sql("SELECT count(*) AS McAfee_NUM FROM demodata WHERE result.McAfee!='null'")
count_McAfee.show()

//output Microsoft number
val count_Microsoft = sqlContext.sql("SELECT count(*) AS Microsoft_NUM FROM demodata WHERE result.Microsoft!='null'")
count_Microsoft.show()

//output Norman number
val count_Norman = sqlContext.sql("SELECT count(*) AS Norman_NUM FROM demodata WHERE result.Norman!='null'")
count_Norman.show()

//output Panda number
val count_Panda = sqlContext.sql("SELECT count(*) AS Panda_NUM FROM demodata WHERE result.Panda!='null'")
count_Panda.show()

//output Rising number
val count_Rising = sqlContext.sql("SELECT count(*) AS Rising_NUM FROM demodata WHERE result.Rising!='null'")
count_Rising.show()

//output SUPERAntiSpyware number
val count_SUPERAntiSpyware = sqlContext.sql("SELECT count(*) AS SUPERAntiSpyware_NUM FROM demodata WHERE result.SUPERAntiSpyware!='null'")
count_SUPERAntiSpyware.show()

//output Sophos number
val count_Sophos = sqlContext.sql("SELECT count(*) AS Sophos_NUM FROM demodata WHERE result.Sophos!='null'")
count_Sophos.show()

//output Symantec number
val count_Symantec = sqlContext.sql("SELECT count(*) AS Symantec_NUM FROM demodata WHERE result.Symantec!='null'")
count_Symantec.show()

//output TheHacker number
val count_TheHacker = sqlContext.sql("SELECT count(*) AS TheHacker_NUM FROM demodata WHERE result.TheHacker!='null'")
count_TheHacker.show()

//output TotalDefense number
val count_TotalDefense = sqlContext.sql("SELECT count(*) AS TotalDefense_NUM FROM demodata WHERE result.TotalDefense!='null'")
count_TotalDefense.show()

//output TrendMicro number
val count_TrendMicro = sqlContext.sql("SELECT count(*) AS TrendMicro_NUM FROM demodata WHERE result.TrendMicro!='null'")
count_TrendMicro.show()

//output VBA32 number
val count_VBA32 = sqlContext.sql("SELECT count(*) AS VBA32_NUM FROM demodata WHERE result.VBA32!='null'")
count_VBA32.show()

//output VIPRE number
val count_VIPRE = sqlContext.sql("SELECT count(*) AS VIPRE_NUM FROM demodata WHERE result.VIPRE!='null'")
count_VIPRE.show()

//output ViRobot number
val count_ViRobot = sqlContext.sql("SELECT count(*) AS ViRobot_NUM FROM demodata WHERE result.ViRobot!='null'")
count_ViRobot.show()

//output nProtect number
val count_nProtect = sqlContext.sql("SELECT count(*) AS nProtect_NUM FROM demodata WHERE result.nProtect!='null'")
count_nProtect.show()

val time = System.currentTimeMillis() - start
println("Execution time: " + time + "ms")