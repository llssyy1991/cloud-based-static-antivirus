import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
/*object AVGCount
{
	def main(args: Array[String])
	{
*/

		//time of read file
		val start = System.currentTimeMillis()

		//the path to virus's information(several kinds of number)
		//###if you want test program, please modify path here###
		val path_info = "VirusShare_00001.txt"

		//the path to virus's information(which kind of virus)
		//###if you want test program, please modify path here###
		val path_classify = "VirusShare_00001hash.txt"
		
		val file_info = sc.textFile(path_info)

		val file_classify = sc.textFile(path_classify)

		//get time of read file
		val time = System.currentTimeMillis() - start
		println("Read file time: " + time + "ms")

		//time of warm start
		val start = System.currentTimeMillis()
		
		//read the md5 file
		//###if you want test program, please modify path here###
		val source = Source.fromFile("finaldemo.txt")
		val lineIterator = source.getLines

		//find same file in virus data base
		for (l <- lineIterator)
		{
			val warmstart = System.currentTimeMillis()
			val virusinformation = file_info.filter(line => line.contains(l)).collect().foreach(println)
			val virusserial = file_classify.filter(line => line.contains(l)).collect().foreach(println)
			val warmend = System.currentTimeMillis() - warmstart
			println("Warm executable time: " + warmend + "ms")
			println()
		}

		val time = System.currentTimeMillis() - start
		println("Execution time: " + time + "ms")

/*
	}