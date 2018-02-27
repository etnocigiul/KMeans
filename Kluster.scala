import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable
import scala.math
import scala.collection.breakOut
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

object Kluster extends Serializable{


/* Case class di appoggio utilizzata per la formattazione dell'input.
 * Composta da due parametri: ID, che è il codice identificativo del datapoint; vector è il vettore associato al datapoint da utilizzare come parametro di input per il KMeans
*/
case class CC(ID: String, vector: org.apache.spark.mllib.linalg.Vector)

	def main(args: Array[String]) {
		
		val conf = new SparkConf().setAppName("Sim").set("spark.cores.max","4").set("spark.hadoop.validateOutputSpecs", "false")
		val sc = new SparkContext(conf)



		if(args.size != 2){
			println("Numero di parametri errato, ritenta" + 
				"USAGE: [Dataset] [Num clusters]")
			sc.stop()
		}



		//SQL context per poter lavorare con i data frames
		val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

		val outputDir = "/home/luigi/workspace/data/kmeans/output/" + args(0) + "/" 

		val startTime = System.nanoTime()
		
		// Leggo e formatto i dati di input utilizzando la case class CC
		val data = sc.textFile("/home/luigi/workspace/data/kmeans/input/" + args(0) + "/part-00000.gz")

		val parsedData = data.map(l => l.split("\t")).map(v => (v(0).toString, v(1).toString))

		val convertedData = parsedData.map(x => CC(x._1, Vectors.dense(x._2.split(' ').map(_.toDouble)))).cache

		// Converto l'RDD[CC] in dataframe
		val allDF = convertedData.toDF()
		
		//Parametri di input per l'esecuzione del KMeans
		val vectors = convertedData.map(x => x.vector).cache
		val numClusters = args(1).toInt
		val numIterations = 20

		//Eseguo l'algoritmo KMeans ed ottengo come risultato un KMeansModel
		val kMeansModel = KMeans.train(vectors, numClusters, numIterations)

		//Ottengo i center points ricavati dall'esecuzione del KMeans 
		val centroIds = kMeansModel.clusterCenters
		
		//RDD contenente come chiave l'id del datapoint e come valore l'indice del cluster a cui appartiene
		val predictions = convertedData.map{ p => (p.ID, kMeansModel.predict(p.vector)) }
		//Converto l'RDD in dataframe specificando il nome delle colonne
		val predDF = predictions.toDF("ID", "CLUSTER")
		
		//Eseguo una join tra i due dataframe
		val results = allDF.join(predDF, "ID")
	
		//Formatto il nuovo dataframe 
		val resultsToArray = results.map(r =>  r(2) + "," + r(0)).rdd.collect
		sc.parallelize(resultsToArray).coalesce(1).saveAsTextFile(outputDir + numClusters + "_cluster")

		val dataCluster = sc.textFile(outputDir + numClusters + "_cluster/part-00000").map(l => l.split(",")).map(v => (v(0).toInt, v(1).toInt))

		//aggregateByKey per ottenere il set di datapoint appartenenti ai vari cluster
		val clusterMembers = dataCluster.aggregateByKey(mutable.Set[Int]())(
			      seqOp = (s, d) => s ++ mutable.Set(d.toInt),
			      combOp = (s1, s2) => s1 ++ s2
			    )


		//Formattazione dati per stampare i risultati ottenuti dall'esecuzione del KMeans su file
		val clusterMembersToArray = clusterMembers.map(v => ("Cluster " + v._1 + "\nMembers: " + v._2.mkString("[", ",", "]\n") + "N° members: " + v._2.size + "\n")).collect

		val centroIdsToArray = centroIds.zipWithIndex.map(c => "centroId " + c._2 + ": " + c._1 + "\n")

		val elapsedTime: String = "Tempo di esecuzione: " + ((System.nanoTime() - startTime) / 60000000000.0) + " minuti\n"

		val resultFile = Array(elapsedTime) ++ centroIdsToArray ++ clusterMembersToArray
		val vectorsFile = parsedData.map(x  => ("ID: " + x._1 + "\nVECTOR:  " + x._2.mkString("[", "","]\n" ).replaceAll(" ", ", ")))

		//Stampo i due file contenenti le informazioni necessarie all'analisi dei risultati
		sc.parallelize(resultFile).coalesce(1).saveAsTextFile(outputDir + numClusters + "_cluster")
		vectorsFile.coalesce(1).saveAsTextFile(outputDir + "VECTORS")



	}
}
