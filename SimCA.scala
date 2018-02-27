import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable
import scala.math
import scala.collection.breakOut
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object SimCA extends Serializable{

	def main(args: Array[String]) {
		
		//val inputPath = "hdfs://" + ip + ":" + port + "/user/conte/input/" + args(0)
		val inputPath = "/home/luigi/workspace/data/sim/" + args(0)
		//Inizializzo lo SparkContext
		val conf = new SparkConf().setAppName("Sim").set("spark.cores.max","4").set("spark.hadoop.validateOutputSpecs", "false")
		val sc = new SparkContext(conf)



		val startTime = System.nanoTime()
		//Dall'input ricavo prima i collegamenti tra i vari autori, e poi da questi ottengo una lista dove ad ogni autore associo la lista degli altri autori con i quali ha collaborato
		val input = sc.textFile(inputPath)
		var edges = input.filter(l => l.length > 0 && l(0) != '#').map({l => val a = l.split("\\s+"); (a(0).toInt, a(1).toInt)})

		//Con questo passaggio evito di tralasciare i collegamenti mancanti, per esempio se esiste il collegamento 1 --> 2 ma non il viceversa, allora quest ultimo viene aggiunto
		edges = edges.map(x => (x._2, x._1)).union(edges).distinct 

		//Ricavo un RDD in cui ogni elemento è composto dall'id dell'autore e la sua lista di adiacenza
		val authors = edges.aggregateByKey(mutable.Set[Int]())(
				seqOp = (s, d) => s ++ mutable.Set(d),
				combOp = (s1, s2) => s1 ++ s2
			).map(v => (v._1, v._2.toArray))

		val elapsedTimeInput = System.nanoTime() - startTime
		println("\n-----------------------------------------------------\nTempo lettura file: " + (elapsedTimeInput / 1000000000.0) + " secondi\n-----------------------------------------------------\n")
		//authors.checkpoint() //eseguo il checkpointing dell'RDD authors
		val nVertex = authors.count //authors Rdd evaluated by calling count() action
		//Eseguo il prodotto cartesiano sull'RDD authors così da ottenere ogni possibile coppia di autori e le loro corrispettive liste di adiacenza
println("\n\n--------------------------------------------\n" + nVertex + "\n--------------------------------------------\n")
		val cartesian_autohors: RDD[((Int, Array[Int]), (Int, Array[Int]))] = authors.cartesian(authors)

		//cartesian_autohors.checkpoint() //eseguo il checkpointing dell'RDD cartesian_authors
		val nSim = cartesian_autohors.count //authors Rdd evaluated by calling count() action
		
		//Definisco i valori da passare alla funzione obtainSimilaritiesMatrix
		val default_jaccard:Double = nVertex / (authors.map(x => x._2.length).sum).toDouble //oppure 1 / (authors.map(x => x._2.length).sum).toDouble???
		val default_preference: Double = -10.0

		val similarities_matrix = obtainSimilaritiesMatrix(cartesian_autohors, default_preference, default_jaccard)//.coalesce(16)
		
		
		val new_sm = similarities_matrix.map(x => (x._1, (x._2, x._3))).aggregateByKey(mutable.Set[(Int, Double)]())(
				seqOp = (s, d) => s ++ mutable.Set(d),
				combOp = (s1, s2) => s1 ++ s2
			).map(v => (v._1, v._2.toArray)).sortBy(x => (x._1), ascending = true).map(x => ( x._1,x._2.sortWith(_._1 < _._1).map(v => v._2).mkString(" ") ) )

		
		//similarities_matrix.checkpoint()
		val nRighe = similarities_matrix.count

		val outDirName = args(0).split(".txt").head
		
		val outPath = "/home/luigi/workspace/data/kmeans/input/" + outDirName
		//Salvo la matrice di similarità su file
		val outFile= new_sm.map(x => (x._1 + "\t" + x._2))
		
		outFile.coalesce(1).saveAsTextFile(outPath, classOf[GzipCodec])

		val elapsedTimeExecution = System.nanoTime() - startTime
		println("n: " + nRighe + "\n-----------------------------------------------------\nTempo di esecuzione: " + (elapsedTimeExecution / 60000000000.0) + " minuti, n° similarities: " + nSim + "\n-----------------------------------------------------")
		}


	/*
	* Funzione obtainSimilaritiesMatrix
	* param authors: RDD contenente tutte le possibili coppie di autori
	* param default_preference: valore di default della preference
	* param default_jaccard: valore di default per l'indice di jaccard
	* retval: RDD contenente la matrice delle similarities ordinata per autori crescenti
	*/
	def obtainSimilaritiesMatrix(authors: RDD[( (Int, Array[Int]), (Int, Array[Int]) )], default_preference: Double, default_jaccard: Double)  : RDD[(Int, Int, Double)]  = {
			
			//Viene utilizzato il logaritmo naturale del coefficiente di similarità di Jaccard per il calcolo delle similarities
			val initial_similarities_matrix : RDD[(Int, Int, Double)] = authors.map{x =>
										var a: Double = (x._1._2.intersect(x._2._2).length);
										var b: Double = (x._1._2.union(x._2._2).length);

										if(a != 0){
											var jaccard: Double = math.log(a/b);
											var sim: Double = jaccard * default_preference;
										
											(x._1._1, x._2._1, sim)
											}
										else (x._1._1, x._2._1, 100.0)
										}

			//Ricavo i valori delle similarità per calcolare poi le preferences
			val similarities_values = initial_similarities_matrix.filter(x => x._1 != x._2).map(x => x._3)
			val preference: Double = (similarities_values.max + similarities_values.min) / 2

			//Utilizzo la preference per aggiornare la initial_similarities_matrix, nella quale ci sono preferences errate
			val tmp1 = initial_similarities_matrix.filter(x => x._1 == x._2).map(x => (x._1, x._2, 0.0))
			val tmp2 = initial_similarities_matrix.filter(x => x._1 != x._2)
			
			//Il valore di ritorno
			val ris = tmp1.union(tmp2).sortBy(x => (x._1, x._2), ascending = true)

			ris
	}

}
