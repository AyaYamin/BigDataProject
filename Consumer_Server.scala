package org.main.scala
package docs.http.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.clearspring.analytics.stream.frequency.CountMinSketch
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, length, split}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.main.scala.GeoHash.{decode, encode}

import scala.collection.mutable.ListBuffer


object Consumer_Server{

  def main(args: Array[String]): Unit = {

    // define topic
    val topic = "streamtest"
    val topics = Array(topic)


    // needed for the future flatMap --> onComplete in the end
    implicit val system = ActorSystem(Behaviors.empty, "my-system")
    implicit val executionContext = system.executionContext


    // set app configuration
    val conf = new SparkConf().setMaster("local[2]").setAppName("Tweets Consuming")

    // define spark streaming of interval of 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))


    // define count min sketch
    val countMinSketch = new CountMinSketch(0.001, 0.99, 1)

    // define kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

     // get stream
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Get the lines
    val lines = stream.map(_.value)


    //**************************** Processing on lines**********************************************************************************


    // regular expressions
    val reg_1 = raw"[^A-Za-z\s]+" // no numbers
    val reg_2 = "[^\\w\\s]|('s|ly|ed|ing|ness)" //stemming

    val stream_lines = lines
      .map(oi => (
        oi.split(",")(1).replaceAll("timestamp", "").replaceAll(": ", "").replaceAll("\"", "")
        , (oi.split(",")(3).replaceAll("coordinates", "").replaceAll(":", "").replaceAll("\'", "").replaceAll("\\[", "").replaceAll("  ", "").toDouble, oi.split(",")(4).replaceAll("}", "").replaceAll("]", "").replaceAll("\"", "").toDouble)
        , oi.split(",")(5).toLowerCase.replaceAll(reg_1, "").replaceAll(reg_2, "").replaceAll("text", "").replaceAll("  ", "")
      ))


    //*********************************************************************************************************************************************************

    // create collection of sets
    var set = scala.collection.mutable.Set[String]()
    var setG = scala.collection.mutable.Set[String]()
    var setCH = scala.collection.mutable.Set[String]()

    // For each stream
    stream_lines.foreachRDD(
      rdd => {
        // map data into (word,geo_hash(coordinates),date)
        val stream_lines_1 = rdd.map(oi => (oi._1, encode(oi._2._1, oi._2._2), oi._3))
        // store geo on a set
        val geoo = rdd.map(oi => (encode(oi._2._1, oi._2._2))).collect()
        geoo.map(x => x).foreach(set += _)

        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        // Convert RDD[String] to DataFrame
        var wordsDataFrame1 = stream_lines_1.toDF("time", "coordinates", "text")


        //**************************** Processing on data frame ****************************************************************************************************************

        wordsDataFrame1 = wordsDataFrame1.withColumn("date_1", split(col("time"), " ")(1))
        wordsDataFrame1 = wordsDataFrame1.withColumn("date_2", split(col("time"), " ")(2))
        wordsDataFrame1 = wordsDataFrame1.withColumn("hours", split(col("date_2"), ":")(0))
        wordsDataFrame1 = wordsDataFrame1.withColumn("minutes", split(col("date_2"), ":")(1))
        wordsDataFrame1 = wordsDataFrame1.select(col("date_1"), col("hours"), col("minutes"), col("coordinates"), split(col("text"), " ") as "text")
        wordsDataFrame1 = wordsDataFrame1.withColumn("words", explode(col("text")))
        wordsDataFrame1 = wordsDataFrame1.drop(col("text"))
        wordsDataFrame1 = wordsDataFrame1.filter(length(col("words")) > 2)

        //****************************************************************************************************************************************

        // Convert DataFrame to RDD
        val rows = wordsDataFrame1.rdd

        // map data into word_geo_date-hours-minutes format
        val edit = rows.map(row => (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        val keyword = edit.map(oi => (oi._5 + "_" + oi._4 + "_" + oi._1 + "-" + oi._2 + "-" + oi._3))

        // Convert  RDD to  DataFrame
        val words_l = keyword.toDF("data")
        val xx = words_l.select("data").as[String].collect()

        // add each word_geo_date-hours-minutes into count min sketch
        xx.foreach(x => countMinSketch.add(x, 1))

      }//rdd
    )//stream_lines


    //**************************** Akka HTTP SERVER ****************************************************************************************************************

    // HTML form to insert a word and 2 dates
    val route: Route = cors() {
      concat(
        path ("home"){
          get{
              val htmlCode =
                """
                <!DOCTYPE html>
                  <html>
                    <head>
                      <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">
                        <link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.4.1/css/all.css" integrity="sha384-5sAR7xN1Nv6T6+dT2mhtzEpVJvfS3NScPQTrOxhwjIuvcA67KV2R5Jz6kr4abQsz" crossorigin="anonymous">
                          <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.2/jquery.min.js" integrity="sha512-tWHlutFnuG0C6nQRlpvrEhE4QpkG1nn2MOUMWmUeRePl4e3Aki0VB6W1v3oLjFtd0hVOtRQ9PHpSfN6u6/QXkQ==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
                          <style>
                            html, body {
                            display: flex;
                            justify-content: center;
                            font-family: Roboto, Arial, sans-serif;
                            font-size: 15px;
                             }
                            form {
                             border: 5px solid #f1f1f1;
                             }
                             input[type=text], input[type=password] {
                             width: 100%;
                             padding: 16px 8px;
                             margin: 8px 0;
                             display: inline-block;
                             border: 1px solid #ccc;
                             box-sizing: border-box;
                             }
                            .icon {
                               font-size: 110px;
                               display: flex;
                               justify-content: center;
                               color: #4286f4;
                            }
                            button {
                               background-color: #4286f4;
                               color: white;
                               padding: 14px 0;
                               margin: 10px 0;
                               border: none;
                               cursor: grab;
                               width: 48%;
                            }
                            h1 {
                              text-align:center;
                              fone-size:18;
                            }
                            button:hover {
                            opacity: 0.8;
                            }
                           .formcontainer {
                             text-align: center;
                              margin: 24px 50px 12px;
                           }
                           .container {
                           padding: 16px 0;
                           text-align:left;
                           }
                           span.psw {
                            float: right;
                            padding-top: 0;
                            padding-right: 15px;
                           }
                           @media screen and (max-width: 300px) {
                             span.psw {
                               display: block;
                                 float: none;
                                }
                             }
                          </style>
                    </head>
                  <body>
                  <form action="http://localhost:9001/search" id="form" method="get">
                      <div class="formcontainer">
                            <div class="container">
                                <label><strong>Start Date</strong></label>
                                <input type="text" placeholder="Enter Start Date" name="sdate" id="sdate" required>
                                <label ><strong>End Date</strong></label>
                                <input type="text" placeholder="Enter End Date" name="edate" id="edate" required>
                                <label><strong>Word</strong></label>
                                <input type="text" placeholder=Enter word to search for" name="word" id="word" required>
                            </div>
                            <button type="submit" id="submit-btn"><strong>Sent</strong></button>
                      </div>
                  </form>
                  <script>
                    $("#form").on('submit',function(e){
                      e.preventDefault();
                      let url = $('#form').attr("action") + "/" + $("#sdate").val() + "/" + $("#edate").val() + "/" + $("#word").val()
                      window.location.href = url;
                      return false;
                    });

                  </script>
                </body>
              </html>
              """
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,htmlCode))
          }
        },
        get {
          pathPrefix("search"/ akka.http.scaladsl.server.PathMatchers.Segment / akka.http.scaladsl.server.PathMatchers.Segment / akka.http.scaladsl.server.PathMatchers.Segment ) { (start_date_http,end_date_http,word_http) =>

            // get data from the form
            val start_d = start_date_http
            val end_d = end_date_http
            val word = word_http
            //split minutes
            var s = start_d.split("-")(4).toInt
            val e = end_d.split("-")(4).toInt
            print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
            // initialize a count to compute frequency of the word
            var count = 0
            // initialize 2 buffer lists to store dates and counts
            var datelist = new ListBuffer[String]()
            var countlist = new ListBuffer[Long]()

            // for each geo_hash
            val res = set.map(geohash => {

                  // while start date < end date
                  while (s <= e) {

                    // map data into word_geo_date
                    val Fdata = start_d.split("-")(0) + "-" + start_d.split("-")(1) + "-" + start_d.split("-")(2) + "-" + start_d.split("-")(3) + "-" + s.toString
                    val word_date_geo = word + "_" + geohash + "_" + Fdata
                    println(word_date_geo)

                    // estimate count for word_geo_date
                    val results = countMinSketch.estimateCount(word_date_geo)
                    println(results)

                    // store dates,counts into their lists
                    datelist += Fdata
                    countlist += results

                    // store counts to set
                    val Data_Count = (Fdata, results).toString()
                    setCH += Data_Count

                    // store decode for the geo_hashes into a set
                    val Geo_Count = (decode(geohash), results).toString()
                    setG += Geo_Count
                    count = count + results.asInstanceOf[Int]

                    s = s + 1;

                    (word_date_geo, results)

                  }
                }
            )


            println(setCH)
            println(setG)
            println("############################################################################################################################################\n")
            println(count)
            println("############################################################################################################################################\n")

            // convert buffer lists into list
            val DL = datelist.toList
            val CL = countlist.toList
            print(CL)

            // map them into a specific format
            val str_DL = DL.mkString("[\"", "\", \"", "\"]")
            val str_CL = CL.mkString("[", ", ", "]")
            print(str_CL)

            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"""{"start": "$start_d" ,"end": "$end_d","word":"$word_http","count":$count ,"data":"$setG"}"""))

            // send data to chart JS
            val htmlCode =
              s"""
                   <!DOCTYPE html>
                      <html>
                      <head>
                        <style>
                            .myDiv {
                               border: 5px outset red;
                               background-color: lightblue;
                               text-align: center;
                              }

                              .gfg {
                                background-color: white;
                                border: 2px solid black;
                                color: red;
                                padding: 5px 10px;
                                text-align: center;
                                display: inline-block;
                                font-size: 20px;
                                margin: 10px 30px;
                                cursor: pointer;
                                }
                        </style>
                      </head>
                      <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.7.3/Chart.min.js"></script>
                      <body>
                      <div class="myDiv">
                         <h2>The Frequency Of $word_http  is $count</h2>
                      </div>
                      <div>
                          <br>
                          <br>
                      </div>
                      <div class="card-body"><canvas id="myBarChart" width="100%" height="40"></canvas></div>
                      <br>
                      <button class = "gfg" type="submit">
                                           <a href="http://localhost:9005/home1">Go to Map</a>
                      </button>

                      <script>
                      Chart.defaults.global.defaultFontColor = '#292b2c';
                      var ctx = document.getElementById("myBarChart");
                      var myLineChart = new Chart(ctx, {
                        type: 'bar',
                        data: {
                          labels: $str_DL,

                          datasets: [{
                            label: "$word_http",
                            backgroundColor: "rgba(2,117,216,1)",
                            borderColor: "rgba(2,117,216,1)",
                            data: $str_CL,
                          }],
                        },
                        options: {
                          scales: {
                            xAxes: [{
                              time: {
                                unit: 'date'
                              },
                              gridLines: {
                                display: false
                              },
                              ticks: {
                                maxTicksLimit: 6
                              }
                            }],
                            yAxes: [{
                              ticks: {
                                min: 0,
                                maxTicksLimit: 5
                              },
                              gridLines: {
                                display: true
                              }
                            }],
                          },
                          legend: {
                            display: false
                          }
                        }
                      });
                      </script>
                      </body>
                   </html>
                   """
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, htmlCode))
          }
        }
      )
    }


    val bindingFuture = Http().newServerAt("localhost", 9001).bind(route)
    println(s"Server now online. Please navigate to http://localhost:9001/home\n")

    ssc.start()
    ssc.awaitTermination()
  }}