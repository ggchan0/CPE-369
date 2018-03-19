import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._

object App {
   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      val conf = new SparkConf().setAppName("Lab7")

      val sc = new SparkContext(conf)

      def convertDateFormat(x: String) : String = {
         return x.split("/")(2).trim + "-" + x.split("/")(0).trim
      }

      // ID, name, birthday, address, city, ZIP, state, phoneNumber
      val customers = sc.textFile("/home/ggchan/CPE-369/lab5/customers2.csv")

      // ID, SalesID, ProductID, quantity
      val items = sc.textFile("/home/ggchan/CPE-369/lab5/lineItem2.csv")

      // ID, description, price
      val products = sc.textFile("/home/ggchan/CPE-369/lab5/products2.csv")

      // ID, date, time, storeID, customerID
      val sales = sc.textFile("/home/ggchan/CPE-369/lab5/sales5.csv")

      // ID, storename, address, city, zip, state, phoneNumber
      val stores = sc.textFile("/home/ggchan/CPE-369/lab5/stores2.csv")

      // SalesID, (ProductID, quantity)
      val itemMap = items.map(line => (line.split(",")(1).trim.toInt,
         (line.split(",")(2).trim.toInt, line.split(",")(3).trim.toInt)))
   
      // SalesID, (Date, StoreID)
      val saleMap = sales.map(line => (line.split(",")(0).trim.toInt, 
         (convertDateFormat(line.split(",")(1).trim), 
         line.split(",")(3).trim.toInt)))

      // StoreID, (Store Name, State)
      val storeMap = stores.map(line => (line.split(",")(0).trim.toInt, 
      (line.split(",")(1).trim, line.split(",")(5).trim)))
      
      // ProductID, price
      val productMap = products.map(line => (line.split(",")(0).trim.toInt, 
      line.split(",")(2).trim.toFloat))

      val res = itemMap.leftOuterJoin(saleMap)
         .map{case(salesID, ((productID, quantity), Some((date, storeID)))) => (storeID, (date, productID, quantity))}
         .leftOuterJoin(storeMap)
         .map{case(storeID, ((date, productID, quantity), Some((storeName, state)))) => (productID, (quantity, storeName, state, date))}
         .leftOuterJoin(productMap)
         .map{case(productID, ((quantity, storeName, state, date), Some(price))) => ((date, (storeName, state)), quantity * price)}
         .reduceByKey((x,y) => x + y) //Get total income for that year-month for the business
         .map{case((date,(storeName, state)), income) => (date, (income, (storeName, state)))}
         .groupByKey() //Group all the businesses by the year-month
         .map{case(date, list) => (date, list.toList.sorted.reverse.slice(0,10))} //Get the top 10 for that month
         .sortByKey()
         .collect()

      res.foreach(println);
   }
}