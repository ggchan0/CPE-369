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

      val conf = new SparkConf().setAppName("Lab6")

      val sc = new SparkContext(conf)
      
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

      //ProductID, (SalesID, quantity)
      val itemsMap = items.map(line => (line.split(",")(2).trim.toInt, 
      (line.split(",")(1).trim.toInt, 
      line.split(",")(3).trim.toInt)))

      //ProductID, price
      val productMap = products.map(line => (line.split(",")(0).trim.toInt, 
      line.split(",")(2).trim.toFloat))

      //SaleID, StoreID
      val saleMap = sales.map(line => (line.split(",")(0).trim.toInt, 
      line.split(",")(3).trim.toInt))

      //StoreID, State
      val storeMap = stores.map(line => (line.split(",")(0).trim.toInt, 
      line.split(",")(5).trim))

      //Join each purchase with the corresponding product and get the saleID and the total price
      //Join each saleID, total with the sale RDD to get the store ID and total
      //Join the storeID, total with the store RDD to get the state, storeID, and total for that particular product
      //Reduce by key to get the total earned by the store for every product
      val res = itemsMap.leftOuterJoin(productMap)
         .map{case(productID, ((salesID, quantity), Some(price))) => (salesID, quantity * price)}
         .leftOuterJoin(saleMap)
         .map{case(saleID, (total, Some(storeID))) => (storeID, total)}
         .leftOuterJoin(storeMap)
         .map{case(storeID, (total, Some(state))) => ((state, storeID), total)}
         .reduceByKey((a, b) => a + b) //get totals for each store
         .sortByKey() //sorts results
         .collect() //collects into an array
         
      res.foreach{case((state, storeID), total) => println(state + ", " + storeID + ", " + total)}
   }
}