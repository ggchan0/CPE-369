import scala.collection.immutable.TreeMap
import scala.io._
import scala.collection.JavaConversions._

object Lab5 {
   def main(args: Array[String]) : Unit = {
      // ID, name, birthday, address, city, ZIP, state, phoneNumber
      val customers = Source.fromFile("./customers2.csv").getLines().toList;
      
      // ID, SalesID, ProductID, quantity
      val items = Source.fromFile("./lineItem2.csv").getLines().toList;
      
      // ID, description, price
      val products = Source.fromFile("./products2.csv").getLines().toList;
      
      // ID, date, time, storeID, customerID
      val sales = Source.fromFile("./sales5.csv").getLines().toList;
      
      // ID, storename, address, city, zip, state, phoneNumber
      val stores = Source.fromFile("./stores2.csv").getLines().toList;

      // storeId, storeName
      val storeToName = stores.map(line => (line.split(",")(0).trim.toInt, line.split(",")(1).trim)).toMap;

      // storeId, storeState
      val storeToState = stores.map(line => (line.split(",")(0).trim.toInt, line.split(",")(5).trim)).toMap;

      // salesId, (productId, quantity)
      val transactions = items.map(line => (line.split(",")(1).trim.toInt, (line.split(",")(2).trim.toInt, line.split(",")(3).trim.toInt)))

      // salesId, storeId
      val salesToStore = sales.map(line => (line.split(",")(0).trim.toInt, line.split(",")(3).trim.toInt)).toMap;

      // productId, price
      val productsToPrice = products.map(line => (line.split(",")(0).trim.toInt, line.split(",")(2).trim.toFloat)).toMap;
     
      val map = new java.util.TreeMap[String, Float]();

      transactions.foreach{
         case(salesId, (productId, quantity)) =>
            val price = productsToPrice.getOrElse(productId, 0.0);

            val income = price.toString.toFloat * quantity.toFloat;
            val storeId = salesToStore.getOrElse(salesId, 0);

            val storeName = storeToName.getOrElse(storeId, "");

            val state = storeToState.getOrElse(storeId, "");

            val compositeKey = state + ", " + storeId.toString;

            val oldIncome = map.getOrElse(compositeKey, 0.0);
            
            map.put(compositeKey, oldIncome.toString.toFloat + income.toFloat);
      }

      for (pair <- map.entrySet) {
         System.out.println(pair.getKey + "," + pair.getValue);
      }
   }   
}
