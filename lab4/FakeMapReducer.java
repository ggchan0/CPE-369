import java.util.*;
import java.lang.*;
import java.io.*;
import java.text.*;

public class FakeMapReducer {

    //Returns a treeset of the top 10 most expensive products
    public static TreeSet<Product> parseData(String filename) throws FileNotFoundException, ParseException {
        File file = new File(filename);
        Scanner scan = new Scanner(file);
        TreeSet<Product> set = new TreeSet<>();
        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            String [] fields = line.split(",");
            Product p = new Product(Integer.parseInt(fields[0]), fields[1], Float.parseFloat(fields[2]));
            set.add(p);
            if (set.size() > 10) {
                set.pollLast();
            }
        }

        return set;
    }

    public static void main(String [] argv) {
        if (argv.length == 0) {
            System.out.println("Need file");
        }
        TreeSet<Product> set = null;

        try {
            set = parseData(argv[0]);
        } catch (FileNotFoundException e) {
            System.out.println("Enter in a file that exists");
            System.exit(0);
        } catch (ParseException e) {
            System.out.println("Incorrect date format for input file");
            System.exit(0);
        } catch (Exception e) {
            throw e;
        }

        for (Product p : set) {
            System.out.println(p);
        }

    }

    public static class ProductComparator implements Comparator<Product> {
        public int compare(Product p1, Product p2) {
            return p1.compareTo(p2);
        }
    }

    public static class Product implements Comparable<Product> {
        int id;
        String info;
        float price;

        public Product(int id, String info, float price) {
            this.id = id;
            this.info = info;
            this.price = price;
        }

        public String toString() {
            return this.id + ", " + this.info + ", " + this.price; 
        }

        public int compareTo(Product p) {
            if (this.price > p.price) {
                return -1;
            } else if (this.price < p.price) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
