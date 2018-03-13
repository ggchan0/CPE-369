import java.util.*;
import java.lang.*;
import java.io.*;
import java.text.*;
import java.time.*;

public class FakeMapReducer {
    public static HashMap<Date, Integer> parseData(String filename) throws FileNotFoundException, ParseException {
        File file = new File(filename);
        Scanner scan = new Scanner(file);
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");
        HashMap<Date, Integer> map = new HashMap<>();
        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            Date date = dateFormat.parse(line.split(",")[1]);
            int val = 0;
            if (map.containsKey(date)) {
                val = map.get(date);
            }

            map.put(date, val + 1);
        }

        return map;
    }

    public static void printSortedOrder(HashMap<Date, Integer> map) throws IOException {
        PrintWriter writer = new PrintWriter("output.txt", "UTF-8");
        Date [] keys = map.keySet().toArray(new Date[map.size()]);
        Arrays.sort(keys);
        for (Date key : keys) {
            //System.out.println();
            writer.println("Date: " + key + ", Total Sales: " + map.get(key));
        }
        writer.close();
    }

    public static void main(String [] argv) {
        if (argv.length == 0) {
            System.out.println("Need file");
        }
        HashMap<Date, Integer> map = null;

        try {
            map = parseData(argv[0]);
        } catch (FileNotFoundException e) {
            System.out.println("Enter in a file that exists");
            System.exit(0);
        } catch (ParseException e) {
            System.out.println("Incorrect date format for input file");
            System.exit(0);
        } catch (Exception e) {
            throw e;
        }

        try {
            printSortedOrder(map);
        } catch (IOException e) {
            System.out.println("IOException");
        }
    }

    public static class SaleTime implements Comparable<SaleTime> {
        private String time;
        private int sale;

        SaleTime() {

        }

        SaleTime(String time, int sale) {
            this.time = time;
            this.sale = sale;
        }

        public int compareTo(SaleTime b) {
            return this.time.compareTo(b.time);
        }
    }
}

/*
    read line by line
    key : date
    value : count
    get keyset and sort it
    print data out

*/
