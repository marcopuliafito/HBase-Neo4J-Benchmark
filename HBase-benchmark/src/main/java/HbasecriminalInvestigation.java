/**
 *
 * @author marco
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbasecriminalInvestigation {
    //call_1000 = 1000 records
    static String fileName = "../dataset/Call_10000.csv";
    
    protected static String MY_NAMESPACE_NAME = "HbaseBenchmark"; //HbaseBenchmark ---> 1000 records
    static TableName MY_TABLE_NAME = TableName.valueOf("criminal_data_10000"); //criminal_data ----> 1000 records
    static Table table;
    
    static byte[] MY_COLUMN_FAMILY_NAME_PERSON = Bytes.toBytes("Person");
    static byte[] MY_COLUMN_FAMILY_NAME_CALL = Bytes.toBytes("Call");
    static byte[] MY_COLUMN_FAMILY_NAME_LOCATION = Bytes.toBytes("Location");
    
    static String[]columnPerson = {"ID"/*row key*/,"FULL_NAME","FIRST_NAME","LAST_NAME","CALLING_NBR"};
    static String[]columnCall = {"ID"/*row key*/,"FULL_NAME_CALLED","CALLED_NBR","START_DATE","DURATION","END_DATE"};
    static String[]columnLocation = {"ID"/*row key*/,"CELL_TOWER","CITY","STATE","ADDRESS"};
    static int input;
    FileWriter w;
    HbaseController hbase = new HbaseController();
    
    public HbasecriminalInvestigation() throws IOException{
        this.fileName = fileName;
        this.MY_NAMESPACE_NAME = MY_NAMESPACE_NAME;
        this.MY_TABLE_NAME = MY_TABLE_NAME;
        this.MY_COLUMN_FAMILY_NAME_PERSON = MY_COLUMN_FAMILY_NAME_PERSON;
        this.MY_COLUMN_FAMILY_NAME_CALL = MY_COLUMN_FAMILY_NAME_CALL;
        this.MY_COLUMN_FAMILY_NAME_LOCATION = MY_COLUMN_FAMILY_NAME_LOCATION;
        this.columnPerson = columnPerson;
        this.columnCall = columnCall;
        this.columnLocation = columnLocation;
        this.table = table;
    }
    
    boolean connectionHBase() throws IOException{
        try{
            Configuration config = HBaseConfiguration.create();

            /**
            * ConnectionFactory#createConnection() automatically looks for
            * hbase-site.xml (HBase configuration parameters) on the system's
            * CLASSPATH, to enable creation of Connection to HBase via ZooKeeper.
            */
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            //admin.getClusterMetrics(); // assure connection successfully established
            System.out.println("\n*** CONNESSIONE STABILITA ***\n");
            //creo la tabella se non è stata creata specificando la struttura delle column family
            hbase.createNamespaceAndTable(admin,MY_NAMESPACE_NAME, MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME_PERSON,MY_COLUMN_FAMILY_NAME_CALL,MY_COLUMN_FAMILY_NAME_LOCATION);
            //effettua la connessione con hbase e ottieni Table table
            this.table = connection.getTable(MY_TABLE_NAME);
            return true;
        }
        catch(Exception e){
            return false;
        } 
    } 
    
    void putValuePerson(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_PERSON, this.columnPerson);
    }
    void putValueCall(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_CALL, this.columnCall);
    }
    void putValueLocation(){ 
        hbase.importLocalFileToHBase(this.fileName, this.table, this.MY_COLUMN_FAMILY_NAME_LOCATION, this.columnLocation);
    }
    
    //Cercare il proprietario del numero: 86(644)491-7854
    void query1() throws IOException{
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_PERSON,
            Bytes.toBytes("CALLING_NBR"), CompareOp.EQUAL, Bytes.toBytes("86(644)491-7854"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        //stampo il risultato di scan
        /*for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }*/
    }
     
    void timeQuery1() throws IOException{
        double millisecondi = Math.pow(10, 6);
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/HBase/10000_records/Query1";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 1");
        for(int i=0; i<31; i++){
            long start = System.nanoTime(); //nano time è un valore 10 alla meno 9
            /******* ESEGUO LA QUERY 1*******/
            query1();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " ");
        }
        System.out.println("Fine query 1");
        w.flush();
        w.close();  
    }
    
    
    //Filtrare le chiamate effettuate in un determinato momento
    void query2() throws IOException{
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_CALL,
            Bytes.toBytes("START_DATE"), CompareOp.EQUAL, Bytes.toBytes("1537178627"));
        filter.setFilterIfMissing(true); //non contare se manca la colonna
        scan.setFilter(filter);
        ResultScanner scanner = this.table.getScanner(scan);
        Result result = scanner.next();
        //stampo il risultato di scan
        /*for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }*/
    }
     
    void timeQuery2() throws IOException{
        double millisecondi = Math.pow(10, 6);
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/HBase/10000_records/Query2";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 2");
        for(int i=0; i<31; i++){
            long start = System.nanoTime(); //nano time è un valore 10 alla meno 9
            /******* ESEGUO LA QUERY 1*******/
            query2();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " ");
        }
        System.out.println("Fine query 2");
        w.flush();
        w.close();  
    }
    
    //Cercare le chiamate fatte al numero : 380(486)299-6217 or numero : 389(365)470-8680
    void query3() throws IOException{
        List<Filter> filters = new ArrayList<Filter>(); //array di filtri (per fare condizioni concatenate)
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_CALL,
            Bytes.toBytes("CALLED_NBR"), CompareOp.EQUAL, Bytes.toBytes("380(486)299-6217"));
        filters.add(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_CALL,
            Bytes.toBytes("CALLED_NBR"), CompareOp.EQUAL, Bytes.toBytes("389(365)470-8680"));
        filters.add(filter2);
         //creo un istanza di FIlterList,FilterList.Operator.MUST_PASS_ONE: corrisponde ad OR
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = this.table.getScanner(scan);
        Result result = scanner.next();
        //stampo il risultato di scan
        /*for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }*/
    }
     
  
    void timeQuery3() throws IOException{
        double millisecondi = Math.pow(10, 6);
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/HBase/10000_records/Query3";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 3");
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            /******* ESEGUO LA QUERY 2*******/
            query3();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " ");
        }
        System.out.println("Fine query 3");
        w.flush();
        w.close();  
    }
  
        
    //Filtrare le chiamate avvenute in una determinata zona e data 
    void query4() throws IOException{
        List<Filter> filters = new ArrayList<Filter>(); //array di filtri (per fare condizioni concatenate)
        Scan scan = new Scan();
        //data zona (cell tower)
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_LOCATION,Bytes.toBytes("CELL_TOWER"), 
            CompareOp.EQUAL, Bytes.toBytes("115"));
        filters.add(filter1);
        //chiamata effettuata dopo il time indicato come value
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_CALL,Bytes.toBytes("START_DATE"), 
            CompareOp.GREATER, Bytes.toBytes("1578753860"));
        filters.add(filter2);
        //chiamata effettuata prima del time indicato come value
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_CALL,Bytes.toBytes("START_DATE"), 
            CompareOp.LESS, Bytes.toBytes("1589725288"));
        filters.add(filter3);
        //creo un istanza di FIlterList,FilterList.Operator.MUST_PASS_ALL: corrisponde ad AND
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = this.table.getScanner(scan);
        Result result = scanner.next();
        /*//stampo il risultato di scan
        for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength()));
        }
        }*/
    }
     
    void timeQuery4() throws IOException{
        double millisecondi = Math.pow(10, 6);
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/HBase/10000_records/Query4";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 4");
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            /******* ESEGUO LA QUERY 2*******/
            query4();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " ");
        }
        System.out.println("Fine query 4");
        w.flush();
        w.close();  
    }
    
    //Cercare le chiamate effettuate dalla città di Albany e da New York city
    void query5() throws IOException{
        List<Filter> filters = new ArrayList<Filter>(); //array di filtri (per fare condizioni concatenate)
        Scan scan = new Scan();
        //data zona (cell tower)
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_LOCATION, Bytes.toBytes("CITY"), 
           CompareOp.EQUAL, Bytes.toBytes("Albany"));
        filters.add(filter1);
        //chiamata effettuata dopo il time indicato come value
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(this.MY_COLUMN_FAMILY_NAME_LOCATION, Bytes.toBytes("CITY"), 
            CompareOp.EQUAL, Bytes.toBytes("New York City"));
        filters.add(filter2);
        //creo un istanza di FIlterList,FilterList.Operator.MUST_PASS_ONE: corrisponde ad OR
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters); 
        scan.setFilter(filterList);
        ResultScanner scanner = this.table.getScanner(scan);
        Result result = scanner.next();
        /*//stampo il risultato di scan
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
            }
        }*/
    }
     
  
    void timeQuery5() throws IOException{
        double millisecondi = Math.pow(10, 6);
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/HBase/10000_records/Query5";
        w = new FileWriter(percorso);
        System.out.println("Esecuzione query 5");
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            /******* ESEGUO LA QUERY 2*******/
            query5();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi); //trasformo in millisecondo dividendo per 10 alla meno 6
            w.write(total + " ");
        }
        System.out.println("Fine query 5");
        w.flush();
        w.close();  
    }

}
