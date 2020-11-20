/**
 *
 * @author marco
 */
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public final class HbaseController {
    /*static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
    static final byte[] MY_FIRST_COLUMN_QUALIFIER
            = Bytes.toBytes("myFirstColumn");
    static final byte[] MY_SECOND_COLUMN_QUALIFIER
            = Bytes.toBytes("mySecondColumn");
    static final byte[] MY_ROW_ID = Bytes.toBytes("rowId01");*/
    // Private constructor included here to avoid checkstyle warnings
    public HbaseController() {
    }
    

    public void importLocalFileToHBase(String fileName, Table table, byte[] columnFamily, String[] column) {
        long st = System.currentTimeMillis();
        try{	
            int count = 0;
            Reader csvData = new FileReader(fileName);
            CSVParser parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(csvData);
            for (CSVRecord csvRecord : parser) { //scorre le righe del csv
                String rowKey = csvRecord.get("ID");//prendo la colonna con header ID
                Put put = new Put(Bytes.toBytes(rowKey));//creo un inserimento a partire dalla row key
                //scorro la riga in base alle colonne interessate
                for(int i=1;i<column.length;i++){
                    //inserimento nella columnfamily delle colonne con lo scorrere dei valori in csv
                    put.addColumn(columnFamily, Bytes.toBytes(column[i]),Bytes.toBytes(csvRecord.get(column[i])));
                }
                try {
                   table.put(put); // put to server
                }
                catch (IOException e) {
                   e.printStackTrace();
                }
                
                //stampo il contenuto della table--> CF : [column == value,..]
                Result row = table.get(new Get(Bytes.toBytes(rowKey)));
                
                System.out.println("Row [" + Bytes.toString(row.getRow())
                   + "] was retrieved from Table ["
                   + table.getName().getNameAsString()
                   + "] in HBase, with the following content:");

                for (Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry
                    : row.getNoVersionMap().entrySet()) {
                    String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

                    System.out.println("  Columns in Column Family [" + columnFamilyName
                        + "]:");

                    for (Entry<byte[], byte[]> columnNameAndValueMap
                        : colFamilyEntry.getValue().entrySet()) {

                    System.out.println("    Value of Column [" + columnFamilyName + ":"
                        + Bytes.toString(columnNameAndValueMap.getKey()) + "] == "
                        + Bytes.toString(columnNameAndValueMap.getValue()));
                    }
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        } 
        /*finally { 
            try {
                table.close(); // must close the client
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    } 
         

    /**
    * Invokes Admin#createNamespace and Admin#createTable to create a namespace
    * with a table that has one column-family.
    *
    * @param admin Standard Admin object
    * @throws IOException If IO problem encountered
    */
    public void createNamespaceAndTable(final Admin admin, String name_space, TableName table_name, byte[] columnFamilyPerson, byte[] columnFamilyCall, byte[] columnFamilyLocation) throws IOException {

        if (!namespaceExists(admin, name_space)) {
          System.out.println("Creating Namespace [" + name_space + "].");

             admin.createNamespace(NamespaceDescriptor
                .create(name_space).build());
        }
        if (!admin.tableExists(table_name)) {
            System.out.println("Creating Table [" + table_name.getNameAsString()
                + "], with Column Family ["
                + Bytes.toString(columnFamilyPerson) + ", "
                +Bytes.toString(columnFamilyCall) +  ", "
                + Bytes.toString(columnFamilyLocation) + "].");
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(table_name)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyPerson))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyCall))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyLocation))
                .build();
            admin.createTable(desc);
        }
    }

    /**
    * Checks to see whether a namespace exists.
    *
    * @param admin Standard Admin object
    * @param namespaceName Name of namespace
    * @return true If namespace exists
    * @throws IOException If IO problem encountered
    */
    static boolean namespaceExists(final Admin admin, final String namespaceName)
           throws IOException {
        try {
           admin.getNamespaceDescriptor(namespaceName);
        } catch (NamespaceNotFoundException e) {
          return false;
        }
        return true;
    }

}
  
