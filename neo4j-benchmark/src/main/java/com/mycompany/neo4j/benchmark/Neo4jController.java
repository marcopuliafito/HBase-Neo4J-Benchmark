package com.mycompany.neo4j.benchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

/**
 *
 * @author marco
 */
public class Neo4jController {
    private final Driver driver;
    String uri = "bolt://localhost:7687"; //collegati all'indirizzo del server local di neo4j
    String user = "neo4j"; //username per l'accesso  area personale db
    String password = "guitar22"; //password per accesso area personale db
    public Neo4jController(){
       this.uri = uri;
       this.user = user;
       this.password = password;
       driver = GraphDatabase.driver(this.uri, AuthTokens.basic(this.user, this.password));
    }
    
    public void close() throws Exception{
        driver.close();
    }
    
    public boolean connectionNeo4j(){
        try{
            Session session = driver.session();
            Neo4jController db = new Neo4jController();
            return true;   
        } catch(Exception e){
            return false;     
        }
    }
    
    
    
    public void timeQuery1() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        String separator = System.getProperty("line.separator");
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/Neo4j/10000_records/Query1";
        w = new FileWriter(percorso);
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            query1();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi);
            w.write(total + " ");
        }
        w.flush();
        w.close();
        close(); //driver.close
    }
    //Cercare il proprietario del numero: 86(644)491-7854
    public void query1(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH(c:PERSON {number:'86(644)491-7854'}) RETURN c");
        }catch(Exception ex){
        }
    }
    
    
    public void timeQuery2() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        String separator = System.getProperty("line.separator");
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/Neo4j/10000_records/Query2";
        w = new FileWriter(percorso);
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            query2();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi);
            w.write(total + " ");
        }
        w.flush();
        w.close();
        close(); //driver.close
    }
    //Filtrare le chiamate effettuate in un determinato momento
    public void query2(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH(a:CALL {start:'1537178627'}) RETURN a");
            /*while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }*/
        }catch(Exception ex){
        }
    }


    public void timeQuery3() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        String separator = System.getProperty("line.separator");
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/Neo4j/10000_records/Query3";
        w = new FileWriter(percorso);
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            query3();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi);
            w.write(total + " ");
        }
        w.flush();
        w.close();
        close(); //driver.close
    }
    //Cercare le chiamate fatte al numero : 380(486)299-6217 or numero : 389(365)470-8680
    public void query3(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (c:PERSON)-[:MADE_CALL]->(a)-[:RECEIVED_CALL]->(d:PERSON)\n" +
                "WHERE d.number='380(486)299-6217' OR d.number = '389(365)470-8680'\n" +
                "RETURN a");
            /*while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }*/
        }catch(Exception ex){
        }
    }
    
    public void timeQuery4() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        String separator = System.getProperty("line.separator");
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/Neo4j/10000_records/Query4";
        w = new FileWriter(percorso);
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            query4();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi);
            w.write(total + " ");
        }
        w.flush();
        w.close();
        close(); //driver.close
    }
    //Filtrare le chiamate avvenute in una determinata zona e data.
    public void query4(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (a:CALL)-[:LOCATED_IN]->(b:LOCATION) WHERE b.cell_tower ='115' "
                + "AND '1578753860' < a.start AND a.start < '1589725288' WITH a, b MATCH (c:PERSON)-[:MADE_CALL]->(a)-[:RECEIVED_CALL]->(d:PERSON)"
                + "RETURN c.full_name");
            /*while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }*/
        }catch(Exception ex){
        }
    }
    
    public void timeQuery5() throws IOException, Exception{
        double millisecondi = Math.pow(10,6);
        FileWriter w;
        String separator = System.getProperty("line.separator");
        String percorso = System.getProperty("user.dir") + File.separator + "../Query_Time/Neo4j/10000_records/Query5";
        w = new FileWriter(percorso);
        for(int i=0; i<31; i++){
            long start = System.nanoTime();
            query5();
            long end = System.nanoTime();
            String total = String.valueOf((end - start) / millisecondi);
            w.write(total + " ");
        }
        w.flush();
        w.close();
        close(); //driver.close
    }
    //Cercare le chiamate effettuate dalla città di Albany e da New York city
    public void query5(){
        try(Session session = driver.session()){
            Result result = session.run("MATCH (a:CALL)-[:LOCATED_IN]->(b:LOCATION) WHERE b.city = 'Albany' OR  b.city = 'New York City' Return a");
            /*while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record.fields());
            }*/
        }catch(Exception ex){
        }
    }
}
    