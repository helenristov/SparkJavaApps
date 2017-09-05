package com.datastax.spark.demo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class JavaDemo implements Serializable {

    private transient SparkConf conf;

    private JavaDemo(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
        sc.stop();
    }
    
    public static class Person implements Serializable {
        private Integer id;
         private String name;
         private Date birthDate;

        // Remember to declare no-args constructor
         public Person() { }

        public Person(Integer id, String name, Date birthDate) {
        this.id = id;
        this.name = name;
        this.birthDate = birthDate;
}

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }

        public String getName() { return name; }
         public void setName(String name) { this.name = name; }

        public Date getBirthDate() { return birthDate; }
        public void setBirthDate(Date birthDate) { this.birthDate = birthDate; }

        // other methods, constructors, etc.
}

    public void generateData(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS ks");
            session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE ks.people (id INT PRIMARY KEY, name TEXT, birthDate Date)");}

     // Prepare the data to be inserted
         List<Person> people = Arrays.asList(
                new Person(1, "John", new Date()),
                new Person(2, "Troy", new Date()),
                new Person(3, "Andrew", new Date())
        );
        JavaRDD<Person> rdd = sc.parallelize(people);
        javaFunctions(rdd).writerBuilder("ks", "people", mapToRow(Person.class)).saveToCassandra();

      }
    
     public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(JavaDemo.class);
                logger.info("Java Demo");

	if (args.length != 2) {
            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
            
   	 for(int i = 0;i<args.length;i++) {
     		System.out.print(args[i]);
	 }

            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("spark://192.168.1.184:7077");
       //conf.set("spark.cassandra.connection.host", "192.168.1.184");

        JavaDemo app = new JavaDemo(conf);
        app.run();
    }


}
