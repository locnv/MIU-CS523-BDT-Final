package cs523_final.spark_sql;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Spark SQL Demonstrating
 *
 */
public class App 
{
	final static String HIVE_WAREHOUSE_DIR = "hdfs://quickstart.cloudera:8020/user/hive/warehouse/";
	final static String HIVE_METASTORE_URL = "thrift://localhost:9083";
	final static String HIVE_EXEC_SCRATCH_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/stock/";
    public static void main( String[] args )
    {
    	SparkConf conf = new SparkConf()
    		.setAppName("StreamingJob")
    		.setMaster("local[1]");

    	conf.set("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR);
		conf.set("hive.metastore.uris", HIVE_METASTORE_URL);
		conf.set("hive.exec.scratchdir", HIVE_EXEC_SCRATCH_DIR);
		
		SparkSession ss = SparkSession
				.builder()
				.master("local")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
//		ss.sql("show databases").show();
		ss.sql("use default").show(false);
//		ss.sql("show tables").show();
		
		QueryBase stockQueries = new StockQueryDemo(ss);
		QueryBase cryptoQueries = new CryptoQueryDemo(ss);
		
		stockQueries.runQueryDemo();
//		cryptoQueries.runQueryDemo();
    }
}
