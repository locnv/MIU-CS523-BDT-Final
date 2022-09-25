package cs523_final.spark_sql;

import org.apache.spark.sql.SparkSession;

public abstract class QueryBase {
	protected SparkSession session;
	protected String hiveTable;
	
	public QueryBase(SparkSession session, String hiveTable) {
		this.session = session;
		this.hiveTable = hiveTable;
	}
	
	/**
	 * Demonstrating running simple query:
	 * Select * from hiveTable limit 10;
	 */
	protected void runSimpleQuery() {
		StringBuilder sb = new StringBuilder();
		sb
			.append("select * from ")
			.append(this.hiveTable)
			.append(" limit 10");
		String query = sb.toString();
		this.session.sql(query).show();
	}
	
	protected void runQueryDemo() {
		this.runSimpleQuery();
		this.runQueries();
	}
	
	abstract void runQueries();
	
}
