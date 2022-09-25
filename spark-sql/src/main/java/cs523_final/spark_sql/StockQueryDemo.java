package cs523_final.spark_sql;

import org.apache.spark.sql.SparkSession;

public class StockQueryDemo extends QueryBase {

	public StockQueryDemo(SparkSession session) {
		super(session, "stocks");
	}

	@Override
	void runQueries() {
		this.findAppleHighestVolume();
		this.findOpenPriceOnDate();
	}
	
	private void findAppleHighestVolume() {
		StringBuilder sb = new StringBuilder();
		sb
			.append("select * from ")
			.append(this.hiveTable)
			.append(" where Volume > 140")
			.append(" limit 10");
		String query = sb.toString();
		this.session.sql(query).show();
	}

	private void findOpenPriceOnDate() {
		StringBuilder sb = new StringBuilder();
		sb
			.append("select * from ")
			.append(this.hiveTable)
			.append(" where Date = '2021-10-04'");
		String query = sb.toString();
		this.session.sql(query).show();
	}
	

}
