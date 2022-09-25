package cs523_final.spark_sql;

import org.apache.spark.sql.SparkSession;

public class CryptoQueryDemo extends QueryBase {

	public CryptoQueryDemo(SparkSession session) {
		super(session, "cryptos");
	}

	@Override
	void runQueries() {
		 this.findDateWithHighestVolume();
	}
		
	private void findDateWithHighestVolume() {
		StringBuilder sb = new StringBuilder();
		sb
			.append("select Date, Volume from ")
			.append(this.hiveTable)
			.append(" order by Volume DESC")
			.append(" limit 5");
		String query = sb.toString();
		this.session.sql(query).show();
	}

}
