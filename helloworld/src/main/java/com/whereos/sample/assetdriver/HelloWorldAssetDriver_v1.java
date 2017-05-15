package com.whereos.sample.assetdriver;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import com.whereos.driver.AssetDriver;
import com.whereos.driver.DataAsset;
import com.whereos.driver.DriverException;
import com.whereos.driver.InmemoryData;
import com.whereos.driver.ui.HTMLRow;
import com.whereos.driver.ui.HTMLTable;
import com.whereos.driver.ui.InputColumn;
import com.whereos.driver.ui.TextColumn;
import com.whereos.driver.TaskSettings;

public class HelloWorldAssetDriver_v1 extends AssetDriver {

	@Override
	public void writeAsset(Dataset<Row> df, DataAsset asset, TaskSettings options, OutputStream stream)
			throws DriverException {
		// TODO Auto-generated method stub
		
	}

	private StructType getMySchema() {
		ArrayList<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("my_field_1", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("my_field_2", DataTypes.IntegerType, true));
		 
		StructType schema = DataTypes.createStructType(fields);
		return schema;
	}
	@Override
	public Dataset<Row> readAsset(SparkSession context, DataAsset asset, TaskSettings options, InmemoryData in)
			throws DriverException {

		
		StructType schema = getMySchema();
		
		ArrayList<Row> list = new ArrayList<Row>();
		int count = asset.json.getInt("count");

		for (int i = 0; i < count; i++) {

			Object obj[] = new Object[2];
			obj[0] = "Hello World";
			obj[1] = i;
				
			Row testRow = RowFactory.create(obj);
			list.add(testRow);

		}
		//System.out.println(data);
		int slices = 1;
		if (count > 100) {
			slices = count/50;
		}
		
		JavaSparkContext c = JavaSparkContext.fromSparkContext(context.sparkContext());
		JavaRDD<Row> testRDD = c.parallelize(list, slices);
		Dataset<Row> ds = context.createDataFrame(testRDD, schema);

		return ds;
		
	}
	@Override
	public JSONObject getSchema(SparkSession context, DataAsset asset, TaskSettings options) throws DriverException {
		JSONObject out = new JSONObject();
		recursiveSchema(out, getMySchema());
		return out;
	}

	@Override
	public boolean checkStatus(SparkSession context, DataAsset asset, TaskSettings options) throws DriverException {
		return true;
	}

	@Override
	public boolean isAPI() {
		return false;
	}

	@Override
	public boolean hasTempTables() {
		return false;
	}

	@Override
	public String getDisplayName() {
		return "Hello World";
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public JSONObject handleSave(Map<String, String[]> parameters) {
		JSONObject obj = new JSONObject();
		String paramStr[] = parameters.get("count");
		if (paramStr.length > 1) throw new RuntimeException("Expecting only one value in count parameter");
		if (paramStr.length == 0) throw new RuntimeException("Count parameter is missing");
		int count = Integer.parseInt(paramStr[0]);
		obj.put("count", count);
		return obj;
	}

	@Override
	public String handleLoad(DataAsset asset) {
		
		if (asset == null) {
			asset = new DataAsset();
			asset.json = new JSONObject();
		}
		
		ArrayList<HTMLRow> rows = new ArrayList<HTMLRow>();

		rows.add(new HTMLRow(
				new TextColumn("Count","width:200px"),
				new InputColumn("count",asset.json.optString("count", "1"), "", "width:400px", "width:100px")));

		HTMLTable table1 = new HTMLTable(rows);
		
		return  table1.toHTML();
	
	}

}
