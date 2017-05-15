package com.whereos.sample.functiondriver;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.whereos.driver.DriverFunction;
import com.whereos.driver.FunctionDescription;
import com.whereos.driver.FunctionDriver;

@FunctionDescription(name = "helloworld",
	usage = "_FUNC_(x) - Returns string \"Hello World\" appended with parameter",
	example = "helloworld(123) returns \"Hello World 123\""
)
public class HelloWorldFunctionDriver implements UDF1<Number, String>, FunctionDriver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 119238719875923L;

	public String call(Number n) {
		return "Hello World!! "+n.toString();
		
	}
	
	public HelloWorldFunctionDriver() {
	}

	
	public DriverFunction[] getFunctions() {
		DriverFunction[] functions = new DriverFunction[] {
				new DriverFunction(DriverFunction.SPARK_UDF,"helloworld", this.getClassName(), DataTypes.StringType)
		};
		return functions;
	}

	public String getDisplayName() {
		return "Hello World Driver";
	}

	public String getClassName() {
		return this.getClass().getCanonicalName();
	}

	public int getVersion() {
		return 1;
	}

	public DataType getReturnDataType() {
		return DataTypes.StringType;
	}

}
