package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class exercise1 {
    public static void main(String[] args) throws Exception {

    	final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataSet<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> csv;

        // read the text file from given input path
        csv = env.readCsvFile(params.get("input")).types(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class);


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple4<Integer, Integer, Integer, Integer>> filterStream = csv
                .map(new MapFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple4<Integer, Integer, Integer, Integer>>() {
                    public Tuple4<Integer, Integer, Integer, Integer> map(
							Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
							throws Exception {
						Tuple4<Integer, Integer, Integer, Integer> out = new Tuple4<Integer, Integer, Integer, Integer>(in.f0,
                        		in.f1, in.f3, in.f5);
                        return out;
					}
                })
                .filter(new FilterFunction<Tuple4<Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple4<Integer, Integer, Integer, Integer> in) throws Exception {
                        return in.f3.equals("4");
                    }
                });



        // emit result
        if (params.has("output")) {
            filterStream.writeAsCsv(params.get("output"));
        }

        // execute program
        env.execute("Filter");

    }
}