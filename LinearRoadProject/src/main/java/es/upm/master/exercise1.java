package es.upm.master;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class exercise1 {
    public static void main(String[] args) throws Exception {

    	final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data reading the text file from given input path
        String path = params.get("input");
        DataStreamSource<String> source = env.readTextFile(path);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer, Integer>> sumTumblingEventTimeWindows = source.
                map(new MapFunction<String, Tuple3<Integer, Integer, Integer>>() {
                    public Tuple3<Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Integer, Integer, Integer> out = new Tuple3(Integer.parseInt(fieldArray[0]),
                        		Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
                    public boolean filter(Tuple3<Integer, Integer, Integer> in) throws Exception {
                        return in.f2.equals(4);
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple3<Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }

                ).startNewChain()
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1))).apply(new SimpleSum());

        // emit result
        if (params.has("output")) {
        	sumTumblingEventTimeWindows.writeAsCsv(params.get("output"));
        }

        // execute program
        env.execute("exercise1");

    }
    
    public static class SimpleSum implements AllWindowFunction<Tuple3<Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, TimeWindow> {
        public void apply(TimeWindow timeWindow, Iterable<Tuple3<Integer, Integer, Integer>> input, Collector<Tuple4<Integer, Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple3<Integer, Integer, Integer>> iterator = input.iterator();
            Tuple3<Integer, Integer, Integer> first = iterator.next();
            Integer ts = 0;
            Integer xway = 0;
            Integer exit = 0;
            Integer temp = 1;
            if(first!=null){
                xway = first.f1;
                ts = first.f0;
                exit = first.f2;
            }
            while(iterator.hasNext()){
            	Tuple3<Integer, Integer, Integer> next = iterator.next();
                temp += 1;
            }
            out.collect(new Tuple4<Integer, Integer, Integer, Integer>(ts, xway, exit, temp));
        }
    }
}