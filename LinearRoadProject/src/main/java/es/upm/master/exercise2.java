package es.upm.master;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class exercise2 {
    public static void main(String[] args) throws Exception {

    	final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data reading the text file from given input path
        String path = params.get("input");
        final Integer time = Integer.parseInt(params.get("time"));
        final Integer speed = Integer.parseInt(params.get("speed"));
        final Integer startSegment = Integer.parseInt(params.get("startSegment"));
        final Integer endSegment = Integer.parseInt(params.get("endSegment"));
        DataStreamSource<String> source = env.readTextFile(path);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer, String>> sumTumblingEventTimeWindows = source.
                map(new MapFunction<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6(Integer.parseInt(fieldArray[0]),
                        		Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        return ((startSegment<=in.f5) && (endSegment>=in.f5) && in.f4.equals(0) && in.f2>speed);
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(time))).apply(new Exercise2());

        // emit result
        if (params.has("output")) {
        	sumTumblingEventTimeWindows.writeAsCsv(params.get("output"));
        }

        // execute program
        env.execute("exercise2");
        
    }

    
    public static class Exercise2 implements AllWindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, String>, TimeWindow> {
        public void apply(TimeWindow timeWindow, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Integer, Integer, Integer, String>> out) throws Exception {
            Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer ts = 0;
            Integer xway = 0;
            Integer cars = 0;
            String vids = "";
            if(first!=null){
                xway = first.f3;
                ts = first.f0;
                cars = 1;
                vids = "["+first.f1.toString();
            }
            while(iterator.hasNext()){
            	Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
            	cars += 1;
            	vids = vids.concat("-"+next.f1);
            }
            if (!iterator.hasNext()) {
            	vids = vids.concat("]");
            }
            out.collect(new Tuple4<Integer, Integer, Integer, String>(ts, xway, cars, vids));
        }
        
    }
  
}