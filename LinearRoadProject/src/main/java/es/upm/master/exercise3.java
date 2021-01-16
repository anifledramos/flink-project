package es.upm.master;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

public class exercise3 {
    public static void main(String[] args) throws Exception {
    	final ParameterTool params = ParameterTool.fromArgs(args);
    	
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // get the number of segment
        final int  segment=params.getInt("segment");
        // get input data reading the text file from given input path
        String path = params.get("input");
        DataStreamSource<String> source = env.readTextFile(path);
        
        
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer,Integer>> filterStream = source.
                map(new MapFunction<String, Tuple5<Integer, Integer, Integer,Integer,Integer>>() {
                    public  Tuple5<Integer, Integer, Integer,Integer,Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple5<Integer, Integer, Integer,Integer,Integer> out = new Tuple5(Integer.parseInt(fieldArray[0]),
                        		Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[2]),Integer.parseInt(fieldArray[6]));
                        return out;
                    }
                })
                .filter(new FilterFunction< Tuple5<Integer, Integer, Integer,Integer,Integer>>() {
                    public boolean filter( Tuple5<Integer, Integer, Integer,Integer,Integer> in) throws Exception {
                        return in.f4.equals(segment);
                    }
                })
                .map(new  MapFunction<Tuple5<Integer, Integer, Integer,Integer,Integer>, Tuple4<Integer, Integer, Integer,Integer>>(){
                    public Tuple4<Integer, Integer, Integer,Integer> map(Tuple5<Integer, Integer, Integer,Integer,Integer> in) throws Exception{
                    	Tuple4<Integer, Integer, Integer,Integer> out = new Tuple4(in.f0,in.f1,in.f2,in.f3);
                        return out;
                    }
                });
        
        KeyedStream<Tuple4<Integer, Integer, Integer,Integer>, Tuple> keyedStream = filterStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple4<Integer, Integer, Integer,Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple4<Integer, Integer, Integer,Integer> element) {
                                return element.f0*1000;
                            }
                        }

                ).keyBy(1);
        
        SingleOutputStreamOperator<Tuple4<Integer,Integer, Integer, Integer>> intTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new AvgCar());
        
        // Include watermark to reduce the time ( 6737->6731)
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> highAvgTumblingEventTimeWindows =
        		intTumblingEventTimeWindows.assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple4<Integer,Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple4<Integer,Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }

                ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new HighAvgCar());
        
        SingleOutputStreamOperator<Tuple3<Integer,Integer, Integer>> avgTumblingEventTimeWindows = 
        		intTumblingEventTimeWindows.map(new  MapFunction< Tuple4<Integer, Integer, Integer,Integer>, Tuple3<Integer,Integer, Integer>>(){
                    public Tuple3<Integer,Integer, Integer> map(Tuple4<Integer, Integer, Integer,Integer> in) throws Exception{
                    	Tuple3<Integer,Integer, Integer> out = new Tuple3(in.f1,in.f2,in.f3);
                        return out;
                    }
                });
     // emit result
        if (params.has("output1")) {
        	avgTumblingEventTimeWindows.writeAsCsv(params.get("output1"));
        }
        
     // emit result
        if (params.has("output2")) {
        	highAvgTumblingEventTimeWindows.writeAsCsv(params.get("output2"));
        }

        // execute program
        env.execute("Exercise3");
    }
    
    public static class AvgCar implements WindowFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer,Integer, Integer, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Integer,Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple4<Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple4<Integer, Integer, Integer, Integer> first = iterator.next();
            Integer VID = 0;
            Integer xway = 0;
            Integer avgSpeed = 0;
            Integer time=0;
            Integer temp = 1;
            if(first!=null){
            	xway = first.f2;
                VID = first.f1;
                avgSpeed =first.f3;
                time=first.f0;
            }
            while(iterator.hasNext()){
            	Tuple4<Integer, Integer, Integer, Integer> next = iterator.next();
            	temp += 1;
                avgSpeed +=next.f3;
            }
            avgSpeed=avgSpeed/temp;
            
            out.collect(new Tuple4<Integer,Integer, Integer, Integer>(time,VID, xway, avgSpeed));
        }
    }
    public static class HighAvgCar implements  AllWindowFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple3< Integer, Integer, Integer>, TimeWindow> {
        public void apply(TimeWindow timeWindow, Iterable<Tuple4<Integer, Integer, Integer, Integer>> input, Collector<Tuple3< Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple4<Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple4<Integer, Integer, Integer, Integer> first = iterator.next();
            Integer VID = 0;
            Integer xway = 0;
            Integer avgSpeed = 0;
            Integer auxiliar=0;
            if(first!=null){
                avgSpeed =first.f3;
                VID =first.f1;
                xway = first.f2;
            }
            while(iterator.hasNext()){
            	Tuple4<Integer, Integer, Integer, Integer> next = iterator.next();
            	auxiliar=next.f3;
            	if(auxiliar>avgSpeed) {
            		avgSpeed=auxiliar;
            		xway = next.f2;
                    VID = next.f1;
            	}
                 
            }
            
            out.collect(new Tuple3<Integer, Integer, Integer>(VID, xway, avgSpeed));
        }
    }
}