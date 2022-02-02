package Utilities;

import MapReduce.ReduceResults;
import MapReduce.Reducer;

import java.util.Collection;

public class TestReducer implements Reducer<String,Integer,Integer> {
    @Override
    public void reduce(String s, Collection<Integer> values, Collection<ReduceResults<String, Integer>> results) {
        int sum = 0;
        for(var v : values){
            sum += v;
        }
        results.add(new ReduceResults<>(s,sum));
    }
}
