package Utilities;

import MapReduce.Collectors.KeyValueCollector;
import MapReduce.Mapper;

import java.util.Scanner;

public class TestMapper implements Mapper<String,Integer> {

    @Override
    public void map(Scanner text, String filename, KeyValueCollector<String, Integer> collector) {
        while (text.hasNext()){
            String word = text.next();
            collector.collect(word,1);
        }
    }
}
