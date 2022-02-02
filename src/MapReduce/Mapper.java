package MapReduce;

import MapReduce.Collectors.KeyValueCollector;

import java.util.Scanner;

public interface Mapper<Key1,Value1>{
    /**
     *
     * @param text stream of words we process
     * @param filename name of file we are processing
     * @param collector entity that collects our key value pairs
     */
    void map(Scanner text,String filename, KeyValueCollector<Key1,Value1> collector);
}
