package MapReduce;

import Utilities.BoundedInputStream;
import MapReduce.Collectors.KeyValueCollector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

/**
 *
 * Task that creates initial key value pairs from a block of data
 */
public class MapTask<Key,Value> extends Task {
    public Mapper<Key,Value> mapper;
    public MapArgs filedata;
    public KeyValueCollector<Key,Value> collector;
    public String delim;

    /**
     * Strip the block of data into stream of words that are passed to the map function
     */
    @Override
    void work() {
        int offset = filedata.offset();
        int chunksize = filedata.chunksize();
        String filename = filedata.filename();
        int bytesRead = 0;
        try {
            long filesize = Files.size(Path.of(filename));
            RandomAccessFile rafile = new RandomAccessFile(filename,"r");
            long skipped = 0;
            //move the offset part of the file
            do {
                skipped += rafile.skipBytes((int) (offset - 1 - skipped));
            }while(skipped < offset - 1);
            //increment the offset in case we start int he middle of a word
            if(offset != 0){
                char ch;
                while (offset + bytesRead < filesize){
                    ch= (char) rafile.readByte();
                    if(delim.indexOf(ch) != -1)
                        break;
                    bytesRead++;
                }
            }
            offset = offset + bytesRead;
            chunksize = chunksize - bytesRead;
            //increment the chunksize in case our file chunk end in the middle of a word
            rafile.seek(offset+chunksize - 1);
            char ch;
            while (offset+chunksize < filesize){
                ch= (char) rafile.readByte();
                if(delim.indexOf(ch) != -1)
                    break;
                chunksize++;
            }
            rafile.seek(offset);

            FileInputStream fileStream = new FileInputStream(rafile.getFD());
            //use a bounded inputstream so we can pass each worker only its allocated part of the file
            BoundedInputStream stream = new BoundedInputStream(fileStream,chunksize);
            Scanner sc = new Scanner(stream);

            //use regex to delimit words and escape all regex characters so we dont have any nasty surprises
            StringBuilder REGEX = new StringBuilder("[");
            for(char delimch : delim.toCharArray()){
                REGEX.append(""+'\\' + delimch);
            }
            REGEX.append("]+");
            String regexstr = REGEX.substring(0,REGEX.length());
            sc.useDelimiter(regexstr);
            mapper.map(sc,filename,collector);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
