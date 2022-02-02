package MapReduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Task carried by workers during split-phase in which
 * every file is divided into equal parts according to the
 * specified chunksize
 */
public class SplitTask extends Task {
    public LinkedBlockingQueue<MapArgs> mapArgs;
    public String filename;
    public int chunksize;


    /**
     * Divides the files and saves the data(filename,offset,chunksize)
     * into the LinkedBlockingQueue
     */
    @Override
    void work() {
        long filesize = 0;
        try {
            filesize= Files.size(Path.of(filename));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int parts = (int) (filesize / chunksize);
        for(var i = 0; i < parts ; i++){
            mapArgs.add(new MapArgs(filename,i*chunksize,chunksize));
        }
        int lastbit = (int) (filesize % chunksize);
        mapArgs.add(new MapArgs(filename,parts*chunksize,lastbit));

    }
}
