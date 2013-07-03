package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.lab41.dendrite.generator.kronecker.mapreduce.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 * 
 * @author kramachandran
 */
public class MatrixBlockInputFormat extends InputFormat<MatrixBlockInputSplit, NullWritable> {
    private Long startRow;
    private Long endRow;
    private Long startCol;
    private Long endCol;

    Logger log =  LoggerFactory.getLogger(MatrixBlockInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        int n = Integer.parseInt(conf.get(Constants.N));

        long dim =  (long) Math.pow(2, n);

        startRow = 1L;
        endRow = dim;

        startCol = 1L;
        endCol = dim;


         List<InputSplit> splits = new ArrayList<InputSplit>();

        long block_size = context.getConfiguration().getLong(Constants.BLOCK_SIZE, (long) Math.pow(2, 14));

        log.info("dim : " + dim);
        log.info("block_size" + block_size);

        for( long row = startRow; row <= endRow; row += block_size)
        {
            long endRowInterval = row + block_size -1;
            if(endRowInterval > endRow)
                endRowInterval = endRow;

            for(long col = startCol; col <= endCol; col += block_size)
            {


                long endColInterval = col + block_size -1;
                if(endColInterval > endCol)
                        endColInterval = endCol;


                log.info(String.format("startRow: %1$d startCol: %2$d - endRow: %3$d endCol: %4$d",row, endRowInterval, col, endColInterval));
                MatrixBlockInputSplit split = new MatrixBlockInputSplit(row, endRowInterval, col, endColInterval);
                splits.add(split);
            }
        }

        return splits;
    }


    @Override
    public RecordReader<MatrixBlockInputSplit, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        log.info("creating record reader");
        MatrixBlockInputSplit msplit = (MatrixBlockInputSplit) split;
        MatrixBlockRecordReader recordReader = new MatrixBlockRecordReader();
        log.info(String.format("startRow: %1$d startCol: %2$d - endRow: %3$d endCol: %4$d",msplit.getStartRowInterval(),
                msplit.getStartColInterval(), msplit.getEndRowInterval(), msplit.getEndColInterval()));
        recordReader.initialize(msplit, context);
        return recordReader;

    }
}
