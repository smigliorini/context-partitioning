package it.univr.hadoop.output;

import it.univr.hadoop.mapreduce.ContextBasedReducer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class ContextBasedOutputCommitter extends FileOutputCommitter {

    private static Logger LOGGER = LogManager.getLogger(ContextBasedOutputCommitter.class);
    private Path out;

    public ContextBasedOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        out = outputPath;
    }

    public ContextBasedOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        out = outputPath;
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
        super.commitJob(context);

        // Path outputPath = this.get;getOutputPath();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        List<Path> resultFiles = Stream.of(fileSystem.listStatus(out)).map(FileStatus::getPath)
                .filter(p -> p.getName().contains(ContextBasedReducer.MASTER_FILE_NAME))
                .sorted((p1, p2) -> p1.getName().compareTo(p2.getName())).collect(Collectors.toList());

        if(resultFiles.size() == 0) {
            LOGGER.warn("Master file not found");
        } else {
            Path masterFile = new Path(out, ContextBasedReducer.MASTER_FILE_NAME);
            FSDataOutputStream outputStream = fileSystem.create(masterFile);
            final byte[] NEWLINE = new byte[] {'\n'};
            Text line = new Text();
            long count = 0;
            for(Path file : resultFiles) {
                LineReader lineReader = new LineReader(fileSystem.open(file));
                while (lineReader.readLine(line) > 0) {
                    Text output =  new Text(format("%d%s", count, ContextBasedReducer.SPLITERATOR));
                    output.append(line.getBytes(), 0, line.getLength());
                    outputStream.write(output.getBytes(), 0, output.getLength());
                    outputStream.write(NEWLINE);
                    count++;
                }
                lineReader.close();
                fileSystem.delete(file, false);
            }
            outputStream.close();
        }

    }
}
