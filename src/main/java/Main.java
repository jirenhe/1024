import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author jirenhe | jirenhe@shulie.io
 * @since 2021/10/22 11:28 上午
 */
public class Main {

    private static final String prefix = "/Users/jirenhe";
    //private static final String prefix = "";

    private static final String path = prefix + "/data/input.data";

    private static final String out_path = prefix + "/data/result/output.data";

    private static final int threadNum = Runtime.getRuntime().availableProcessors() + 2;

    private static final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadNum);

    private static final byte lineBytes = 10;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long st = startTime;
        long totalLength = new RandomAccessFile(path, "r").length();
        long perSize = totalLength / threadNum;
        List<Future<PieceResult>> futureList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            long start = i * perSize;
            long end = i == threadNum - 1 ? totalLength : start + perSize;
            futureList.add(threadPoolExecutor.submit(new FileReaderRunner(start, end, i)));
        }
        List<PieceResult> all = new ArrayList<>();
        for (Future<PieceResult> listFuture : futureList) {
            all.add(listFuture.get());
        }
        System.out.println("read file take time :" + (System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();
        writeResult(all);
        System.out.println("write file take time :" + (System.currentTimeMillis() - startTime));
        threadPoolExecutor.shutdown();
        System.out.println("total time :" + (System.currentTimeMillis() - st));
    }

    private static void writeResult(List<PieceResult> result)
        throws IOException, ExecutionException, InterruptedException {
        File file = new File(out_path);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        long nextStart = 0;
        List<Future<?>> futureList = new ArrayList<>();
        for (PieceResult pieceResult : result) {
            System.out.println("write thread start : " + nextStart + " end " + (nextStart + pieceResult.totalBytes));
            futureList.add(threadPoolExecutor.submit(
                new FileWriteRunner(nextStart, nextStart + pieceResult.totalBytes, pieceResult.result)));
            nextStart += pieceResult.totalBytes;
        }
        for (Future<?> future : futureList) {
            future.get();
        }
    }

    private static class FileWriteRunner implements Callable<Void> {

        private final long start;

        private final long end;

        private final List<Integer> writeList;

        private FileWriteRunner(long start, long end, List<Integer> result) {
            this.start = start;
            this.end = end;
            this.writeList = result;
        }

        @Override
        public Void call() throws Exception {
            try (RandomAccessFile file = new RandomAccessFile(out_path, "rw");
                 FileChannel channel = file.getChannel()) {
                //MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, start, this.end - start);
                ByteBuffer buffer = ByteBuffer.allocate((int)(this.end - start));
                channel.position(start);
                for (Integer s : writeList) {
                    buffer.put(String.valueOf(s).getBytes(StandardCharsets.UTF_8));
                    buffer.put(lineBytes);
                    //file.write(String.valueOf(s).getBytes(StandardCharsets.UTF_8));
                    //file.write(lineBytes);
                }
                buffer.flip();
                channel.write(buffer);
                channel.force(false);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            return null;
        }
    }

    private static class PieceResult {
        private final long totalBytes;
        private final List<Integer> result;

        private PieceResult(long totalBytes, List<Integer> result) {
            this.totalBytes = totalBytes;
            this.result = result;
        }
    }

    private static class FileReaderRunner implements Callable<PieceResult> {

        private final long start;

        private final long end;

        private final int no;

        private long totalBytes = 0;

        public FileReaderRunner(long start, long end, int no) {
            this.start = start;
            this.end = end;
            this.no = no;
        }

        @Override
        public PieceResult call() {
            List<Integer> result = new ArrayList<>();
            try (RandomAccessFile file = new RandomAccessFile(path, "r");
                 FileChannel channel = file.getChannel()) {
                long acStart = start;
                long acEnd = end;
                file.seek(start);
                if (this.start != 0) {
                    acStart = nextLineIdx(file);
                }
                if (this.end != file.length()) {
                    file.seek(end);
                    acEnd = nextLineIdx(file);
                }
                file.seek(acStart);
                System.out.printf("thread : %s acstart : %s acend : %s%n", no, acStart, acEnd);
                MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, acStart, acEnd - acStart);
                buffer.load();
                char[] chars = new char[64];
                int length = 0;
                while (buffer.hasRemaining()) {
                    int b = buffer.get();
                    switch (b) {
                        case -1:
                        case '\n':
                            dealLine(result, chars, length);
                            length = 0;
                            break;
                        case '\r':
                            int cur = buffer.position();
                            if (buffer.hasRemaining() && (buffer.get()) != '\n') {
                                buffer.position(cur);
                            } else {
                                dealLine(result, chars, length);
                                length = 0;
                            }
                            break;
                        default:
                            chars[length++] = (char)b;
                            break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("thread " + no + " has error", e);
            }
            System.out.printf("thread %s result size %s totalBytes %s \n", no, result.size(), totalBytes);
            return new PieceResult(totalBytes, result);
        }

        private void dealLine(List<Integer> result, char[] chars, int length) {
            int number = LineMatcher.match(chars, length);
            if (number != -1 && PrimeCheck.isPrime(number)) {
                result.add(number);
                totalBytes += length + 1;
            }
        }

        private long nextLineIdx(RandomAccessFile file) throws IOException {
            boolean eol = false;
            while (!eol) {
                switch (file.read()) {
                    case -1:
                    case '\n':
                        eol = true;
                        break;
                    case '\r':
                        eol = true;
                        long cur = file.getFilePointer();
                        if ((file.read()) != '\n') {
                            file.seek(cur);
                        }
                        break;
                }
            }
            return file.getFilePointer();
        }

    }
}
