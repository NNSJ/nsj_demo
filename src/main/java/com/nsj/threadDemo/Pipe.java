package com.nsj.threadDemo;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;

public class Pipe {
    static class ReaderThread implements Runnable {
        private PipedReader reader;
        public ReaderThread(PipedReader reader) {
            this.reader = reader;
        }
        @Override
        public void run() {
            System.out.println("this is reader");
            int receive = 0;
            try {
                while ((receive = reader.read()) != -1) {
                    System.out.print((char)receive);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    static class WriterThread implements Runnable {
        private PipedWriter writer;
        public WriterThread(PipedWriter writer) {
            this.writer = writer;
        }
        @Override
        public void run() {
            System.out.println("this is writer");
            int receive = 0;
            try {
                writer.write("test");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void main(String[] args) throws Exception{
    PipedWriter writer = new PipedWriter();
    PipedReader reader = new PipedReader();
    writer.connect(reader); // 这⾥注意⼀定要连接，才能通信
    new Thread(new ReaderThread(reader)).start();
    Thread.sleep(1000);
    new Thread(new WriterThread(writer)).start();
}

}
