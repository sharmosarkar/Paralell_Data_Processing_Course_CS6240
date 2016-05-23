package main;

import java.net.*;
import java.io.IOException;

public class Barrier {
    static final int BASE_PORT = 3500;

    static Object lock;
    static int barrier_count;

    static int node_id;
    static int nodes;

    public static void main(String[] args) throws Exception {
        node_id = Integer.parseInt(args[0]);
        nodes   = Integer.parseInt(args[1]);

        lock = new Object();
        barrier_count = 0;

        ListenThread lt = new ListenThread(BASE_PORT + node_id);
        lt.start();

        System.out.println("Waiting for other processes to start... " + node_id);
        Thread.sleep(5000);

        System.out.println("Barrier... " + node_id);
        barrier();

        System.out.println("Done " + node_id);

        System.exit(0);
   }

   static void barrier() {
       inc(); // This node is in the barrier.

       for (int ii = 0; ii < nodes; ++ii) {
           int port = BASE_PORT + ii;

           try {
               Socket ss = new Socket("localhost", port);
               ss.close();
           }
           catch (IOException ee) {
               System.err.println("Error opening socket, giving up.");
               throw new Error("argh!");
           }
       } 

       synchronized(lock) {
           while (barrier_count < nodes) {
               try {
                   lock.wait();
               }
               catch (InterruptedException _ee) {
                   // Do nothing.
               }
           }
       }
   }

   static void inc() {
       synchronized(lock) {
           barrier_count += 1;
           lock.notifyAll();
       }
   }
}

class ListenThread extends Thread {
    final int port;

    public ListenThread(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            loop();
        }
        catch (Exception ee) {
            System.err.println("Error in ListenThread: " + ee.toString());
        }
    }

    void loop() throws Exception {
        ServerSocket svr = new ServerSocket(port);

        Socket conn;
        while (null != (conn = svr.accept())) {
            ConnThread tt = new ConnThread(conn);
            tt.start();
        }
    }
}

class ConnThread extends Thread {
    final Socket conn;

    public ConnThread(Socket conn) {
        this.conn = conn;
    }

    @Override 
    public void run() {
        try {
            handleConn();
        }
        catch (Exception ee) {
            System.err.println("IO Error: " + ee.toString());
        }
    }

    void handleConn() throws Exception {
        Barrier.inc();
        conn.close();
    }
}


