package main;

import java.io.IOException;
import textsock.TextSocket;

public class WebClient {
    public static void main(String[] args) throws IOException {
        TextSocket conn = new TextSocket("localhost", 3002);
        conn.putln("GET / HTTP/1.1");
        conn.putln("Host: localhost");
        conn.putln("");

        for (String line : conn) {
            System.out.println(line);
        }

        conn.close();
    }
}

