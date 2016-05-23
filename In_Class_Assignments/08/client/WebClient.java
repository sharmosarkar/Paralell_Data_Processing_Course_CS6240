package main;

import java.io.IOException;
import textsock.TextSocket;
import java.util.Scanner;
import java.io.*;

public class WebClient {
    public static void main(String[] args) throws IOException {
        int n = 0;
        TextSocket conn = new TextSocket("localhost", 10002);
        System.out.println("Enter Number");
        Scanner sc = new Scanner(System.in);
        n = sc.nextInt();
        conn.putln("GET / HTTP/1.1 "+ n);
        conn.putln("Host: localhost");
        conn.putln("");
        System.out.println("Response");
        for (String line : conn) {
            System.out.println(line);
        }

        conn.close();
    }
}

