package main;

import java.io.IOException;
import textsock.TextSocket;

public class WebServer {
    public static void main(String[] args) throws IOException {
        final int s = Integer.parseInt(args[0]);
        TextSocket.Server[] svr;
        svr = new TextSocket.Server[s];
        for(int i=2;i<=s+1;i++){
            System.out.println("Server Processes starting up");
            svr [i-2] = new TextSocket.Server(10000 + i);
        }
        
        TextSocket conn;
        while (null != (conn = svr[0].accept())) {
            String req = conn.getln();
            System.out.println(req);
            while (!conn.getln().equals("")) {
                // skip it.
            }
            System.out.println(req);

            String[] parts = req.split(" ");
            System.out.println(parts[0]);
            if(parts.length ==3){
                if (!parts[0].equals("GET")) {
                throw new IOException("Expected GET");
            }
            if (!parts[2].equals("HTTP/1.1")) {
                throw new IOException("Expected HTTP/1.1");
            }

            String path = parts[1];
            conn.putln("HTTP/1.1 200 OK");
            conn.putln("Content-type: text/plain");
            conn.putln("");
            conn.putln("Hello, path = " + path);

            }
            if(parts.length ==4){
                int no = Integer.parseInt(parts[3]);
                conn.putln(no+"");
                boolean flag = false;
                if(no%1==0 && no==2){
                    conn.putln("No"+ no +"prime" + "X :"+2);
                }
                else if(no%2==0){
                    conn.putln("No "+ no +"not prime." + "X :"+2);

                }
                else
		{
                    int j = 0;
                    for(j=1;j<=s-1;j++){
                        conn = svr[j].accept();
                        conn.putln(no+"");
                        if(no%(j+2)==0)
                        {
                            System.out.println((j+2)+"");
                            conn.putln("Prime" + (j+2));
                            flag = true;
                            break;
                        }
                    }
                    String text;
                    if(flag==true){

                        text = no+"prime  and X is :" +(j+2);

                    }
                    else{
                        text = no+ " not prime";
                    }
                    System.out.println(text);
                    for(int k=2;k>=0;k--){
                        conn=svr[k].accept();
                        conn.getln();
                        conn.putln(text);
                        if(k==0)
                            conn.putln(text);

                    }

                    }

                }
                

            }

            conn.close();
    }
}
