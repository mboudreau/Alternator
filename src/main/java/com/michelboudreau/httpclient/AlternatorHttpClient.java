/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michelboudreau.httpclient;

import java.net.*;
import java.io.*;

/**
 *
 * @author thomasbredillet
 */
public class AlternatorHttpClient {

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(8888);
        } catch (IOException e) {
            System.err.println("Could not listen on port: 8888.");
            System.exit(1);
        }

        Socket clientSocket = null;
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            System.err.println("Accept failed.");
            System.exit(1);
        }
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String inputLine, outputLine;
        AlternatorProtocol kkp = new AlternatorProtocol();

        outputLine = kkp.processInput(null);
        out.println(outputLine);
        System.out.println(in);
        System.out.println(in.toString());
        System.out.println(clientSocket.getInputStream());
        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
            outputLine = kkp.processInput(inputLine);
            out.println(outputLine);
            if (outputLine.equals("Bye.")) {
                break;
            }
        }
        out.close();
        in.close();
        clientSocket.close();
        serverSocket.close();
    }
}
