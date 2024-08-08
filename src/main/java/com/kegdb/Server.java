package com.kegdb;

import com.kegdb.resp.RESPParser;
import com.kegdb.storage.Keg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static com.kegdb.Constants.DEFAULT_PORT;

public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(DEFAULT_PORT)) {
            log.info("Listening on port: {}", serverSocket.getLocalPort());

            try (
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    OutputStream out = clientSocket.getOutputStream()
            ) {
                //Currently supporting only one client, It'll be modified to support multiple clients
                log.info("Accepted connection from {}", clientSocket.getInetAddress().getHostAddress());
                RESPParser respParser = new RESPParser();
                Keg keg = new Keg();
                while (true) {
                    var respObject = respParser.deserialize(in);
                    var response = keg.storeOrRetrieve(respObject);
                    out.write(respParser.serialize(response).getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
            }
        } catch (IOException e) {
            log.error("Server exception: {}", e.getMessage());
            e.printStackTrace();
        }

        log.info("Server is shutting down.");
    }
}
