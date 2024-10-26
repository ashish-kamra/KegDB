package com.kegdb;

import com.kegdb.resp.RESPObject;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private final ExecutorService executor;
    private final Keg keg;
    private volatile boolean isRunning;

    public Server() {
        this.executor = new ThreadPoolExecutor(
                Config.getThreadPoolSize(),
                Config.getThreadPoolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        this.keg = new Keg();
        this.isRunning = true;
    }

    public void start() {
        // Shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        try (ServerSocket serverSocket = new ServerSocket(Config.getPort())) {
            log.info("Server started. Listening on port {}", serverSocket.getLocalPort());

            while (isRunning && !serverSocket.isClosed()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    log.info("Accepted connection from {}", clientSocket.getInetAddress().getHostAddress());
                    executor.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    if (isRunning) {
                        log.error("Error accepting client connection: {}", e.getMessage());
                    } else {
                        log.info("Server socket closed. Stopping accept loop.");
                    }
                }
            }
        } catch (IOException e) {
            log.error("Server encountered an exception: {}", e.getMessage(), e);
        } finally {
            shutdown();
        }

        log.info("Server has been shut down.");
    }

    private void handleClient(Socket clientSocket) {
        RESPParser respParser = new RESPParser();
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                OutputStream out = clientSocket.getOutputStream()
        ) {
            while (isRunning && !clientSocket.isClosed()) {
                RESPObject respObject = respParser.deserialize(in);
                if (respObject == null) {
                    log.debug("Client {} disconnected.", clientSocket.getInetAddress().getHostAddress());
                    break;
                }
                RESPObject response = keg.storeOrRetrieve(respObject);
                String serializedResponse = respParser.serialize(response);
                out.write(serializedResponse.getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException e) {
            log.error("Error handling client {}: {}", clientSocket.getInetAddress().getHostAddress(), e.getMessage());
        } finally {
            try {
                clientSocket.close();
                log.info("Closed connection with {}", clientSocket.getInetAddress().getHostAddress());
            } catch (IOException e) {
                log.error("Error closing client socket: {}", e.getMessage());
            }
        }
    }

    private void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        log.info("Shutting down server...");

        try {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in the specified time. Forcing shutdown.");
                executor.shutdownNow();
            }
            log.info("Executor service shut down successfully.");
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted: {}", e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
