package com.example.sample;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioServer implements Runnable {
  private final int port;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private ServerSocketChannel serverSocket; // Promoted to field for access in stop()

  public NioServer(int port) {
    this.port = port;
  }

  public void stop() {
    running.set(false);
    try {
      // Closing the socket will interrupt the blocking accept() call
      if (serverSocket != null) {
        serverSocket.close();
      }
    } catch (IOException e) {
      // Ignore errors during shutdown
    }
  }

  @Override
  public void run() {
    try {
      serverSocket = ServerSocketChannel.open();
      serverSocket.bind(new InetSocketAddress("localhost", port));
      System.out.println("[Server] Listening on port " + port);

      while (running.get()) {
        try (SocketChannel client = serverSocket.accept()) {
          System.out.println("[Server] Client connected.");
          client.configureBlocking(true);

          ByteBuffer inputBuffer = ByteBuffer.allocate(1024);

          // Loop: Wait for request, then send response
          while (running.get() && client.read(inputBuffer) != -1) {
            inputBuffer.flip();
            inputBuffer.clear();

            String message = "Hello from Server " + System.currentTimeMillis();
            byte[] bytes = message.getBytes();
            ByteBuffer outputBuffer = ByteBuffer.allocate(bytes.length);
            outputBuffer.put(bytes);
            outputBuffer.flip();

            while (outputBuffer.hasRemaining()) {
              client.write(outputBuffer);
            }
          }
          System.out.println("[Server] Client disconnected.");
        } catch (IOException e) {
          // If we stopped the server, this exception is expected (Socket Closed)
          if (running.get()) {
            System.out.println("[Server] Connection error: " + e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      if (running.get()) e.printStackTrace();
    } finally {
      System.out.println("[Server] Server thread stopped.");
    }
  }
}