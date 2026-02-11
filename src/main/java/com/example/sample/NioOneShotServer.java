package com.example.sample;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class NioOneShotServer implements Runnable {
  private final int port;

  public NioOneShotServer(int port) {
    this.port = port;
  }

  @Override
  public void run() {
    try (ServerSocketChannel serverSocket = ServerSocketChannel.open()) {
      serverSocket.bind(new InetSocketAddress("localhost", port));
      System.out.println("[Server] Listening on port " + port);

      // Block until a client connects
      try (SocketChannel client = serverSocket.accept()) {
        System.out.println("[Server] Client connected. Sending data...");

        String message = "Hello, world.";
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());

        while (buffer.hasRemaining()) {
          client.write(buffer);
        }
        System.out.println("[Server] Data sent. Closing.");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}