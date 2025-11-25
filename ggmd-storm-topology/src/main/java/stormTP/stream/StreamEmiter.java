package stormTP.stream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Classe permettant d'Ã©mettre un output en UDP/TCP.
 * Fixed version: Maintains connection state and removes infinite loop.
 */
public class StreamEmiter implements Serializable {

    private static final long serialVersionUID = 4262369370788016342L;
    private int port = -1;

    // Transient fields to manage the connection state across tuples.
    // They are transient because they cannot be serialized by Storm,
    // and must be re-initialized when the Bolt starts or reconnects.
    private transient ServerSocket server;
    private transient Socket socket;
    private transient BufferedWriter out;

    public StreamEmiter(int port) {
        this.port = port;
    }

    public void send(String row) {
        try {
            // Establish connection if it doesn't exist (e.g. first tuple or after disconnect)
            if (out == null) {
                connect();
            }

            // Write the received row to the listener
            out.write(row);
            out.newLine();
            out.flush();

        } catch (IOException e) {
            System.err.println("Error sending data: " + e.getMessage());
            e.printStackTrace();
            // If the write fails (e.g., broken pipe), close the connection
            // so we can try to reconnect cleanly on the next tuple.
            close();
        }
    }

    private void connect() throws IOException {
        System.out.println("StreamEmiter: Ensuring server socket on port " + this.port + "...");

        // Only create the server socket if it's not already active
        if (server == null || server.isClosed()) {
            server = new ServerSocket(this.port);
        }

        System.out.println("StreamEmiter: Waiting for client connection...");
        // Blocks here until the Listener connects
        socket = server.accept();
        System.out.println("StreamEmiter: Client connected.");

        out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    private void close() {
        try {
            if (out != null) out.close();
            if (socket != null) socket.close();
            if (server != null) server.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out = null;
            socket = null;
            server = null;
        }
    }

    @Override
    public String toString() {
        return "StreamEmiter[port=" + this.port + "]";
    }
}