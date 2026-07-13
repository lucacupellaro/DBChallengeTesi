package org.example;

import Controller.Executor;

import java.io.IOException;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Uso: Main <cartella-csv>");
            System.exit(1);
        }
        Path dataDir = java.nio.file.Paths.get(args[0]);

        Executor executor = new Executor();
        executor.execute(dataDir);
    }
}
