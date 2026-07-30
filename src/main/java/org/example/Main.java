package org.example;

import Controller.Executor;
import Service.FlinkJob;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: Main producer <cartella-csv> | consumer");
            System.exit(1);
        }

        switch (args[0]) {
            case "producer":
                if (args.length < 2) {
                    System.err.println("Uso: Main producer <cartella-csv>");
                    System.exit(1);
                }
                new Executor().execute(java.nio.file.Paths.get(args[1]));
                break;

            case "consumer":
                new FlinkJob().start();
                break;

            default:
                System.err.println("Comando sconosciuto: " + args[0]);
                System.err.println("Uso: Main producer <cartella-csv> | consumer");
                System.exit(1);
        }
    }
}
