package org.example;

import Controller.Executor;
import Service.BenchmarkRunner;
import Service.FlinkJob;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: Main producer <cartella-csv> | consumer | benchmark | benchmark-full <cartella-csv>");
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

            case "benchmark":
                int min = 2;
                int max = Runtime.getRuntime().availableProcessors();
                int step = 2;
                if (args.length >= 2) min  = Integer.parseInt(args[1]);
                if (args.length >= 3) max  = Integer.parseInt(args[2]);
                if (args.length >= 4) step = Integer.parseInt(args[3]);

                System.out.printf("[benchmark] sweep parallelism %d -> %d (step %d), vCPU disponibili: %d%n",
                        min, max, step, Runtime.getRuntime().availableProcessors());

                for (int p = min; p <= max; p += step) {
                    System.out.printf("%n========== RUN parallelism = %d ==========%n", p);
                    try {
                        new FlinkJob(p).start();
                    } catch (Exception e) {
                        Throwable cause = e;
                        while (cause.getCause() != null) cause = cause.getCause();
                        System.err.printf("[benchmark] ERRORE parallelism=%d: %s%n", p, cause.getMessage());
                    }
                }
                System.out.println("\n[benchmark] completato -> src/main/java/results/benchmark.csv");
                break;

            case "benchmark-full":
                if (args.length < 2) {
                    System.err.println("Uso: Main benchmark-full <cartella-csv>");
                    System.exit(1);
                }
                Path dataDir = java.nio.file.Paths.get(args[1]);

                int[] partitions   = {2, 4, 8, 16};
                int[] parallelisms = {2, 4, 6, 8, 10, 12};

                new BenchmarkRunner(dataDir, partitions, parallelisms).run();
                break;

            default:
                System.err.println("Comando sconosciuto: " + args[0]);
                System.err.println("Uso: Main producer <cartella-csv> | consumer | benchmark | benchmark-full <cartella-csv>");
                System.exit(1);
        }
    }
}
