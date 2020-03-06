package com.upwork.hermes.traffic;

public class RunMain {
    public static void main(final String[] args) {
        picocli.CommandLine.run(new TrafficApplication(), System.out, args);
    }
}
