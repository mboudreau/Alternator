package com.michelboudreau.alternator;

import java.io.File;

public class AlternatorDBServer {

    /**
     * Allow the database to be persisted to disk when the server shuts down.
     */
    private static final boolean sandboxStatusSaveToDisk = false;
    
    /**
     * Discard the database contents when the server shuts down.
     */
    private static final boolean sandboxStatusDiscardData = true;
    
    private static final int defaultPort = 9090;

    public static void main(final String[] args) throws Exception {

        File persistenceFile = null;
        
        int port = defaultPort;

        if (args.length >= 1) {
            String persistencePath = args[0];
            persistenceFile = new File(persistencePath);
        }
        
        AlternatorDB db = new AlternatorDB(port, persistenceFile, sandboxStatusSaveToDisk);
        db.start();

        if (persistenceFile != null) {
            System.out.println(
                    String.format("+++++ AlternatorDB loaded data from file: %s", persistenceFile.getCanonicalPath()));
        }
        
        System.out.println(
                String.format("+++++ AlternatorDB has started and is listening on port %d.", port));

        if (persistenceFile != null) {
            System.out.println(
                    String.format("Press the Enter key to exit gracefully and save data to file: %s", persistenceFile.getCanonicalPath()));
        }
        System.out.print("Use Control-C to force exit (without saving data changes): ");
        String input = System.console().readLine();

        System.out.println("----- AlternatorDB is shutting down...");

        db.stop();
        
        if (persistenceFile != null) {
            System.out.println(
                    String.format("----- Data was saved to file: %s", persistenceFile.getCanonicalPath()));
        }
    }
}
