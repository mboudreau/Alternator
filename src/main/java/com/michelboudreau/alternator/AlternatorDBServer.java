package com.michelboudreau.alternator;

public class AlternatorDBServer
{
    public static void main(final String[] args) throws Exception
    {
        int port = 9090;

        // TODO: Parse args for alternate port.
        AlternatorDB db = new AlternatorDB(port);
        db.start();

        System.out.println(
                String.format("AlternatorDB has started and is listening on port %d.", port));
    }
}
