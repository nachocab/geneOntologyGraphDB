/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 09/04/11
 * Time: 20:00
 * To change this template use File | Settings | File Templates.
 */
//http://arin.me/blog/indexing-nodes-in-neo4j
// i think it's the old api

import org.neo4j.api.core.*;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.LuceneIndexService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LuceneIndex {
    private static final String USERNAME_INDEX = "usernameIndex";
    private static final int NUM_THREADS = 10;
    private static final int NUM_LINES = 1000000;
    private static final String USERNAME = "phatduckk";

    public static void main(String[] args) {
        // always use a new store
        NeoService neo = new EmbeddedNeo("test-" + System.currentTimeMillis());

        // now create the node we want indexed:
        Transaction txUser = neo.beginTx();
        Node userNode = neo.createNode();
        userNode.setProperty(USERNAME_INDEX, USERNAME);
        txUser.success();
        txUser.finish();

        // now create the index & setup a pool
        IndexService idxServ = new LuceneIndexService(neo);
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        // now let's index that same node NUM_LINES times
        // the reason we're indexing the same node is cuz i'm checking for thread safety during indexing issues
        // otherwise you'd normally be indexing new nodes who's data you got from some external source
        for (int i = 0; i < NUM_LINES; i++) {
            System.out.println("line: " + i);
            IndexRunner command = new IndexRunner(userNode, neo, idxServ);
            executorService.execute(command);
        }

        // should do a clean neo.shutdown() at some point ;-)
    }

    static class IndexRunner implements Runnable {
        NeoService neo;
        IndexService idxServ;
        Node userNode;

        IndexRunner(Node userNode, NeoService neo, IndexService idxServ) {
            this.userNode = userNode;
            this.neo = neo;
            this.idxServ = idxServ;
        }

        public void run() {
            Transaction nodetx = neo.beginTx();
            Node nodeFromIndex = idxServ.getSingleNode(USERNAME_INDEX, USERNAME);

            if (nodeFromIndex != null) {
                System.out.println("found " + USERNAME + " in the " + USERNAME_INDEX
                        + " index. Node ID is: " + nodeFromIndex.getId());
            } else {
                idxServ.index(userNode, USERNAME_INDEX, USERNAME);
            }

            nodetx.success();
            nodetx.finish();
        }
    }
}