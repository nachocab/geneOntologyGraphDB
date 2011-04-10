/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 09/04/11
 * Time: 19:59
 * To change this template use File | Settings | File Templates.
 */

//http://arin.me/blog/indexing-nodes-in-neo4j
// i think it's the old api
import org.neo4j.api.core.*;
import org.neo4j.util.index.SingleValueIndex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hack example that reveals a "More than one relationship" issue
 *
 * User: Arin Sarkissian
 * Date: Dec 10, 2009
 * Time: 9:40:27 AM
 */
public class SVINotFound2 {
    private static final String USERNAME_INDEX = "usernameIndex";
    private static final int N_THREADS = 5;
    private static final int NUM_LINES = 300000;

    public static void main(String[] args) {
        NeoService neo = new EmbeddedNeo("test-" + System.currentTimeMillis());
        Transaction tx = neo.beginTx();
        Node indexNode = null;

        // use node #1 as the index node & create it if it doesn't exist
        // is there a better way to do this?
        try {
            indexNode = neo.getNodeById(1);
        } catch (NotFoundException nfe) {
            indexNode = neo.createNode();
        } finally {
            tx.success();
            tx.finish();
        }

        // now create the node we want indexed:
        Transaction txUser = neo.beginTx();
        Node userNode = neo.createNode();
        userNode.setProperty("username", "phatduckk");
        txUser.success();
        txUser.finish();

        // now create the index. do i need a transaction about this?
        // setup a pool
        final SingleValueIndex svi = new SingleValueIndex(USERNAME_INDEX, indexNode, neo);
        final ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);

        // now here's some fake data that we'll index in threads
        for (int i = 0; i < NUM_LINES; i++) {
            System.out.println("Processing line: " + i);
            IndexRunner command = new IndexRunner(userNode, neo, svi);
            executorService.execute(command);
        }
    }

    static class IndexRunner implements Runnable {
        NeoService neo;
        SingleValueIndex svi;
        Node userNode;

        IndexRunner(Node userNode, NeoService neo, SingleValueIndex svi) {
            this.userNode = userNode;
            this.neo = neo;
            this.svi = svi;
        }

        public void run() {
            Iterable<Node> foundInIndex = svi.getNodesFor("phatduckk");
            if (foundInIndex.iterator().hasNext()) {
                foundInIndex.iterator().next();
                if (foundInIndex.iterator().hasNext()) {
                    // why are there multiple values in a single value index?

                    // this next line will get mad
                    svi.getSingleNodeFor("phatduckk");
                    System.exit(-1);
                }
            } else {
                Transaction nodetx = neo.beginTx();
                svi.index(userNode, "phatduckk");
                nodetx.success();
                nodetx.finish();
            }
        }
    }
}
