import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.*;

import java.util.*;

import com.ximpleware.*;

/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 04/04/11
 * Time: 14:20
 * To change this template use File | Settings | File Templates.
 */


public class GeneOntologyGraphDB {

    private static final String ONTOLOGY_FILE = "var/go_daily-termdb.rdf-xml";
    //    private static final String ONTOLOGY_FILE = "var/small.termdb.rdf-xml";
    private static final String DB_PATH = "var/graphdb/";

    private static final String RDF_NS_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private static final String GO_NS_URI = "http://www.geneontology.org/dtds/go.dtd#";

    private static final String KEY_ACCESSION = "accession";
    private static final String KEY_NAME = "name";
    private static final String KEY_DEFINITION = "definition";
    private static final String[] NODE_PROPERTIES = {KEY_ACCESSION, KEY_NAME, KEY_DEFINITION};

    private static final String KEY_IS_A = "is_a";
    private static final String KEY_PART_OF = "part_of";
    private static final String KEY_REGULATES = "regulates";
    private static final String KEY_POSITIVELY_REGULATES = "positively_regulates";
    private static final String KEY_NEGATIVELY_REGULATES = "negatively_regulates";
    private static final String[] RELATIONSHIP_NAMES = {KEY_IS_A, KEY_PART_OF, KEY_REGULATES, KEY_POSITIVELY_REGULATES, KEY_NEGATIVELY_REGULATES};


    private static final String KEY_TERM_PATH = "/go:go/rdf:RDF/go:term";
    private static final String KEY_NAMESPACE = "go:";

    public static void parseOntologyNodes(String oboXMLFile, BatchInserter batchInserter, BatchInserterIndex accessionsIndex) {
        VTDGen vg = new VTDGen();

        if (vg.parseFile(oboXMLFile, true)) {
            try {
                VTDNav vn = vg.getNav();
                AutoPilot ap1 = new AutoPilot();
                AutoPilot ap2 = new AutoPilot();

                ap1.declareXPathNameSpace("rdf", RDF_NS_URI);
                ap1.declareXPathNameSpace("go", GO_NS_URI);

                ap1.selectXPath(KEY_TERM_PATH);

                ap1.bind(vn);
                ap2.bind(vn);

                while ((ap1.evalXPath()) != -1) {
                    HashMap properties = new HashMap();
                    for (String attributeName : NODE_PROPERTIES) {
                        ap2.selectXPath(KEY_NAMESPACE + attributeName);
                        properties.put(attributeName, ap2.evalXPathToString());
                    }
                    long node = batchInserter.createNode(properties);
                    accessionsIndex.add(node, properties);
                }
//                ap1.resetXPath();

            } catch (NavException e) {
                System.out.println("Exception during navigation " + e);
            } catch (XPathEvalException e) {
                e.printStackTrace();
            } catch (XPathParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static Node findOrCreateOntologyNode(HashMap<String, String> nodeAttributes, GraphDatabaseService graphDb) {
        Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);
        Node ontologyNode = accessionsIndex.get(KEY_ACCESSION, nodeAttributes.get(KEY_ACCESSION)).getSingle();

        if (ontologyNode == null) {
            ontologyNode = graphDb.createNode();

            for (String key : nodeAttributes.keySet()) {
                ontologyNode.setProperty(key, nodeAttributes.get(key));
            }
        }
        return ontologyNode;
    }

    public static void parseOntologyRelationships(String oboXMLFile, BatchInserter batchInserter, BatchInserterIndex accessionsIndex) {
        VTDGen vg = new VTDGen();

        if (vg.parseFile(oboXMLFile, true)) {
            try {
                VTDNav vn = vg.getNav();
                AutoPilot ap1 = new AutoPilot();
                AutoPilot ap2 = new AutoPilot();

                ap1.declareXPathNameSpace("rdf", RDF_NS_URI);
                ap1.declareXPathNameSpace("go", GO_NS_URI);

                ap1.selectXPath(KEY_TERM_PATH);

                ap1.bind(vn);
                ap2.bind(vn);

                while ((ap1.evalXPath()) != -1) {
                    ap2.selectXPath(KEY_NAMESPACE + KEY_ACCESSION);
                    String siblingAccession = ap2.evalXPathToString();
                    long sibling = accessionsIndex.get(KEY_ACCESSION, siblingAccession).getSingle();

                    for (String relationshipName : RELATIONSHIP_NAMES) {
                        ap2.selectXPath(KEY_NAMESPACE + relationshipName + "/@rdf:resource");

                        String ancestorAccession = ap2.evalXPathToString();

                        if (ancestorAccession != "") {
                            ancestorAccession = ancestorAccession.split("#")[1];
                            try {
                                long ancestor = accessionsIndex.get(KEY_ACCESSION, ancestorAccession).getSingle();
                                batchInserter.createRelationship(sibling, ancestor, OntologyRelTypes.valueOf(relationshipName.toUpperCase()), null);
                            } catch (NullPointerException e) {
                                System.out.println(siblingAccession + " " + relationshipName + " " + ancestorAccession + " which doesn't have a node");
                            }
                        }
                    }
                }
//                ap1.resetXPath();

            } catch (NavException e) {
                System.out.println("Exception during navigation " + e);
            } catch (XPathEvalException e) {
                e.printStackTrace();
            } catch (XPathParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createDefaultRelationships(GraphDatabaseService graphDb) {
        Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);

        Node defaultNode = graphDb.getReferenceNode();

        Node all = accessionsIndex.get(KEY_ACCESSION, "all").getSingle();

        Transaction tx = graphDb.beginTx();
        try {
            all.createRelationshipTo(defaultNode, OntologyRelTypes.IS_A);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public static void main(final String[] args) throws Exception {

        BatchInserter batchInserter = new BatchInserterImpl(DB_PATH, BatchInserterImpl.loadProperties("var/neo4j_parser_config.props"));
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(batchInserter);

        BatchInserterIndex accessionsIndex = indexProvider.nodeIndex(KEY_ACCESSION, MapUtil.stringMap("type", "exact"));
        accessionsIndex.setCacheCapacity(KEY_ACCESSION, 100000);

        parseOntologyNodes(ONTOLOGY_FILE, batchInserter, accessionsIndex);
        parseOntologyRelationships(ONTOLOGY_FILE, batchInserter, accessionsIndex);

        indexProvider.shutdown();
        batchInserter.shutdown();

        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(DB_PATH, EmbeddedGraphDatabase.loadConfigurations("var/neo4j_config.props"));
        createDefaultRelationships(graphDb);

        graphDb.shutdown();
    }
}
