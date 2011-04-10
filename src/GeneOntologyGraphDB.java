import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;

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

    public static void parseOntologyNodes(String oboXMLFile, GraphDatabaseService graphDb) {
        VTDGen vg = new VTDGen();

        if (vg.parseFile(oboXMLFile, true)){
            Transaction tx = graphDb.beginTx();
            try {
                VTDNav vn = vg.getNav();
                AutoPilot ap1 = new AutoPilot();
                AutoPilot ap2 = new AutoPilot();

                ap1.declareXPathNameSpace("rdf",RDF_NS_URI);
                ap1.declareXPathNameSpace("go",GO_NS_URI);

                ap1.selectXPath(KEY_TERM_PATH);

                Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);
                Index<Node> namesIndex = graphDb.index().forNodes(KEY_NAME);

                ap1.bind(vn);
                ap2.bind(vn);

                while((ap1.evalXPath()) != -1){
                    HashMap nodeAttributesHash = new HashMap();
                    for (String attributeName : NODE_PROPERTIES){
                        ap2.selectXPath(KEY_NAMESPACE + attributeName);
                        nodeAttributesHash.put(attributeName, ap2.evalXPathToString());
                    }
                    Node node = findOrCreateOntologyNode(nodeAttributesHash, graphDb);

                    accessionsIndex.add(node, KEY_ACCESSION, node.getProperty(KEY_ACCESSION));
                    namesIndex.add(node, KEY_NAME, node.getProperty(KEY_NAME));
                }
//                ap1.resetXPath();

                tx.success();
            }catch (NavException e) {
                System.out.println("Exception during navigation " + e);
            } catch (XPathEvalException e){
                e.printStackTrace();
            } catch (XPathParseException e ){
                e.printStackTrace();

            } finally {
                tx.finish();
            }
        }
    }

    public static Node findOrCreateOntologyNode(HashMap<String, String> nodeAttributes, GraphDatabaseService graphDb) {
        Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);
        Node ontologyNode = accessionsIndex.get(KEY_ACCESSION, nodeAttributes.get(KEY_ACCESSION)).getSingle();

        if (ontologyNode == null){
            ontologyNode = graphDb.createNode();

            for (String key : nodeAttributes.keySet()){
                ontologyNode.setProperty(key, nodeAttributes.get(key));
            }
        }
        return ontologyNode;
    }

    public static void parseOntologyRelationships(String oboXMLFile, GraphDatabaseService graphDb){
        VTDGen vg = new VTDGen();

        if (vg.parseFile(oboXMLFile, true)){
            Transaction tx = graphDb.beginTx();
            try {
                VTDNav vn = vg.getNav();
                AutoPilot ap1 = new AutoPilot();
                AutoPilot ap2 = new AutoPilot();

                ap1.declareXPathNameSpace("rdf", RDF_NS_URI);
                ap1.declareXPathNameSpace("go", GO_NS_URI);

                ap1.selectXPath(KEY_TERM_PATH);

                Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);

                ap1.bind(vn);
                ap2.bind(vn);

                while((ap1.evalXPath()) != -1){
                    ap2.selectXPath(KEY_NAMESPACE + KEY_ACCESSION);
                    Node sibling = accessionsIndex.get(KEY_ACCESSION, ap2.evalXPathToString()).getSingle();

                    for (String relationshipName : RELATIONSHIP_NAMES){
                        ap2.selectXPath(KEY_NAMESPACE + relationshipName + "/@rdf:resource");

                        String ancestorAccession = ap2.evalXPathToString();

                        if (ancestorAccession != "") {
                            ancestorAccession = ancestorAccession.split("#")[1];
                            Node ancestor = accessionsIndex.get(KEY_ACCESSION, ancestorAccession).getSingle();
                            sibling.createRelationshipTo(ancestor, OntologyRelTypes.valueOf(relationshipName.toUpperCase()));
                        }
                    }
                }
//                ap1.resetXPath();

                tx.success();
            }catch (NavException e) {
                System.out.println("Exception during navigation " + e);
            } catch (XPathEvalException e){
                e.printStackTrace();
            } catch (XPathParseException e ){
                e.printStackTrace();

            } finally {
                tx.finish();
            }
        }
    }

    public static void createDefaultRelationships(GraphDatabaseService graphDb ){
        Index<Node> accessionsIndex = graphDb.index().forNodes(KEY_ACCESSION);

        Node defaultNode = graphDb.getReferenceNode();

        Node all = accessionsIndex.get(KEY_ACCESSION, "all").getSingle();

        Transaction tx = graphDb.beginTx();
        try {
            all.createRelationshipTo(defaultNode, OntologyRelTypes.IS_A);
            tx.success();
        } finally{
            tx.finish();
        }
    }

    public static void main(final String[] args) throws Exception {
        Map<String, String> configuration = EmbeddedGraphDatabase.loadConfigurations("var/neo4j_config.props");
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase("var/graphdb/", configuration);

        parseOntologyNodes(ONTOLOGY_FILE, graphDb);
        parseOntologyRelationships(ONTOLOGY_FILE, graphDb);

        createDefaultRelationships(graphDb);

        graphDb.shutdown();
    }
}
