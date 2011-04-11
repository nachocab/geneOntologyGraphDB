import org.neo4j.graphdb.RelationshipType;

/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 09/04/11
 * Time: 17:17
 * To change this template use File | Settings | File Templates.
 */
public enum OntologyRelTypes implements RelationshipType {
    IS_A,
    PART_OF,
    REGULATES,
    NEGATIVELY_REGULATES,
    POSITIVELY_REGULATES
}
