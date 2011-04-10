import org.neo4j.graphdb.*;

/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 09/04/11
 * Time: 21:00
 * To change this template use File | Settings | File Templates.
 */
public class TermImpl {
    private final Node underlyingNode;

    private static final String KEY_ACCESSION = "accession";
    private static final String KEY_NAME = "name";
    private static final String KEY_DESCRIPTION = "description";

    public TermImpl( Node underlyingNode){
        this.underlyingNode = underlyingNode;
    }

    public void setAccession(String accession){
        underlyingNode.setProperty( KEY_ACCESSION, accession);
    }

    public String getAccession(){
        return (String) underlyingNode.getProperty(KEY_ACCESSION);
    }

    public void setName(String name){
        underlyingNode.setProperty( KEY_NAME, name);
    }

    public String getName(){
        return (String) underlyingNode.getProperty(KEY_NAME);
    }

    public void setDescription(String description){
        underlyingNode.setProperty( KEY_NAME, description);
    }

    public String getDescription(){
        return (String) underlyingNode.getProperty(KEY_DESCRIPTION);
    }
}
