import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
/**
 * Created by IntelliJ IDEA.
 * User: nachocab
 * Date: 09/04/11
 * Time: 19:55
 * To change this template use File | Settings | File Templates.
 */
//http://nosql.mypopescu.com/post/405045629/get-a-taste-of-graph-databases-infogrid-and-neo4j
public class TagExample 
{
    private static final String KEY_URI = "uri";
    private static final String KEY_LABEL = "label";
    
    private static enum RelTypes implements RelationshipType
    {
        TAG,
        TAGGED_WITH,
    }
    
    private static GraphDatabaseService graphDb;
    private static IndexService index;
    
    public static void main( String[] args )
    {
        graphDb = new EmbeddedGraphDatabase( "neo4j-db" );
        index = new LuceneIndexService( graphDb );
        
        Transaction tx = graphDb.beginTx();
        try
        {
            // Tag some resources
            Node tagLibrary = getTagLibrary();
            Node cnn = getOrCreateResource( "http://cnn.com/" );
            Node onion = getOrCreateResource( "http://theonion.com/" );
            Node xkcd = getOrCreateResource( "http://xkcd.com/" );
            
            Node funny = getOrCreateTag( "funny" );
            Node good = getOrCreateTag( "good" );
            
            tagResource( cnn, good );
            tagResource( xkcd, good );
            tagResource( onion, funny );
            tagResource( xkcd, funny );
            
            // Do some querying
            System.out.println( "Listing all tags and resources" );
            for ( Relationship tagRelationship : graphDb.getReferenceNode().getRelationships(
                    RelTypes.TAG ) )
            {
                Node tag = tagRelationship.getEndNode();
                System.out.println( "    " + tag.getProperty( KEY_LABEL ) );
                for ( Relationship resourceRelationship : tag.getRelationships(
                        RelTypes.TAGGED_WITH ) )
                {
                    Node resource = resourceRelationship.getOtherNode( tag );
                    System.out.println( "        " + resource.getProperty( KEY_URI ) );
                }
            }
            
            // Find out which tags xkcd is tagged with
            System.out.println( "Xkcd is tagged with:" );
            for ( Relationship relationship : xkcd.getRelationships( RelTypes.TAGGED_WITH ) )
            {
                Node tag = relationship.getOtherNode( xkcd );
                System.out.println( "    " + tag.getProperty( KEY_LABEL ) );
            }
            
            // List tagged sites
            System.out.println( "All tagged sites (note there are no duplicates)" );
            ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
            {
                public boolean isReturnableNode( TraversalPosition currentPos )
                {
                    return currentPos.lastRelationshipTraversed() != null &&
                            currentPos.lastRelationshipTraversed().getType().name().equals(
                                    RelTypes.TAGGED_WITH.name() );
                }
            };
            for ( Node resource : tagLibrary.traverse( Order.BREADTH_FIRST,
                    StopEvaluator.END_OF_GRAPH, returnEvaluator,
                    RelTypes.TAG, Direction.OUTGOING,
                    RelTypes.TAGGED_WITH, Direction.INCOMING ) )
            {
                System.out.println( "    " + resource.getProperty( KEY_URI ) );
            }

            
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        index.shutdown();
        graphDb.shutdown();
    }

    
    private static Node getTagLibrary()
    {
        // TODO Just for this example use the reference node, but you could
        // consider creating a sub-reference node instead…
        return graphDb.getReferenceNode();
    }

    private static void tagResource( Node resource, Node tag )
    {
        // See if it’s already tagged?
        for ( Relationship relationship : resource.getRelationships( RelTypes.TAGGED_WITH ) )
        {
            if ( relationship.getOtherNode( resource ).equals( tag ) )
            {
                return;
            }
        }
        
        // Tag it
        resource.createRelationshipTo( tag, RelTypes.TAGGED_WITH );
    }

    private static Node getOrCreateTag( String label )
    {
        Node node = index.getSingleNode( KEY_LABEL, label);
        if ( node == null )
        {
            node = graphDb.createNode();
            node.setProperty( KEY_LABEL, label );
            getTagLibrary().createRelationshipTo( node, RelTypes.TAG );
            index.index( node, KEY_LABEL, label );
        }
        return node;
    }

    private static Node getOrCreateResource( String uri )
    {
        Node node = index.getSingleNode( KEY_URI, uri );
        if ( node == null )
        {
            node = graphDb.createNode();
            node.setProperty( KEY_URI, uri );
            index.index( node, KEY_URI, uri );
        }
        return node;
    }
}

