package XMLToCypher;

import java.util.*;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Hello world!
 *
 */
public class Convert
{
    @SuppressWarnings("deprecation")
	public String getCypher( String XML ) {

    	String cypherQuery = null;

    	try {

    		//syötteitä * a/b/c a[@b]
            //olettaa yhden syötteen args 0:sta
    		String inputCmd = null;
    		inputCmd = XML;
            // create a CharStream that reads from standard input
            ANTLRInputStream input = new ANTLRInputStream(inputCmd);

            // create a lexer that feeds off of input CharStream
            xpathLexer lexer = new xpathLexer(input);

            // create a buffer of tokens pulled from the lexer
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            // create a parser that feeds off the tokens buffer
            xpathParser parser = new xpathParser(tokens);

            MyListener mylistener = new MyListener();

            parser.addParseListener(mylistener);
            ParseTree tree = parser.main();    // begin parsing at rule main
            //System.out.println(tree.toStringTree(parser)); // print LISP-style tree

			cypherQuery = mylistener.wholeQuery.toString();
            
    		/*Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "testi" ) );
        	  Session session = driver.session();
        	  StatementResult stre = session.run(cypQ);
        	  List<Record> lr = stre.list();
        	  System.out.println(lr.size());
        	  Gson gson = new GsonBuilder().setPrettyPrinting().create();
        	  for (int i = 0; i < lr.size(); i++) {
        		String json = gson.toJson(gson.toJson(lr.get(i).asMap()));
        		System.out.println(json);
        	}
        	*/
        	//System.out.println(cypQ);
        	//System.out.println(lr.size());
        	//driver.close();
    	} catch (Exception e) {
    		System.out.println("virhe!");
    		System.out.println(e);
    	}

    return cypherQuery;
    }
}
