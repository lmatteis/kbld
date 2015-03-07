import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.http.HTTPClient;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.HTTPDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.any23.writer.TurtleWriter;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MyTripleHandler2 implements TripleHandler {
    private Model model;
    private String uri;

    public MyTripleHandler2(String uri, Model model) {
        super();
        this.uri = uri;
        this.model = model;
    }

    @Override
    public void receiveTriple(Resource s, URI p, Value o, URI g,
            ExtractionContext context) throws TripleHandlerException {
        // TODO should only add where URI is either s or o
        if(s.toString().equals(uri) || o.toString().equals(uri)) {
            model.add(s, p, o, null);
        }
    }

    @Override
    public void close() throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void closeContext(ExtractionContext arg0)
            throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void endDocument(URI arg0) throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void openContext(ExtractionContext arg0)
            throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void receiveNamespace(String arg0, String arg1,
            ExtractionContext arg2) throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setContentLength(long arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void startDocument(URI arg0) throws TripleHandlerException {
        // TODO Auto-generated method stub
        
    }

}

public class CopyOfMain {
    private static HashMap<String, Model> resolved = new HashMap<String, Model>();
    private static PrintWriter out;
    private static long startTime;
    private static int totalTriples = 0;
    
    public static void Navigate(ArrayList<String> S, ArrayList<String> Knew) throws URISyntaxException, IOException, TripleHandlerException, ExtractionException, RDFHandlerException {
        ArrayList<String> K = (ArrayList<String>) Knew.clone();
        String currentKeyword = K.remove(0); // shift array
        Model T = new LinkedHashModel();
        for(String s: S) {
            Model R = resolveURI(s);
            Model F = filter(currentKeyword, R);
            remove(R, F); // this removes F statements from R: R-F
            T.addAll(R);
            if(F.size() > 0 && K.size() == 0) {
                long estimatedTime = System.currentTimeMillis() - startTime;
                totalTriples += F.size();
                Rio.write(F, System.out, RDFFormat.NTRIPLES);
                out.println("("+estimatedTime / 1000.0 + "," + totalTriples + ") ["+currentKeyword+"]");
                out.flush();
            }
            if(F.size() > 0 && K.size() > 0) {
                long estimatedTime = System.currentTimeMillis() - startTime;
            	totalTriples += F.size();
                out.println("("+estimatedTime / 1000.0 + "," + totalTriples + ") ["+currentKeyword+"]");
                out.flush();
                Navigate(getURIsExceptSandPredicates(S, F), K);         
            }
        }
        for(Statement t: T) {
            Model P = resolveURI(t.getPredicate().toString());
            Model F = filter(currentKeyword, P, true); // should filter only literals
            if(F.size() > 0 && K.size() == 0) {
                Rio.write(F, System.out, RDFFormat.NTRIPLES);
                long estimatedTime = System.currentTimeMillis() - startTime;
                totalTriples += 1;
                out.println("("+estimatedTime / 1000.0 + ","+totalTriples+") ["+currentKeyword+"]");
                out.flush();  
            }
            if(F.size() > 0 && K.size() > 0) {
            	long estimatedTime = System.currentTimeMillis() - startTime;
                totalTriples += 1;
                out.println("("+estimatedTime / 1000.0 + ","+totalTriples+") ["+currentKeyword+"]");
                out.flush(); 
                Navigate(getURIsExceptSandPredicates(S, t), K);         
            }
        }
    }




    private static String getEdge(ArrayList<String> uris, Statement statement) {
        Resource subj = statement.getSubject();
        Value obj = statement.getObject();
        URI edgeURI = null;
        if(subj instanceof URI && !uris.contains(subj.toString())) {
            //System.out.println("edge subject: " + subj.toString());
            edgeURI = (URI) subj;
        } else if(obj instanceof URI && !uris.contains(obj.toString())) {
            //System.out.println("edge object:  " + obj.toString());
            edgeURI = (URI) obj;
        }
        if(edgeURI == null) return null;
        return edgeURI.toString();
    }



    private static ArrayList<String> getEdges(ArrayList<String> uris, Model model) {
        ArrayList<String> edges = new ArrayList<String>();
        
        for (Statement statement: model) {
            Resource subj = statement.getSubject();
            Value obj = statement.getObject();
            URI edgeURI = null;
            if(subj instanceof URI && !uris.contains(subj.toString())) {
                //System.out.println("edge subject: " + subj.toString());
                edgeURI = (URI) subj;
            } else if(obj instanceof URI && !uris.contains(obj.toString())) {
                //System.out.println("edge object:  " + obj.toString());
                edgeURI = (URI) obj;
            }
            // resolve this edge URI
 

            if(edgeURI != null) {
                if(edges.contains(edgeURI.toString())) // we already did this edge
                    continue;
                edges.add(edgeURI.toString());
            }

        }
        return edges;
    }

    // removes statements of m2 from m1: m1 - m2
    private static void remove(Model m1, Model m2) {
        for(Statement st: m2) {
            if(m1.contains(st)) {
                m1.remove(st);
            }
        }
    }

    private static ArrayList<String> getPredicates(Model model) {
        ArrayList<String> predicates = new ArrayList<String>();
        
        for (Statement statement: model) {
            String p = statement.getPredicate().toString();

            if(predicates.contains(p)) // we already did this predicate
                continue;
            predicates.add(p);
        }
        return predicates;
    }
    
    private static ArrayList<String> getURIsExceptSandPredicates(ArrayList<String> s, Statement st) {
        ArrayList<String> ret = new ArrayList<String>();
        
        Resource subj = st.getSubject();
        Value obj = st.getObject();
        if(subj instanceof URI && !s.contains(subj.toString())) {
            if(!ret.contains(subj.toString()))
                ret.add(subj.toString());
        }
        if(obj instanceof URI && !s.contains(obj.toString())) {
            if(!ret.contains(obj.toString()))
                ret.add(obj.toString());
        }
        return ret;
    }
    private static ArrayList<String> getURIsExceptSandPredicates(ArrayList<String> s, Model MT) {
        ArrayList<String> ret = new ArrayList<String>();
        for(Statement st: MT) {
            Resource subj = st.getSubject();
            Value obj = st.getObject();
            if(subj instanceof URI && !s.contains(subj.toString())) {
                if(!ret.contains(subj.toString()))
                    ret.add(subj.toString());
            }
            if(obj instanceof URI && !s.contains(obj.toString())) {
                if(!ret.contains(obj.toString()))
                    ret.add(obj.toString());
            }
        }
        return ret;
    }


    private static Model filter(String k, Model model, boolean b) {
        Model m = new LinkedHashModel();
        for (Statement statement: model) {      
            String s = statement.getObject().toString();
            if(s.toLowerCase().contains(k.toLowerCase())) {
                m.add(statement);
            }
            
        }
        return m;
    }

    private static Model filter(String k, Model model) {
        Model m = new LinkedHashModel();
        for (Statement statement: model) {      
            String s = statement.getSubject().toString() + statement.getPredicate().toString() + statement.getObject().toString();
            if(s.toLowerCase().contains(k.toLowerCase())) {
                m.add(statement);
            }
            
        }
        return m;
    }

    // resolves URI and returns model containing only triples where this URI appears
    // in either subject or object position
    private static Model resolveURI(String uri) throws URISyntaxException, IOException, TripleHandlerException, RDFHandlerException  {
        Any23 runner = new Any23();
        runner.setHTTPUserAgent("test-user-agent");
        HTTPClient httpClient = runner.getHTTPClient();
        DocumentSource source = new HTTPDocumentSource(
             httpClient,
             uri
        );
        // create a new Model to put statements in
        Model model = new LinkedHashModel(); 
        // this fills up the model
        TripleHandler handler = new MyTripleHandler2(uri, model);
        
        try {
            if(resolved.get(uri) == null) { // we didn't resolve this URI
                runner.extract(source, handler);
                // model contains stuff
                
                resolved.put(uri, model);
            } else {
                model = resolved.get(uri);
                return model;
            }
        } catch (IOException | java.lang.IllegalStateException 
                | java.nio.charset.UnsupportedCharsetException | ExtractionException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
        } finally {
            handler.close();
        }
        return model;
    }
    public static String getDomainName(String url) throws MalformedURLException {
        URL uri = new URL(url);
        String domain = uri.getHost();
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    public static void main(String[] args) throws URISyntaxException, TripleHandlerException, ExtractionException, RDFHandlerException, IOException {     
        out = new PrintWriter(new BufferedWriter(new FileWriter("stream.txt", true)));
        //out = System.out;
        startTime = System.currentTimeMillis();
        
        ArrayList<String> S = new ArrayList<String>(Arrays.asList("http://sws.geonames.org/3169070/"));
        ArrayList<String> K = new ArrayList<String>(Arrays.asList("dbpedia", "airport", "runway"));
        Navigate(S, K);
        
        
        ArrayList<String> domains = new ArrayList<String>();
        for (String key : resolved.keySet()) {
            String domain = getDomainName(key);
            if(!domains.contains(domain)) {
                domains.add(domain);
            }
        }
        out.println("# of accessed servers: " + domains.size());
        
        long estimatedTime = System.currentTimeMillis() - startTime;
        out.println("done. " + estimatedTime / 1000.0);
        
        out.close();
    
    }

}