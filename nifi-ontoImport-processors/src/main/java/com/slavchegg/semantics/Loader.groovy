package com.slavchegg.semantics

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.RDFHandlerException
import org.janusgraph.core.JanusGraph
import org.neo4j.helpers.collection.Iterables

import java.util.concurrent.Callable
import java.util.stream.Collectors

class Loader extends RDFToLPG implements Callable<Integer> {
    private static final String[] EMPTY_ARRAY = new String[0]
    private Cache<String, Vertex> nodeCache

    Loader(JanusGraph graph, RDFParserConfig conf) {
        super(graph, conf)
        nodeCache = CacheBuilder.newBuilder()
                .maximumSize(conf.getNodeCacheSize())
                .build()
    }

    @Override
    void endRDF() throws RDFHandlerException {
        totalTriplesMapped += mappedTripleCounter
        if (parserConfig.getHandleVocabUris() == RDFParserConfig.URL_SHORTEN) {
            persistNamespaceNode()
        }
        Util.inTx(g, this)
    }

    private void persistNamespaceNode() {
        for (Map.Entry<String, String> namespace : namespaces.entrySet()){
            try {
                g.V().hasLabel("namespace").has("uri", namespace.getKey()).has("prefix", namespace.getValue()).next()
            }
            catch (NoSuchElementException e){
                g.addV("namespace").property("uri", namespace.getKey()).property( "prefix", namespace.getValue()).next()
            }
        }
        g.tx().commit()
    }

    private Object toPropertyValue(Object value) {
        Iterable it = (Iterable) value
        Object first = Iterables.firstOrNull(it)
        if (first == null) {
            return EMPTY_ARRAY
        }
        return Iterables.asArray(first.getClass(), it)
    }

    @Override
    Integer call() throws Exception {
        for (Map.Entry<String, String> entry : resourceLabels.entrySet()) {
            final Vertex vertex = nodeCache.get(entry.getKey(), { ->
                Vertex vertex1
                if (entry.getValue() == null) {
                    entry.value = "vertex"
                }
                try {
                    vertex1 = g.V().hasLabel(entry.getValue()).has("uri", entry.getKey()).next()
                }
                catch (NoSuchElementException e) {
                    vertex1 = g.addV(entry.getValue()).property("uri", entry.getKey()).next()
                }
                return vertex1
            })

            resourceProps.get(entry.getKey()).forEach({ k, v ->
                if (v instanceof List) {
                    Object currentValue = vertex.property(k, null)
                    if (currentValue == null) {
                        vertex.property(k, toPropertyValue(v))
                    } else {
                        if (currentValue.getClass().isArray()) {
                            Object[] properties = (Object[]) currentValue
                            for (Object property : properties) {
                                ((List) v).add(property)
                            }
                        } else {
                            ((List) v).add(vertex.property(k))
                        }
                        //we make it a set to remove duplicates. Semantics of multivalued props in RDF.
                        vertex.property(k, toPropertyValue(((List) v).stream().collect(Collectors.toSet())))
                    }
                } else {
                    vertex.property(k, v)
                }
            })
        }

        for (Statement st : statements) {
            final Vertex fromVertex = nodeCache
                    .get(st.getSubject().stringValue(), { ->   //throws AnyException
                        return g.V().has("uri", st.getSubject().stringValue()).next()
                    })

            final Vertex toVertex = nodeCache.get(st.getObject().stringValue(), { ->   //throws AnyException
                return g.V().has("uri", st.getObject().stringValue()).next()
            })

            // check if the rel is already present. If so, don't recreate.
            // explore the node with the lowest degree

            boolean found = false
            Iterator<Edge> fromIter = fromVertex.edges(Direction.OUT, handleIRI(st.getPredicate(), Main.RELATIONSHIP))
            Iterator<Edge> toIter = toVertex.edges(Direction.IN, handleIRI(st.getPredicate(), Main.RELATIONSHIP))
            if (edgeCounter(fromIter) <
                    edgeCounter(toIter)) {
                while (fromIter.hasNext()){
                    if (fromIter.next().outVertex().equals(fromVertex)){
                        found = true
                        break
                    }
                }
            } else {
                while (toIter.hasNext()){
                    if (toIter.next().inVertex() == toVertex){
                        found = true
                        break
                    }
                }
            }

            if (!found) {
                g.V(fromVertex).as("a").V(toVertex).addE(handleIRI(st.getPredicate(), Main.RELATIONSHIP)).from("a").next()
            }
        }

        statements.clear()
        resourceLabels.clear()
        resourceProps.clear()
        return 0
    }

    @Override
    protected void periodicOperation() {
        Util.inTx(g, this)
        totalTriplesMapped += mappedTripleCounter
        mappedTripleCounter = 0
        persistNamespaceNode()
    }

    private int edgeCounter (Iterator i){
        int result = 0
        while (i.hasNext()){
            i.next()
            result ++
        }
        return result
    }
}
