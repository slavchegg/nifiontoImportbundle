package com.slavchegg.semantics

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.eclipse.rdf4j.model.*
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.vocabulary.XMLSchema
import org.eclipse.rdf4j.rio.RDFHandlerException
import org.janusgraph.core.JanusGraph

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import static Main.*
import static RDFParserConfig.*

abstract class RDFToLPG extends ConfiguredStatementHandler{
    Map<String, String> vocMappings
    static protected Map<String, String> namespaces = new HashMap<>()
    protected Set<Statement> statements = new HashSet<>()
    Map<String, Map<String, Object>> resourceProps = new HashMap<>()
    Map<String, String> resourceLabels = new HashMap<>()
    long totalTriplesParsed = 0
    long totalTriplesMapped = 0
    long mappedTripleCounter = 0

    protected JanusGraph janusGraph
    GraphTraversalSource g
    RDFParserConfig parserConfig

    RDFToLPG(JanusGraph graph, RDFParserConfig conf) {
        this.janusGraph = graph
        this.parserConfig = conf
        this.g = janusGraph.traversal()
        this.vocMappings = null
    }

    private void loadNamespaces() {
        GraphTraversal<Vertex, Map<Object, Object>> gts = g.V().hasLabel("namespace").valueMap()
        if (!gts.hasNext()) {
            //no namespace definition, initialise with popular ones
            namespaces.putAll(popularNamespaceList())
        } else {
            while (gts.hasNext()) {
                Map<Object, Object> ns = gts.next()
                String uri = ns.get("uri").toString()
                String prefix = ns.get("prefix").toString()
                namespaces.put(uri.substring(1, uri.length()-1), prefix.substring(1, prefix.length()-1))
            }
            popularNamespaceList().forEach({ k, v ->
                if (!namespaces.containsKey(k)) {
                    namespaces.put(k, v)
                }
            })
        }
    }

    private Map<String, String> popularNamespaceList() {
        Map<String, String> ns = new HashMap<>()
        ns.put("http://schema.org/", "sch")
        ns.put("http://purl.org/dc/elements/1.1/", "dc")
        ns.put("http://purl.org/dc/terms/", "dct")
        ns.put("http://www.w3.org/2004/02/skos/core#", "skos")
        ns.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs")
        ns.put("http://www.w3.org/2002/07/owl#", "owl")
        ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf")
        ns.put("http://www.w3.org/ns/shacl#", "sh")
        return ns
    }


    private String getPrefix(String namespace) {
        if (namespaces.containsKey(namespace)) {
            return namespaces.get(namespace)
        } else {
            namespaces.put(namespace, nextPrefix())
            return namespaces.get(namespace)
        }
    }

    private String nextPrefix() {
        return "ns" + namespaces.values().stream().filter({ x -> x.startsWith("ns") }).count()
    }

    ///
    Object getObjectValue(IRI propertyIRI, Literal object) {
        IRI datatype = object.getDatatype()
        if (datatype == XMLSchema.STRING || datatype == RDF.LANGSTRING) {
            final Optional<String> language = object.getLanguage()
            if (parserConfig.getLanguageFilter() == null || !language.isPresent() || parserConfig
                    .getLanguageFilter() == language.get()) {
                return object.stringValue() + (parserConfig.isKeepLangTag() && language.isPresent() ? "@"
                        + language.get()
                        : "")
            } else {
                return null
            }
        } else if (typeMapsToLongType(datatype)) {
            return object.longValue()
        } else if (typeMapsToDouble(datatype)) {
            return object.doubleValue()
        } else if (datatype == XMLSchema.BOOLEAN) {
            return object.booleanValue()
        } else if (datatype == XMLSchema.DATETIME) {
            try {
                return LocalDateTime.parse(object.stringValue())
            } catch (DateTimeParseException e) {
                //if date cannot be parsed we return string value
                return object.stringValue()
            }
        } else if (datatype == XMLSchema.DATE) {
            try {
                return LocalDate.parse(object.stringValue())
            } catch (DateTimeParseException e) {
                //if date cannot be parsed we return string value
                return object.stringValue()
            }
        } else {
            //it's a custom data type
            if (parserConfig.isKeepCustomDataTypes() && !(parserConfig.getHandleVocabUris() == URL_IGNORE
                    || parserConfig.getHandleVocabUris() ==URL_MAP)) {
                //keep custom type
                String value = object.stringValue()
                if (parserConfig.getCustomDataTypedPropList() == null || parserConfig
                        .getCustomDataTypedPropList()
                        .contains(propertyIRI.stringValue())) {
                    String datatypeString
                    if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
                        datatypeString = handleIRI(datatype,DATATYPE)
                    } else {
                        datatypeString = datatype.stringValue()
                    }
                    value = value.concat(CUSTOM_DATA_TYPE_SEPERATOR + datatypeString)
                }
                return value
            }
        }
        // default
        return object.stringValue()
    }

    private boolean typeMapsToDouble(IRI datatype) {
        return datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
                datatype.equals(XMLSchema.FLOAT)
    }

    private boolean typeMapsToLongType(IRI datatype) {
        return datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG) || datatype
                .equals(XMLSchema.INT) ||
                datatype.equals(XMLSchema.SHORT) || datatype.equals(XMLSchema.BYTE) ||
                datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER) || datatype
                .equals(XMLSchema.POSITIVE_INTEGER) ||
                datatype.equals(XMLSchema.UNSIGNED_LONG) || datatype.equals(XMLSchema.UNSIGNED_INT) ||
                datatype.equals(XMLSchema.UNSIGNED_SHORT) || datatype.equals(XMLSchema.UNSIGNED_BYTE) ||
                datatype.equals(XMLSchema.NON_POSITIVE_INTEGER) || datatype
                .equals(XMLSchema.NEGATIVE_INTEGER)
    }

    @Override
    void handleComment(String comment) throws RDFHandlerException {

    }


    String handleIRI(IRI iri, int elementType) {
        //TODO: would caching this improve perf? It's kind of cached in getPrefix()
        if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
            String localName = iri.getLocalName()
            String prefix = getPrefix(iri.getNamespace())
            return prefix + PREFIX_SEPARATOR + localName
        } else if (parserConfig.getHandleVocabUris() ==URL_IGNORE) {
            return applyCapitalisation(iri.getLocalName(), elementType)
        } else if (parserConfig.getHandleVocabUris() == URL_MAP) {
            return mapElement(iri, elementType, null)
        } else {
            return iri.stringValue()
        }
    }

    private String applyCapitalisation(String name, int element) {
        if (parserConfig.isApplyNeo4jNaming()) {
            //apply Neo4j naming recommendations
            if (element == RELATIONSHIP) {
                return name.toUpperCase()
            } else if (element == LABEL) {
                return name.substring(0, 1).toUpperCase() + name.substring(1)
            } else if (element == PROPERTY) {
                return name.substring(0, 1).toLowerCase() + name.substring(1)
            } else {
                //should not happen
                return name
            }
        } else {
            //keep capitalisation as is
            return name
        }
    }


    private String mapElement(IRI iri, int elementType, String mappingId) {
        //Placeholder for mapping based data load
        //if mappingId is null use default mapping
        if (this.vocMappings.containsKey(iri.stringValue())) {
            return this.vocMappings.get(iri.stringValue())
        } else {
            //if no mapping defined, default to 'IGNORE'
            return applyCapitalisation(iri.getLocalName(), elementType)
        }
    }


    @Override
    void startRDF() throws RDFHandlerException {
        if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
            //differentiate between map/shorten and keep_long urls?
            loadNamespaces()
            System.out.println("Found " + namespaces.size() + " namespaces in the DB: " + namespaces)
        } else {
            System.out.println("URIs will be ignored. Only local names will be kept.")
        }

    }

    @Override
    void handleNamespace(String prefix, String uri) throws RDFHandlerException {

    }

    void addStatement(Statement st) {
        statements.add(st)
    }


    private void initialise(String subjectUri) {
        initialiseProps(subjectUri)
        initialiseLabels(subjectUri)
    }

    private HashMap<String, Object> initialiseProps(String subjectUri) {
        HashMap<String, Object> props = new HashMap<>()
        resourceProps.put(subjectUri, props)
        return props
    }

    private String initialiseLabels(String subjectUri) {
        String labels = null
        resourceLabels.put(subjectUri, labels)
        return labels
    }

    private boolean setProp(String subjectUri, IRI propertyIRI, Literal propValueRaw) {
        Map<String, Object> props

        String propName = handleIRI(propertyIRI, PROPERTY)

        Object propValue = getObjectValue(propertyIRI, propValueRaw)
        if (propValue != null) {
            if (!resourceProps.containsKey(subjectUri)) {
                props = initialiseProps(subjectUri)
            } else {
                props = resourceProps.get(subjectUri)
            }
            if (parserConfig.getHandleMultival() == PROP_OVERWRITE) {
                props.put(propName, propValue)
            } else if (parserConfig.getHandleMultival() == PROP_ARRAY) {
                if (parserConfig.getMultivalPropList() == null || parserConfig.getMultivalPropList()
                        .contains(propertyIRI.stringValue())) {
                    if (props.containsKey(propName)) {
                        List<Object> propVals = (List<Object>) props.get(propName)
                        propVals.add(propValue)
                    } else {
                        List<Object> propVals = new ArrayList<>()
                        propVals.add(propValue)
                        props.put(propName, propVals)
                    }
                } else {
                    props.put(propName, propValue)
                }
            }
        }
//        System.out.println(resourceProps);
        return propValue != null
    }

    void setLabel(String subjectUri, String label) {
        if (!resourceLabels.containsKey(subjectUri)) {
            initialiseProps(subjectUri)
        }
        resourceLabels.put(subjectUri, label)
    }

    private void addResource(String subjectUri) {
        if (!resourceLabels.containsKey(subjectUri)) {
            initialise(subjectUri)
        }
    }

    @Override
    void handleStatement(Statement st) {
        IRI predicate = st.getPredicate()
        Resource subject = st.getSubject()
        Value object = st.getObject()

        if (parserConfig.getPredicateExclusionList() == null || !parserConfig
                .getPredicateExclusionList()
                .contains(predicate.stringValue()))
        // filter by predicate
        {
            if (object instanceof Literal) {
                // DataType property
                if (setProp(subject.stringValue(), predicate, (Literal) object)) {
                    // property may be filtered because of lang filter hence the conditional increment.
                    mappedTripleCounter++
                }
            } else if (parserConfig.isTypesToLabels() && predicate == RDF.TYPE
                    && !(object instanceof BNode)) {
                setLabel(subject.stringValue(), handleIRI((IRI) object, LABEL))
                mappedTripleCounter++
            } else {
                addResource(subject.stringValue())
                addResource(object.stringValue())
                addStatement(st)
                mappedTripleCounter++
            }
        }
        totalTriplesParsed++

        if (parserConfig.getCommitSize() != Long.MAX_VALUE
                && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
            periodicOperation()
        }
    }

    @Override
    RDFParserConfig getParserConfig() {
        return parserConfig
    }

    Map<String, String> getNamespaces() {
        return namespaces
    }

    protected abstract void periodicOperation();
}
