/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.slavchegg.semantics;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.janusgraph.core.JanusGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Tags({"janusGraph", "semantics", "ontology", "import", "parsing"})
@CapabilityDescription("Parses semantics files and uploads them into JanusGraph")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="filename", description="The filename is set to the name of the file on disk")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class SemanticsImport extends AbstractProcessor {

    static final PropertyDescriptor JANUS_CONNECTION = new PropertyDescriptor.Builder()
            .name("JanusGraph Connection Service")
            .description("Specifies connection to JanusGraph")
            .identifiesControllerService(JanusSession.class)
            .required(true)
            .build();

    public static final PropertyDescriptor URIS = new PropertyDescriptor
            .Builder().name("Vocab URI's")
            .description("URI's to labels and props")
            .required(true)
            .allowableValues("Shorten", "Ignore", "Map", "Keep")
            .defaultValue("Shorten")
            .build();

    public static final PropertyDescriptor TYPES_TO_LABELS = new PropertyDescriptor
            .Builder().name("Types to Labels")
            .description("Map RDF Types to JanusGraph Labels")
            .required(true)
            .allowableValues("True", "False")
            .defaultValue("True")
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("my_relationship")
            .description("Example relationship")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    static ComponentLog bigLogger;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(URIS);
        descriptors.add(JANUS_CONNECTION);
        descriptors.add(TYPES_TO_LABELS);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
        bigLogger = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        ComponentLog logger = getLogger();
        final JanusSession js = context.getProperty(JANUS_CONNECTION).asControllerService(JanusSession.class);
        Map<String, Object> props = new HashMap();
        RDFParserConfig conf = new RDFParserConfig(props);
        props.put("handleVocabUris", URIS);
        props.put("typesToLabels", TYPES_TO_LABELS);
        Loader statementLoader = new Loader(js.getGraph(), conf);
        String filename =  flowFile.getAttribute("filename");
        String format = fileFormat().get(parseFileFormat(filename));
        AtomicBoolean flag = new AtomicBoolean();
        session.read(flowFile, in -> {
            logger.info("\nPROCESS STARTED\n");
            try {
                Main.parseRDF (in, filename, format, statementLoader);
                flag.set(true);
            } catch (Main.RDFImportPreRequisitesNotMet rdfImportPreRequisitesNotMet) {
                rdfImportPreRequisitesNotMet.printStackTrace();
                flag.set(false);
            }
        }) ;
        if (flag.get()){
            session.transfer(flowFile, REL_SUCCESS);
        }
        else session.transfer(flowFile, REL_FAILURE);
    }

    private Map<String, String> fileFormat() {
        Map<String, String> ns = new HashMap<>();
        ns.put("owl", "RDF/XML");
        ns.put("rdf", "RDF/XML");
        ns.put("nt", "N-Triples");
        ns.put("ttl", "Turtle");
        ns.put("jsonld", "JSON-LD");
        ns.put("nq", "N-Quads");
        ns.put("trig", "TriG");
        return ns;
    }

    private String parseFileFormat (String filename){
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
}
