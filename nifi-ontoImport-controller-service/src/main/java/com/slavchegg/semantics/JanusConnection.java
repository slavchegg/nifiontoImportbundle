package com.slavchegg.semantics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;


@Tags({"janusgraph", "connection", "pool", "graph"})
@CapabilityDescription("Provides connection to JanusGraph")
public class JanusConnection extends AbstractControllerService implements JanusSession {

    public static final PropertyDescriptor JANUS_CONFIG = new PropertyDescriptor
            .Builder().name("JanusGraph Configuration")
            .description("JanusGraph configuration file")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(JANUS_CONFIG);
        properties = Collections.unmodifiableList(props);
    }

    private JanusGraph graph;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        graph = JanusGraphFactory.open(context.getProperty(JANUS_CONFIG).getValue());
    }

    @OnDisabled
    public void shutdown() {
        graph.close();
    }

    @Override
    public JanusGraph getGraph() {
        return graph;
    }
}
