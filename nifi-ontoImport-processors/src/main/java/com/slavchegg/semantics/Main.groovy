package com.slavchegg.semantics


import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.RDFParser
import org.eclipse.rdf4j.rio.Rio
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings
import org.neo4j.procedure.Name

class Main {
    public static RDFFormat[] availableParsers = [RDFFormat.RDFXML, RDFFormat.JSONLD,
                                                  RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS]

    public static final String PREFIX_SEPARATOR = "__"
    public static final String CUSTOM_DATA_TYPE_SEPERATOR = "^^"
    static final int RELATIONSHIP = 0
    static final int LABEL = 1
    static final int PROPERTY = 2
    static final int DATATYPE = 3

    static void parseRDF(InputStream inputStream, @Name("url") String url,
                                 @Name("format") String format,
                                 ConfiguredStatementHandler handler)
            throws IOException, Main.RDFImportPreRequisitesNotMet {
        RDFParser rdfParser = Rio.createParser(getFormat(format))
        rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, handler.getParserConfig().isVerifyUriSyntax())
        rdfParser.setRDFHandler(handler)
        rdfParser.parse(inputStream, url)
        Util.DEFAULT.shutdownNow()
    }

    static class RDFImportPreRequisitesNotMet extends Exception {

        String message

        RDFImportPreRequisitesNotMet(String s) {
            message = s
        }

        @Override
        String getMessage() {
            return message
        }
    }

    private static RDFFormat getFormat(String format) throws Main.RDFImportPreRequisitesNotMet {
        if (format != null) {
            for (RDFFormat parser : availableParsers) {
                if (parser.getName() == format) {
                    return parser
                }
            }
        }
        throw new Main.RDFImportPreRequisitesNotMet("Unrecognized serialization format: " + format)
    }
}
