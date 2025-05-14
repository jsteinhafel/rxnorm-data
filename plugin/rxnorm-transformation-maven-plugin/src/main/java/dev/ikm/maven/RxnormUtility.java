package dev.ikm.maven;

import dev.ikm.tinkar.common.id.*;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;

import java.util.UUID;
import java.util.regex.*;


public class RxnormUtility {
    public static final String SNOMED_IDENTIFIER_PUBLIC_ID = "ed73f32d-c068-43f8-9767-ace6dfee44db";
    public static final String RXCUID_IDENTIFIER_PUBLIC_ID = "e409e4ce-4527-49a0-bdc6-f4fe79c21088";
    public static final String NDC_IDENTIFIER_PUBLIC_ID = "88f22a11-3715-4d58-8cb4-aa14d49a6b35";
    public static final String VU_IDENTIFIER_PUBLIC_ID = "5d06759b-56f7-4c24-be70-5cea09e0e130";
    public static final String TALLMAN_DESCRIPTION = "64943aee-fbad-485f-921d-ca03501faf26";
    public static final String QUALITATIVE_DISTINCTION_PATTERN = "e409e4ce-4527-49a0-bdc6-f4fe79c21088";
    public static final String QUANTITY_PATTERN = "bd1fdc59-962c-44e1-8a77-8c91a4faac7e";
    public static final String SCHEDULE_PATTERN = "95b64af6-cf7c-4a92-aacd-c7e9fa968aa4";
    public static final String HUMAN_DRUG_PATTERN = "d5fd4cff-b3fa-490d-aed8-85459c1edfe1";
    public static final String VET_DRUG_PATTERN = "8140c0a7-76b5-4fe7-a6e0-c27246e26a1d";
    public static final String TALLMAN_SYNONYM_PATTERN = "6b46672e-6a1d-4f6d-b4d8-9ca047f916cc";
    public static EntityProxy.Pattern makePatternProxy(UUID namespace, String description) {
        return EntityProxy.Pattern.make(description, UuidT5Generator.get(namespace, description));
    }
    public static EntityProxy.Concept makeConceptProxy(UUID namespace, String description) {
        return EntityProxy.Concept.make(description, UuidT5Generator.get(namespace, description));
    }
    public static EntityProxy.Concept getSnomedIdentifierConcept(){
        return EntityProxy.Concept.make(PublicIds.of(UUID.fromString(SNOMED_IDENTIFIER_PUBLIC_ID)));
    }
    public static EntityProxy.Concept getRxcuidConcept(){
        return EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RXCUID_IDENTIFIER_PUBLIC_ID)));
    }
    public static EntityProxy.Concept getVuidConcept(){
        return EntityProxy.Concept.make(PublicIds.of(UUID.fromString(VU_IDENTIFIER_PUBLIC_ID)));
    }
    public static EntityProxy.Concept getNdcIdentifierConcept(){
        return EntityProxy.Concept.make(PublicIds.of(UUID.fromString(NDC_IDENTIFIER_PUBLIC_ID)));
    }
    public static EntityProxy.Concept getTallmanSynonymDescriptionConcept(){
        return EntityProxy.Concept.make(PublicIds.of(UUID.fromString(TALLMAN_DESCRIPTION)));
    }
    public static EntityProxy.Pattern getQualitativeDistinctionPattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(QUALITATIVE_DISTINCTION_PATTERN)));
    }
    public static EntityProxy.Pattern getQuantityPattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(QUANTITY_PATTERN)));
    }
    public static EntityProxy.Pattern getSchedulePattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(SCHEDULE_PATTERN)));
    }
    public static EntityProxy.Pattern getHumanDrugPattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(HUMAN_DRUG_PATTERN)));
    }
    public static EntityProxy.Pattern getVetDrugPattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(VET_DRUG_PATTERN)));
    }
    public static EntityProxy.Pattern getTallmanSynonymPattern(){
        return EntityProxy.Pattern.make(PublicIds.of(UUID.fromString(TALLMAN_SYNONYM_PATTERN)));
    }
    public static String transformOwlString(UUID namespace, String owlString) {
        // First, let's handle URIs in the entire string
        Pattern uriPattern = Pattern.compile("<(http://[^>]+)>");
        Matcher matcher = uriPattern.matcher(owlString);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String uri = matcher.group(1);
            String replacement;
            // Process the URI based on its format
            if (uri.startsWith("http://snomed.info/id/")) {
                String id = uri.substring("http://snomed.info/id/".length());
                EntityProxy.Concept concept = makeConceptProxy(namespace, id);
                replacement = ":[" + concept.publicId().asUuidArray()[0] + "]";
            } else if (uri.startsWith("http://mor.nlm.nih.gov/RXNORM/")) {
                // RxNorm ID
                String id = uri.substring("http://mor.nlm.nih.gov/RXNORM/".length());
                EntityProxy.Concept concept = makeConceptProxy(namespace, id);
                replacement = ":[" + concept.publicId().asUuidArray()[0] + "]";
            } else {
                // Unknown URI type, keep as is
                replacement = "<" + uri + ">";
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        // handle the DataHasValue expressions
        String partialResult = result.toString();
        Pattern dataValuePattern = Pattern.compile("DataHasValue\\(:[\\[](.*?)[\\]]\\s+\"([^\"]*)\"\\^\\^xsd:([^\\)]+)\\)|DataHasValue\\(<(http://[^>]+)>\\s+\"([^\"]*)\"\\^\\^xsd:([^\\)]+)\\)");
        matcher = dataValuePattern.matcher(partialResult);
        result = new StringBuffer();

        while (matcher.find()) {
            String replacement;
            if (matcher.group(1) != null) {
                // Already transformed URI
                String conceptId = matcher.group(1);
                String value = matcher.group(2);
                String dataType = matcher.group(3);
                replacement = "DataHasValue(:[" + conceptId + "] \"" + value + "\"^^xsd:" + dataType + ")";
            } else {
                // Original URI format
                String uri = matcher.group(4);
                String value = matcher.group(5);
                String dataType = matcher.group(6);

                if (uri.startsWith("http://snomed.info/id/")) {
                    String id = uri.substring("http://snomed.info/id/".length());
                    EntityProxy.Concept concept = makeConceptProxy(namespace, id);
                    replacement = "DataHasValue(:[" + concept.publicId().asUuidArray()[0] + "] \"" +
                            value + "\"^^xsd:" + dataType + ")";
                } else {
                    // Keep original format if URI type is unknown
                    replacement = "DataHasValue(<" + uri + "> \"" + value + "\"^^xsd:" + dataType + ")";
                }
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        return result.toString();
    }
}
