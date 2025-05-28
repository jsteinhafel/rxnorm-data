package dev.ikm.maven;

import dev.ikm.tinkar.common.id.*;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RxnormUtility {
    public static final String RXNORM_MODULE = "e7ead9cc-3711-4353-92b6-8e5d4f4ec723";
    public static final String RXNORM_AUTHOR = "0d6d7eb2-3d2d-4ea9-bd54-7f33e74ff815";
    public static final String SNOMED_IDENTIFIER_PUBLIC_ID = "35d6b6cb-b927-4e92-a47b-542f972cd692";
    public static final String RXCUID_IDENTIFIER_PUBLIC_ID = "6b3930f9-26e2-48da-97ac-b26a567b570b";
    public static final String NDC_IDENTIFIER_PUBLIC_ID = "d8ab8e6b-5b77-49f9-8db0-1e7343267740";
    public static final String VU_IDENTIFIER_PUBLIC_ID = "e6d07757-5461-4687-a4ad-0a40ce4cc539";
    public static final String TALLMAN_DESCRIPTION = "84785d67-6cca-4caf-bbd0-113c217bb338";
    public static final String QUALITATIVE_DISTINCTION_PATTERN = "df1c8881-19e7-4159-86a0-91dff62a3d46";
    public static final String QUANTITY_PATTERN = "1be49e47-30a0-417c-a254-2a15c7118aea";
    public static final String SCHEDULE_PATTERN = "8a2e0eb3-80bd-4b61-a628-013a0c920f3d";
    public static final String HUMAN_DRUG_PATTERN = "4f875c6a-b679-470d-ab72-9aa640ea9f87";
    public static final String VET_DRUG_PATTERN = "4cbcd401-2edf-4859-8473-ff075b8178d1";
    public static final String TALLMAN_SYNONYM_PATTERN = "2f75c349-ee27-4113-9c58-1b37c76abf62";
    
    public static final String QUALITATIVE_PATTERN_DISTINCTION_MEANING = "f1ac8825-9e73-43c3-9a2d-a4c45acb623f";
    public static final String QUALITATIVE_PATTERN_LANGUAGE_MEANING = "8255b976-1b0b-4fc9-9a46-76c4866b822f";
    
    public static final String QUANTITY_PATTERN_DRUG_MEANING = "70785113-4769-42d1-bf3b-2281318c8870";
    
    public static final String SCHEDULE_PATTERN_DRUG_MEANING = "a5f5e459-cd14-48fe-95e6-ab0dfff323a6";
    public static final String SCHEDULE_PATTERN_LANGUAGE_MEANING = "6017102b-23f5-4d62-ac7c-5827858885b1";
    
    public static final String HUMAN_DRUG_PATTERN_LANGUAGE_MEANING = "e515d52f-831b-4010-8507-9541a14f7b11";
    
    public static final String VETERINARIAN_DRUG_PATTERN_LANGUAGE_MEANING = "84785d67-6cca-4caf-bbd0-113c217bb338";
  
    public static final String TALLMAN_SYNONYM_PATTERN_LANGUAGE_MEANING = "22f120a0-bbe7-4bd8-ac86-8c52a01d3ea6";
    
    private static final Logger LOG = LoggerFactory.getLogger(RxnormUtility.class.getSimpleName());
    

    /**
     *
     * @param file
     * @return Content of file into a String
     * @throws IOException
     */
    public static String readFile(File file) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }

    /**
     * Extracts RxNorm attributes from the OWL content
     */
    public static List<RxnormData> extractRxnormData(String owlContent) {
        List<RxnormData> attributes = new ArrayList<>();

        // Split the content by "# Class:" to identify each concept section
        String[] classBlocks = owlContent.split("# Class: ");

        // Skip the first block as it's before the first "# Class:"
        for (int i = 1; i < classBlocks.length; i++) {
            String block = classBlocks[i];
            // Extract the URI from the first line
            // Format: <http://mor.nlm.nih.gov/RXNORM/996062> (meclizine hydrochloride 25 MG Oral Film)
            Matcher uriMatcher = Pattern.compile("<([^>]+)>").matcher(block);

            if (uriMatcher.find()) {
                String uri = uriMatcher.group(1);
                RxnormData concept = new RxnormData(uri);

                // Extract annotations
                extractAnnotations(block, concept);

                // Extract EquivalentClasses
                extractEquivalentClasses(block, concept);

                attributes.add(concept);
            }
        }

        return attributes;
    }


    /**
     * Extracts EquivalentClasses from a class block
     */
    /**
     * Extracts annotations from a class block
     */
    public static void extractAnnotations(String block, RxnormData data) {
        // Extract RxNorm_Name
        Matcher rxnormNameMatcher = Pattern.compile(":RxNorm_Name <[^>]+> \"([^\"]*)\"").matcher(block);
        if (rxnormNameMatcher.find()) {
            data.setRxnormName(rxnormNameMatcher.group(1));
        }

        // Extract RxNorm_Synonym
        Matcher rxnormSynonymMatcher = Pattern.compile(":RxNorm_Synonym <[^>]+> \"([^\"]*)\"").matcher(block);
        if (rxnormSynonymMatcher.find()) {
            data.setRxnormSynonym(rxnormSynonymMatcher.group(1));
        }

        // Extract Prescribable_Synonym
        Matcher prescribableSynonymMatcher = Pattern.compile(":Prescribable_Synonym <[^>]+> \"([^\"]*)\"").matcher(block);
        if (prescribableSynonymMatcher.find()) {
            data.setPrescribableSynonym(prescribableSynonymMatcher.group(1));
        }

        // Extract SNOMED CT identifier
        Matcher snomedCtMatcher = Pattern.compile("oboInOwl:hasDbXref <[^>]+> \"SNOMEDCT:\\s*([^\"]*)\"").matcher(block);
        if (snomedCtMatcher.find()) {
            data.setSnomedCtId(snomedCtMatcher.group(1));
        }

        // Extract RxCUI identifier
        Matcher rxCuiMatcher = Pattern.compile("oboInOwl:hasDbXref <[^>]+> \"RxCUI:\\s*([^\"]*)\"").matcher(block);
        if (rxCuiMatcher.find()) {
            data.setRxCuiId(rxCuiMatcher.group(1));
        }

        // Extract VUID identifier
        Matcher vuidMatcher = Pattern.compile("oboInOwl:hasDbXref <[^>]+> \"VUID:\\s*([^\"]*)\"").matcher(block);
        if (vuidMatcher.find()) {
            data.setVuidId(vuidMatcher.group(1));
        }

        // Extract NDC codes with their start and end dates
        Matcher ndcMatcher = Pattern.compile("AnnotationAssertion\\(Annotation\\(:endDate \"(\\d+)\"\\) Annotation\\(:startDate \"\\d+\"\\) :ndc <[^>]+> \"([^\"]*)\"\\)").matcher(block);
        while (ndcMatcher.find()) {
            String endDate = ndcMatcher.group(1);
            String ndcCode = ndcMatcher.group(2);
            data.addNdcCodeWithEndDate(ndcCode, endDate);
        }

        // Extract QUALITATIVE_DISTINCTION
        Matcher qualitativeDistinctionMatcher = Pattern.compile(":QUALITATIVE_DISTINCTION <[^>]+> \"([^\"]*)\"").matcher(block);
        if (qualitativeDistinctionMatcher.find()) {
            data.setQualitativeDistinction(qualitativeDistinctionMatcher.group(1));
        }

        // Extract QUANTITY
        Matcher quantityMatcher = Pattern.compile(":QUANTITY <[^>]+> \"([^\"]*)\"").matcher(block);
        if (quantityMatcher.find()) {
            data.setQuantity(quantityMatcher.group(1));
        }

        // Extract SCHEDULE
        Matcher scheduleMatcher = Pattern.compile(":SCHEDULE <[^>]+> \"([^\"]*)\"").matcher(block);
        if (scheduleMatcher.find()) {
            data.setSchedule(scheduleMatcher.group(1));
        }

        // Extract HUMAN_DRUG
        Matcher humanDrugMatcher = Pattern.compile(":HUMAN_DRUG <[^>]+> \"([^\"]*)\"").matcher(block);
        if (humanDrugMatcher.find()) {
            data.setHumanDrug(humanDrugMatcher.group(1));
        }

        // Extract VET_DRUG
        Matcher vetDrugMatcher = Pattern.compile(":VET_DRUG <[^>]+> \"([^\"]*)\"").matcher(block);
        if (vetDrugMatcher.find()) {
            data.setVetDrug(vetDrugMatcher.group(1));
        }

        // Extract TALLMAN_SYNONYM (can have multiple)
        Matcher tallmanSynonymMatcher = Pattern.compile(":Tallman_Synonym <[^>]+> \"([^\"]*)\"").matcher(block);
        while (tallmanSynonymMatcher.find()) {
            data.addTallmanSynonym(tallmanSynonymMatcher.group(1));
        }

    }

    /**
     * Extracts EquivalentClasses from a class block
     */
    public static void extractEquivalentClasses(String block, RxnormData concept) {
        // Extract the entire EquivalentClasses block with nested parentheses
        int startIndex = block.indexOf("EquivalentClasses(");
        if (startIndex != -1) {
            // Find the matching closing parenthesis by counting opening and closing parentheses
            int openParenCount = 1;
            int endIndex = startIndex + "EquivalentClasses(".length();

            while (openParenCount > 0 && endIndex < block.length()) {
                char c = block.charAt(endIndex);
                if (c == '(') {
                    openParenCount++;
                } else if (c == ')') {
                    openParenCount--;
                }
                endIndex++;
            }

            if (openParenCount == 0) {
                // Extract the full block including "EquivalentClasses"
                String fullEquivalentClasses = block.substring(startIndex, endIndex);
                concept.setEquivalentClassesStr(fullEquivalentClasses);
            }
        }
    }

    /**
     * Parses the timestamp from the file name
     * Expected format: something-YYYY-MM-DD-something.owl
     */
    public static long parseTimeFromFileName(String fileName) {
        try {
            // Extract the date portion using regex
            Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
            Matcher matcher = pattern.matcher(fileName);

            if (matcher.find()) {
                String dateStr = matcher.group(1);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.parse(dateStr).getTime();
            } else {
                LOG.warn("Could not extract date from file name: " + fileName);
                return System.currentTimeMillis(); // Fallback to current time
            }
        } catch (ParseException e) {
            LOG.error("Error parsing date from file name: " + fileName, e);
            return System.currentTimeMillis(); // Fallback to current time
        }
    }

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

    public static UUID generateUUID(UUID namespace, String id) {
        return UuidT5Generator.get(namespace, id);
    }
}
