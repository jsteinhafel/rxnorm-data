package dev.ikm.maven;

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
    }


    /**
     * Extracts EquivalentClasses from a class block
     */
    public static void extractEquivalentClasses(String block, RxnormData concept) {
        // Extract EquivalentClasses
        // Format: EquivalentClasses(<http://mor.nlm.nih.gov/RXNORM/996062> ObjectIntersectionOf(...))
        Matcher equivClassesMatcher = Pattern.compile("EquivalentClasses\\(<[^>]+> ([^)]+)\\)").matcher(block);
        if (equivClassesMatcher.find()) {
            String equivalentClasses = equivClassesMatcher.group(1);
            concept.setEquivalentClassesStr(equivalentClasses);
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

    public static UUID generateUUID(UUID namespace, String id) {
        return UuidT5Generator.get(namespace, id);
    }
}
