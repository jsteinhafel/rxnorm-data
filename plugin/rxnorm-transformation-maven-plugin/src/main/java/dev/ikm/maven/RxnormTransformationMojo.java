package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.assembler.PatternAssembler;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static dev.ikm.tinkar.terms.TinkarTerm.LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.STRING;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.COMPONENT_FIELD;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;

@Mojo(name = "run-rxnorm-transformation", defaultPhase = LifecyclePhase.INSTALL)
public class RxnormTransformationMojo extends AbstractMojo {
    private static final Logger LOG = LoggerFactory.getLogger(RxnormTransformationMojo.class.getSimpleName());

    @Parameter(property = "origin.namespace", required = true)
    String namespaceString;

    @Parameter(property = "rxnormOwlFile", required = true)
    private File rxnormOwl;

    @Parameter(property = "datastorePath", required = true)
    private String datastorePath;

    @Parameter(property = "inputDirectoryPath", required = true)
    private String inputDirectoryPath;

    @Parameter(property = "dataOutputPath", required = true)
    private String dataOutputPath;

    @Parameter(property = "controllerName", defaultValue = "Open SpinedArrayStore")
    private String controllerName;
    private UUID namespace;
    private final String rxnormAuthorStr = "RxNorm Author";
    private final EntityProxy.Concept rxnormAuthor = RxnormUtility.makeConceptProxy(namespace, rxnormAuthorStr);

    private final String rxnormModuleStr = "RxNorm Module";
    private final EntityProxy.Concept rxnormModule = RxnormUtility.makeConceptProxy(namespace, rxnormModuleStr);

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        LOG.info("########## Rxnorm Transformer Starting...");

        this.namespace = UUID.fromString(namespaceString);
        File datastore = new File(datastorePath);

//        try {
//            unzipRawData(inputDirectoryPath);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        initializeDatastore(datastore);
        EntityService.get().beginLoadPhase();

        try {
            Composer composer = new Composer("Rxnorm Transformer Composer");

            try {
                LOG.info("Creating Patterns...");
                createPatterns(composer);
                LOG.info("Starting rxnorm owl file processing...");
                createConcepts(composer);
            } catch (Exception e) {
                LOG.error("Error during data processing", e);
            }
            LOG.info("Committing all sessions...");
            composer.commitAllSessions();
            LOG.info("Sessions committed successfully");
        } finally {
            EntityService.get().endLoadPhase();
            PrimitiveData.stop();
            LOG.info("########## Rxnorm Transformation Completed.");
        }
    }

    private void createPatterns(Composer composer){
        Session session = composer.open(State.ACTIVE, rxnormAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);

        String qualitativeDistinctionSemanticStr = "Qualitative Distinction Semantic";
        EntityProxy.Concept qualitativeDistinctionSemantic = RxnormUtility.makeConceptProxy(namespace, qualitativeDistinctionSemanticStr);
        String textForQualitativeDistinctionStr = "Text for Qualitative Distinction";
        EntityProxy.Concept textForQualitativeDistinction = RxnormUtility.makeConceptProxy(namespace, textForQualitativeDistinctionStr);
        String drugQualitativeDistinctionStr = "Drug Qualitative Distinction";
        EntityProxy.Concept drugQualitativeDistinction = RxnormUtility.makeConceptProxy(namespace, drugQualitativeDistinctionStr);
        String languageForQualitativeDistinctionStr = "Language For Qualitative Distinction";
        EntityProxy.Concept languageForQualitativeDistinction = RxnormUtility.makeConceptProxy(namespace, languageForQualitativeDistinctionStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(RxnormUtility.makePatternProxy(namespace, "Qualitative Distinction"))
                        .meaning(qualitativeDistinctionSemantic)
                        .purpose(qualitativeDistinctionSemantic)
                        .fieldDefinition(textForQualitativeDistinction, drugQualitativeDistinction, STRING)
                        .fieldDefinition(languageForQualitativeDistinction, LANGUAGE, COMPONENT_FIELD))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Qualitative Distinction Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Qualitative Distinction Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String quantitySemanticStr = "Quantity Semantic";
        EntityProxy.Concept quantitySemantic = RxnormUtility.makeConceptProxy(namespace, quantitySemanticStr);
        String textForQuantityStr = "Text for Quantity";
        EntityProxy.Concept textForQuantity = RxnormUtility.makeConceptProxy(namespace, textForQuantityStr);
        String drugQuantityStr = "Drug Quantity";
        EntityProxy.Concept drugQuantity = RxnormUtility.makeConceptProxy(namespace, drugQuantityStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(RxnormUtility.makePatternProxy(namespace, "Quantity"))
                        .meaning(quantitySemantic)
                        .purpose(quantitySemantic)
                        .fieldDefinition(textForQuantity, drugQuantity, STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Quantity Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Quantity Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String scheduleSemanticStr = "Schedule Semantic";
        EntityProxy.Concept scheduleSemantic = RxnormUtility.makeConceptProxy(namespace, scheduleSemanticStr);
        String textForScheduleStr = "Text for Schedule";
        EntityProxy.Concept textForSchedule = RxnormUtility.makeConceptProxy(namespace, textForScheduleStr);
        String drugScheduleStr = "Drug Schedule";
        EntityProxy.Concept drugSchedule = RxnormUtility.makeConceptProxy(namespace, drugScheduleStr);
        String languageForScheduleStr = "Language For Schedule";
        EntityProxy.Concept languageForSchedule = RxnormUtility.makeConceptProxy(namespace, languageForScheduleStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(RxnormUtility.makePatternProxy(namespace, "Schedule"))
                        .meaning(scheduleSemantic)
                        .purpose(scheduleSemantic)
                        .fieldDefinition(textForSchedule, drugSchedule, STRING)
                        .fieldDefinition(languageForSchedule, LANGUAGE, COMPONENT_FIELD))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Quantity Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Quantity Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String humanDrugSemanticStr = "Human Drug Semantic";
        EntityProxy.Concept humanDrugSemantic = RxnormUtility.makeConceptProxy(namespace, humanDrugSemanticStr);
        String humanDrugIdentifierStr = "Human Drug Identifier";
        EntityProxy.Concept humanDrugIdentifier = RxnormUtility.makeConceptProxy(namespace, humanDrugIdentifierStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(RxnormUtility.makePatternProxy(namespace, "Human Drug"))
                        .meaning(humanDrugSemantic)
                        .purpose(humanDrugSemantic)
                        .fieldDefinition(humanDrugIdentifier, humanDrugIdentifier, COMPONENT_FIELD))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Human Drug Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Human Drug Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String vetDrugSemanticStr = "Veterinarian Drug Semantic";
        EntityProxy.Concept vetDrugSemantic = RxnormUtility.makeConceptProxy(namespace, vetDrugSemanticStr);
        String vetDrugIdentifierStr = "Human Drug Identifier";
        EntityProxy.Concept vetDrugIdentifier = RxnormUtility.makeConceptProxy(namespace, vetDrugIdentifierStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(RxnormUtility.makePatternProxy(namespace, "Human Drug"))
                        .meaning(vetDrugSemantic)
                        .purpose(vetDrugSemantic)
                        .fieldDefinition(vetDrugIdentifier, vetDrugIdentifier, COMPONENT_FIELD))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Human Drug Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Human Drug Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));
    }

    private void createConcepts(Composer composer){
        LOG.info("Starting to create concepts from RxNorm OWL file...");

        // Parse the date from the filename
        String fileName = rxnormOwl.getName();
        long timeForStamp = parseTimeFromFileName(fileName);

        // Create module concept if not already existing

        try {
            String owlContent = readFile(rxnormOwl);

            // Process the OWL content to extract class declarations
            List<String> classIds = extractClassIds(owlContent);

            LOG.info("Found " + classIds.size() + " class declarations in the OWL file");

            // Create concepts for each class
            for (String classId : classIds) {
                createRxnormConcept(classId, timeForStamp, composer);
            }

            LOG.info("Completed creating RxNorm concepts");

        } catch (Exception e) {
            LOG.error("Error processing RxNorm OWL file", e);
        }
    }

    /**
     * Creates a RxNorm concept from a class ID
     */
    private void createRxnormConcept(String classId, long time, Composer composer) {
        // Extract the RxNorm identifier from the class ID
        // Example: http://mor.nlm.nih.gov/RXNORM/1000001 -> 1000001
        String rxnormId = extractRxnormId(classId);

        if (rxnormId == null || rxnormId.isEmpty()) {
            LOG.warn("Could not extract RxNorm ID from: " + classId);
            return;
        }

        // Generate UUID based on RxNorm ID
        UUID conceptUuid = UuidT5Generator.get(namespace, rxnormId);

        // Create a session with Active state, RxNorm Author, RxNorm Module, and MasterPath
        Session session = composer.open(State.ACTIVE, time, rxnormAuthor, rxnormModule, TinkarTerm.MASTER_PATH);

        try {
            // Create the concept
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

            session.compose((ConceptAssembler assembler) -> {
                assembler.concept(concept);
            });

            LOG.debug("Created concept for RxNorm ID: " + rxnormId);
        } catch (Exception e) {
            LOG.error("Error creating concept for RxNorm ID: " + rxnormId, e);
        }
    }


    private void unzipRawData(String zipFilePath) throws IOException {
        File outputDirectory = new File(dataOutputPath);
        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                File newFile = new File(outputDirectory, zipEntry.getName());
                if(zipEntry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    try(FileOutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer,0,len);
                        }
                    }
                }
                zis.closeEntry();
            }
        }
        searchDataFolder(outputDirectory);
    }

    private File searchDataFolder(File dir) {
        if (dir.isDirectory()){
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().equals("Part.csv")) {
                        rxnormOwl = file;
                    }
                    File found = searchDataFolder(file);
                    if (found != null) {
                        return found;
                    }
                }
            }
        }
        return null;
    }

    private void initializeDatastore(File datastore){
        CachingService.clearAll();
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, datastore);
        PrimitiveData.selectControllerByName(controllerName);
        PrimitiveData.start();
    }

    /**
     * Extracts the RxNorm ID from a class URI
     */
    private String extractRxnormId(String classUri) {
        // Parse the URI to extract the ID part
        // Format: http://mor.nlm.nih.gov/RXNORM/1000001
        if (classUri.startsWith("http://mor.nlm.nih.gov/RXNORM/")) {
            return classUri.substring("http://mor.nlm.nih.gov/RXNORM/".length());
        }
        return null;
    }

    /**
     * Extracts class IDs from the OWL content
     */
    private List<String> extractClassIds(String owlContent) {
        List<String> classIds = new ArrayList<>();

        // Regular expression to match class declarations
        // Example: Declaration(Class(<http://mor.nlm.nih.gov/RXNORM/1000001>))
        Pattern pattern = Pattern.compile("Declaration\\(Class\\(<(http://mor\\.nlm\\.nih\\.gov/RXNORM/[^>]+)>\\)\\)");
        Matcher matcher = pattern.matcher(owlContent);

        while (matcher.find()) {
            classIds.add(matcher.group(1));
        }

        return classIds;
    }

    private String readFile(File file) throws IOException {
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
     * Parses the timestamp from the file name
     * Expected format: something-YYYY-MM-DD-something.owl
     */
    private long parseTimeFromFileName(String fileName) {
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
}
