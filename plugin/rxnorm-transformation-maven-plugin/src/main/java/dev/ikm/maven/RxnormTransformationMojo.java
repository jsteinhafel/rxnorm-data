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
import dev.ikm.tinkar.composer.assembler.SemanticAssembler;
import dev.ikm.tinkar.composer.template.AxiomSyntax;
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

import static dev.ikm.tinkar.terms.TinkarTerm.*;

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
    private EntityProxy.Concept rxnormAuthor;

    private final String rxnormModuleStr = "RxNorm Module";
    private EntityProxy.Concept rxnormModule;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        LOG.info("########## Rxnorm Transformer Starting...");

        this.namespace = UUID.fromString(namespaceString);
        File datastore = new File(datastorePath);
        this.rxnormModule = RxnormUtility.makeConceptProxy(namespace, rxnormModuleStr);
        this.rxnormAuthor = RxnormUtility.makeConceptProxy(namespace, rxnormAuthorStr);

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
        String vetDrugIdentifierStr = "Veterinarian Drug Identifier";
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

        try {
            String owlContent = readFile(rxnormOwl);

            // Process the OWL content to extract class declarations and annotations
            List<RxnormData> rxnormConcepts = extractRxnormData(owlContent);

            LOG.info("Found " + rxnormConcepts.size() + " class declarations in the OWL file");

            // Create concepts for each class
            for (RxnormData rxnormConcept : rxnormConcepts) {
                createRxnormConcept(rxnormConcept, timeForStamp, composer);
            }

            LOG.info("Completed creating RxNorm concepts");

        } catch (Exception e) {
            LOG.error("Error processing RxNorm OWL file", e);
        }
    }

    /**
     * Creates a RxNorm concept from a class ID
     */
    private void createRxnormConcept(RxnormData rxnormData, long time, Composer composer) {
        String rxnormId = rxnormData.getId();

        if (rxnormId == null || rxnormId.isEmpty()) {
            LOG.warn("Could not extract RxNorm ID");
            return;
        }

        // Generate UUID based on RxNorm ID
        UUID conceptUuid = UuidT5Generator.get(namespace, rxnormId);

        // Create a session with Active state, RxNorm Author, RxNorm Module, and MasterPath
        Session session = composer.open(State.ACTIVE, time, rxnormAuthor, rxnormModule, TinkarTerm.MASTER_PATH);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

            session.compose((ConceptAssembler assembler) -> {
                assembler.concept(concept);
            });

            createDescriptionSemantic(session, concept, rxnormData);
            createIdentifierSemantic(session, concept, rxnormData);
            createStatedDefinitionSemantics(session, concept, rxnormData);

            LOG.info("Created concept for RxNorm ID: " + rxnormId);

        } catch (Exception e) {
            LOG.error("Error creating concept for RxNorm ID: " + rxnormId, e);
        }
    }

    /**
     * Creates a description semantic with the specified description type.
     *
     * @param session The current session
     * @param concept The concept to attach the description to
     * @param rxnormData contains fqn, and synonyms
     */
    private void createDescriptionSemantic(Session session, EntityProxy.Concept concept, RxnormData rxnormData) {
        EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxnormName() + "DESC")));
        LOG.info("RxNorm Name: " + rxnormData.getRxnormName());
        LOG.info("RxNorm Synonym: " + rxnormData.getRxnormSynonym());
        LOG.info("RxNorm Prescribable Synonym: " + rxnormData.getPrescribableSynonym());
        try {

            if(!rxnormData.getRxnormName().isEmpty()) {
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(TinkarTerm.DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(TinkarTerm.ENGLISH_LANGUAGE)
                                .with(rxnormData.getRxnormName())
                                .with(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                                .with(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE)
                        ));
            }

            if(!rxnormData.getRxnormSynonym().isEmpty()){
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(TinkarTerm.DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(TinkarTerm.ENGLISH_LANGUAGE)
                                .with(rxnormData.getRxnormSynonym())
                                .with(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                                .with(REGULAR_NAME_DESCRIPTION_TYPE)
                        ));
            }
            if(!rxnormData.getPrescribableSynonym().isEmpty()){
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(TinkarTerm.DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(TinkarTerm.ENGLISH_LANGUAGE)
                                .with(rxnormData.getPrescribableSynonym())
                                .with(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                                .with(REGULAR_NAME_DESCRIPTION_TYPE)
                        ));
            }
        } catch (Exception e) {
            LOG.error("Error creating description semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a description semantic with the specified description type.
     *
     * @param session The current session
     * @param concept The concept to attach the description to
     * @param rxnormData contains necessary ids for identification
     */
    private void createIdentifierSemantic(Session session, EntityProxy.Concept concept, RxnormData rxnormData) {
        EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getSnomedCtId() + "ID")));

        try {

            if(!rxnormData.getSnomedCtId().isEmpty()) {
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(IDENTIFIER_SOURCE)
                                .with(rxnormData.getSnomedCtId())
                        ));
            }

            if(!rxnormData.getRxCuiId().isEmpty()){
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(IDENTIFIER_SOURCE)
                                .with(rxnormData.getRxCuiId())
                        ));
            }
            if(!rxnormData.getVuidId().isEmpty()){
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(IDENTIFIER_SOURCE)
                                .with(rxnormData.getVuidId())
                        ));
            }
            // TODO: need to add stamp active or in active.....
            if(!rxnormData.getNdcCodes().isEmpty()){
                rxnormData.getNdcCodes().forEach(ndcCode -> {
                    session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                            .semantic(semantic)
                            .pattern(IDENTIFIER_PATTERN)
                            .reference(concept)
                            .fieldValues(fieldValues -> fieldValues
                                    .with(IDENTIFIER_SOURCE)
                                    .with(ndcCode)
                            ));
                });
            }

        } catch (Exception e) {
            LOG.error("Error creating description semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a stated definition semantic that attaches the respective Owl String to the semantic
     */
    private void createStatedDefinitionSemantics(Session session, EntityProxy.Concept concept, RxnormData rxnormData) {
        String owlExpression = rxnormData.getEquivalentClassesStr();
        EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getEquivalentClassesStr() + "AXIOM")));
        try {
            session.compose(new AxiomSyntax()
                            .semantic(axiomSemantic)
                            .text(owlExpression),
                    concept);
        } catch (Exception e) {
            LOG.error("Error creating state definition semantic for concept: " + concept, e);
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
     * Extracts RxNorm attributes from the OWL content
     */
    private List<RxnormData> extractRxnormData(String owlContent) {
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
    private void extractAnnotations(String block, RxnormData data) {
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

        // Extract NDC codes
        Matcher ndcMatcher = Pattern.compile(":ndc <[^>]+> \"([^\"]*)\"").matcher(block);
        while (ndcMatcher.find()) {
            data.addNdcCode(ndcMatcher.group(1));
        }
    }


    /**
     * Extracts EquivalentClasses from a class block
     */
    private void extractEquivalentClasses(String block, RxnormData concept) {
        // Extract EquivalentClasses
        // Format: EquivalentClasses(<http://mor.nlm.nih.gov/RXNORM/996062> ObjectIntersectionOf(...))
        Matcher equivClassesMatcher = Pattern.compile("EquivalentClasses\\(<[^>]+> ([^)]+)\\)").matcher(block);
        if (equivClassesMatcher.find()) {
            String equivalentClasses = equivClassesMatcher.group(1);
            concept.setEquivalentClassesStr(equivalentClasses);
        }
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

    /**
     * Helper class to store RxNorm concept data
     */
    private static class RxnormData {
        private String id;
        private String uri;

        private String rxnormName = "";
        private String rxnormSynonym = "";
        private String prescribableSynonym = "";

        private String snomedCtId = "";
        private String rxCuiId = "";
        private String vuidId = "";
        private List<String> ndcCodes = new ArrayList<>();
        private String equivalentClassesStr;

        public RxnormData(String uri) {
            this.uri = uri;
            if (uri.startsWith("http://mor.nlm.nih.gov/RXNORM/")) {
                this.id = uri.substring("http://mor.nlm.nih.gov/RXNORM/".length());
            }
        }

        public String getId() {
            return id;
        }

        public void setRxnormName(String rxnormName) {
            this.rxnormName = rxnormName;
        }

        public void setRxnormSynonym(String rxnormSynonym) {
            this.rxnormSynonym = rxnormSynonym;
        }

        public void setPrescribableSynonym(String prescribableSynonym) {
            this.prescribableSynonym = prescribableSynonym;
        }

        public void setSnomedCtId(String snomedCtId) {
            this.snomedCtId = snomedCtId;
        }

        public void setRxCuiId(String rxCuiId) {
            this.rxCuiId = rxCuiId;
        }

        public void setVuidId(String vuidId) {
            this.vuidId = vuidId;
        }

        public void addNdcCode(String ndcCode) {
            this.ndcCodes.add(ndcCode);
        }

        public void setEquivalentClassesStr(String equivalentClassesStr) {
            this.equivalentClassesStr = equivalentClassesStr;
        }

        public String getRxnormName() {
            return rxnormName;
        }

        public String getRxnormSynonym() {
            return rxnormSynonym;
        }

        public String getPrescribableSynonym() {
            return prescribableSynonym;
        }

        public String getSnomedCtId() {
            return snomedCtId;
        }

        public String getRxCuiId() {
            return rxCuiId;
        }

        public String getVuidId() {
            return vuidId;
        }

        public List<String> getNdcCodes() {
            return ndcCodes;
        }

        public String getEquivalentClassesStr() {
            return equivalentClassesStr;
        }
    }
}
