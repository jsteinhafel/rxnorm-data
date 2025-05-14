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
import dev.ikm.tinkar.composer.assembler.SemanticAssembler;
import dev.ikm.tinkar.composer.template.AxiomSyntax;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_PATTERN;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.IDENTIFIER_PATTERN;
import static dev.ikm.tinkar.terms.TinkarTerm.DEVELOPMENT_PATH;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.IDENTIFIER_PATTERN;
import static dev.ikm.tinkar.terms.TinkarTerm.PREFERRED;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;

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
        this.rxnormModule = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("ae4818f8-d523-48e8-abf9-099237ae01ab")));
        this.rxnormAuthor = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("65f596bd-3cf8-4818-84ea-740500267818")));

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


    /**
     * Process OWL file and Creates Concepts for each Class
     */
    private void createConcepts(Composer composer){
        LOG.info("Starting to create concepts from RxNorm OWL file...");

        // Parse the date from the filename
        String fileName = rxnormOwl.getName();
        long timeForStamp = RxnormUtility.parseTimeFromFileName(fileName);

        try {
            String owlContent = RxnormUtility.readFile(rxnormOwl);

            // Process the OWL content to extract class declarations and annotations
            List<RxnormData> rxnormConcepts = RxnormUtility.extractRxnormData(owlContent);

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
            LOG.warn("Could not extract RxNorm ID" + rxnormId);
            return;
        }

        // Generate UUID based on RxNorm ID
        UUID conceptUuid = UuidT5Generator.get(namespace, rxnormId);

        // Create session with Active state, RxNorm Author, RxNorm Module, and MasterPath
        Session session = composer.open(State.ACTIVE, time, rxnormAuthor, rxnormModule, DEVELOPMENT_PATH);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

            session.compose((ConceptAssembler assembler) -> {
                assembler.concept(concept);
            });

            createDescriptionSemantic(session, concept, rxnormData);
            createIdentifierSemantic(composer, session, concept, rxnormData, time);
            if(!rxnormData.getEquivalentClassesStr().isEmpty()) {
                createStatedDefinitionSemantics(session, concept, rxnormData);
            }
            createPatternSemantics(session, concept, rxnormData);
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
        try {
            if(!rxnormData.getRxnormName().isEmpty()) {
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxnormName() + "DESC")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(ENGLISH_LANGUAGE)
                                .with(rxnormData.getRxnormName())
                                .with(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .with(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE)
                        ));
            }

            if(!rxnormData.getRxnormSynonym().isEmpty()){
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxnormSynonym() + "SDESC")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(ENGLISH_LANGUAGE)
                                .with(rxnormData.getRxnormSynonym())
                                .with(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .with(REGULAR_NAME_DESCRIPTION_TYPE)
                        ));
            }
            if(!rxnormData.getPrescribableSynonym().isEmpty()){
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getPrescribableSynonym() + "PSDESC")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(ENGLISH_LANGUAGE)
                                .with(rxnormData.getPrescribableSynonym())
                                .with(DESCRIPTION_NOT_CASE_SENSITIVE)
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
    private void createIdentifierSemantic(Composer composer, Session session, EntityProxy.Concept concept, RxnormData rxnormData, long time) {
        try {

            if(!rxnormData.getSnomedCtId().isEmpty()) {
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getSnomedCtId() + "SNOMEDID")));
                EntityProxy.Concept snomedIdentifier = RxnormUtility.getSnomedIdentifierConcept();
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(snomedIdentifier)
                                .with(rxnormData.getSnomedCtId())
                        ));
            }

            if(!rxnormData.getRxCuiId().isEmpty()){
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxCuiId() + "RXID")));
                EntityProxy.Concept rxnormIdentifier = RxnormUtility.getRxcuidConcept();
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(rxnormIdentifier)
                                .with(rxnormData.getRxCuiId())
                        ));
            }
            if(!rxnormData.getVuidId().isEmpty()){
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getVuidId() + "VUID")));
                EntityProxy.Concept vhIdentifier = RxnormUtility.getVuidConcept();
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(vhIdentifier)
                                .with(rxnormData.getVuidId())
                        ));
            }
            if(!rxnormData.getNdcCodesWithEndDates().isEmpty()){
                EntityProxy.Concept ndcIdentifier = RxnormUtility.getNdcIdentifierConcept();
                String fileDate = new SimpleDateFormat("yyyyMM").format(new Date(time));

                for (Map.Entry<String, String> entry : rxnormData.getNdcCodesWithEndDates().entrySet()) {
                    String ndcCode = entry.getKey();
                    String endDate = entry.getValue();

                    // Determine status based on end date
                    State state = State.ACTIVE;
                    if (endDate.compareTo(fileDate) < 0) {
                        state = State.INACTIVE;
                    }

                    EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                            PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + ndcCode + "NDCID")));

                    Session ndcSession = composer.open(state, time, rxnormAuthor, rxnormModule, DEVELOPMENT_PATH);
                    ndcSession.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                            .semantic(semantic)
                            .pattern(IDENTIFIER_PATTERN)
                            .reference(concept)
                            .fieldValues(fieldValues -> fieldValues
                                    .with(ndcIdentifier)
                                    .with(ndcCode)
                            ));
                }
            }

        } catch (Exception e) {
            LOG.error("Error creating description semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a stated definition semantic that attaches the respective Owl String to the semantic
     */
    private void createStatedDefinitionSemantics(Session session, EntityProxy.Concept concept, RxnormData rxnormData) {
        String owlExpression = RxnormUtility.transformOwlString(namespace, rxnormData.getEquivalentClassesStr());
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

    private void createPatternSemantics(Session session, EntityProxy.Concept concept, RxnormData rxnormData) {
        try {
            if(!rxnormData.getQualitativeDistinction().isEmpty()) {
                EntityProxy.Pattern qualitativeDistinctionPattern = RxnormUtility.getQualitativeDistinctionPattern();
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "QD")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                            .semantic(semantic)
                            .reference(concept)
                            .pattern(qualitativeDistinctionPattern)
                            .fieldValues(fv -> fv
                                    .with(rxnormData.getQualitativeDistinction())
                                    .with(ENGLISH_LANGUAGE)
                            ));
            }

            if(!rxnormData.getQuantity().isEmpty()) {
                EntityProxy.Pattern quantityPattern = RxnormUtility.getQuantityPattern();
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "QUANTITY")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .reference(concept)
                        .pattern(quantityPattern)
                        .fieldValues(fv -> fv.with(rxnormData.getQuantity())
                        ));
            }

            if(!rxnormData.getSchedule().isEmpty()) {
                EntityProxy.Pattern schedulePattern = RxnormUtility.getSchedulePattern();
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "SCHEDULE")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .reference(concept)
                        .pattern(schedulePattern)
                        .fieldValues(fv -> fv
                                .with(rxnormData.getSchedule())
                                .with(ENGLISH_LANGUAGE)
                        ));
            }

            if(!rxnormData.getHumanDrug().isEmpty()) {
                EntityProxy.Pattern humanDrugPattern = RxnormUtility.getHumanDrugPattern();
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "HD")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .reference(concept)
                        .pattern(humanDrugPattern)
                        .fieldValues(fv -> fv.with(rxnormData.getHumanDrug())
                        ));
            }

            if(!rxnormData.getVetDrug().isEmpty()) {
                EntityProxy.Pattern vetDrugPattern = RxnormUtility.getVetDrugPattern();
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "VD")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(semantic)
                        .reference(concept)
                        .pattern(vetDrugPattern)
                        .fieldValues(fv -> fv.with(rxnormData.getVetDrug())
                        ));
            }
             createTallmanSynonymPattern(session, concept, rxnormData);
        } catch (Exception e) {
            LOG.error("Error creating pattern semantic for concept: " + concept, e);
        }
    }

    private void createTallmanSynonymPattern(Session session, EntityProxy.Concept concept, RxnormData rxnormData){
        if(!rxnormData.getTallmanSynonyms().isEmpty()) {
            EntityProxy.Concept tallmanSynonymDescriptionConcept = RxnormUtility.getTallmanSynonymDescriptionConcept();
            rxnormData.getTallmanSynonyms().forEach(synonym -> {
                EntityProxy.Semantic descSemantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + synonym + "TSDESC")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(descSemantic)
                        .pattern(DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fieldValues -> fieldValues
                                .with(ENGLISH_LANGUAGE)
                                .with(synonym)
                                .with(DESCRIPTION_CASE_SENSITIVE)
                                .with(REGULAR_NAME_DESCRIPTION_TYPE)
                        ));

                EntityProxy.Pattern tallmanSynonymPattern = RxnormUtility.getTallmanSynonymPattern();
                EntityProxy.Semantic patternSemantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getQualitativeDistinction() + "VD")));
                session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                        .semantic(patternSemantic)
                        .reference(descSemantic)
                        .pattern(tallmanSynonymPattern)
                        .fieldValues(fv -> fv
                                .with(PREFERRED)
                        ));
            });
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
    private void extractEquivalentClasses(String block, RxnormData concept) {
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
        private Map<String, String> ndcCodesWithEndDates = new HashMap<>();
        private String qualitativeDistinction = "";
        private String quantity = "";
        private String schedule = "";
        private String humanDrug = "";
        private String vetDrug = "";
        private List<String> tallmanSynonyms = new ArrayList<>();
        private String equivalentClassesStr = "";

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

        public void setQualitativeDistinction(String qualitativeDistinction) {
            this.qualitativeDistinction = qualitativeDistinction;
        }

        public void setQuantity(String quantity) {
            this.quantity = quantity;
        }

        public void setSchedule(String schedule) {
            this.schedule = schedule;
        }

        public void setHumanDrug(String humanDrug) {
            this.humanDrug = humanDrug;
        }

        public void setVetDrug(String vetDrug) {
            this.vetDrug = vetDrug;
        }

        public void addTallmanSynonym(String tallmanSynonym) {
            this.tallmanSynonyms.add(tallmanSynonym);
        }

        public void setEquivalentClassesStr(String equivalentClassesStr) {
            this.equivalentClassesStr = equivalentClassesStr;
        }
        public void addNdcCodeWithEndDate(String ndcCode, String endDate) {
            this.ndcCodes.add(ndcCode); // Keep original list for backward compatibility
            this.ndcCodesWithEndDates.put(ndcCode, endDate);
        }

        public Map<String, String> getNdcCodesWithEndDates() {
            return ndcCodesWithEndDates;
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

        public String getQualitativeDistinction(){
            return qualitativeDistinction;
        }

        public String getQuantity(){
            return quantity;
        }

        public String getSchedule(){
            return schedule;
        }

        public String getHumanDrug() {
            return  humanDrug;
        }

        public String getVetDrug() {
            return vetDrug;
        }

        public List<String> getTallmanSynonyms(){
            return tallmanSynonyms;
        }

        public String getEquivalentClassesStr() {
            return equivalentClassesStr;
        }
    }
}
