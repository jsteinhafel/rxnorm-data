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
import dev.ikm.tinkar.terms.TinkarTerm;
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

import static dev.ikm.tinkar.terms.TinkarTerm.DEVELOPMENT_PATH;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.IDENTIFIER_PATTERN;
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
            LOG.warn("Could not extract RxNorm ID");
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
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxnormSynonym() + "SDESC")));
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
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getPrescribableSynonym() + "PSDESC")));
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
    private void createIdentifierSemantic(Composer composer, Session session, EntityProxy.Concept concept, RxnormData rxnormData, long time) {
        try {

            if(!rxnormData.getSnomedCtId().isEmpty()) {
                EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getSnomedCtId() + "ID")));
                EntityProxy.Concept snomedIdentifier = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("ed73f32d-c068-43f8-9767-ace6dfee44db")));
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
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getRxCuiId() + "ID")));
                EntityProxy.Concept rxnormIdentifier = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("e409e4ce-4527-49a0-bdc6-f4fe79c21088")));
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
                        PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + rxnormData.getVuidId() + "ID")));
                EntityProxy.Concept vhIdentifier = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("5d06759b-56f7-4c24-be70-5cea09e0e130")));
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
                EntityProxy.Concept ndcIdentifier = EntityProxy.Concept.make(PublicIds.of(UUID.fromString("88f22a11-3715-4d58-8cb4-aa14d49a6b35")));
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
                            PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + ndcCode + "ID")));

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

}
