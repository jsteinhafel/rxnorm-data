package dev.ikm.tinkar.rxnorm.integration;

import dev.ikm.maven.RxnormData;
import dev.ikm.maven.RxnormUtility;
import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class AbstractIntegrationTest {
    Logger LOG = LoggerFactory.getLogger(AbstractIntegrationTest.class);
    static String namespaceString;
    static String rxnormOwlFileName;
    static long timeForStamp;

    @AfterAll
    public static void shutdown() {
        PrimitiveData.stop();
    }

    @BeforeAll
    public static void setup() throws IOException {
        CachingService.clearAll();
        //Note. Dataset needed to be generated within repo, with command 'mvn clean install'
        namespaceString = System.getProperty("origin.namespace"); // property set in pom.xml
        File datastore = new File(System.getProperty("datastorePath")); // property set in pom.xml
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, datastore);
        PrimitiveData.selectControllerByName("Open SpinedArrayStore");
        PrimitiveData.start();
        rxnormOwlFileName = System.getProperty("source.zip"); // property set in pom.xml
        timeForStamp = RxnormUtility.parseTimeFromFileName(rxnormOwlFileName);
    }

    /**
     * Find FilePath
     *
     * @param baseDir
     * @param fileKeyword
     * @return absolutePath
     * @throws IOException
     */
    protected static String findFilePath(String baseDir, String fileKeyword) throws IOException {

        try (Stream<Path> dirStream = Files.walk(Paths.get(baseDir))) {
            Path targetDir = dirStream.filter(Files::isDirectory)
//                    .filter(path -> path.toFile().getAbsoluteFile().toString().toLowerCase().contains(dirKeyword.toLowerCase()))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Target DIRECTORY not found"));

            try (Stream<Path> fileStream = Files.walk(targetDir)) {
                Path targetFile = fileStream.filter(Files::isRegularFile)
                        .filter(path -> path.toFile().getAbsoluteFile().toString().toLowerCase().contains(fileKeyword.toLowerCase()))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Target FILE not found for: " + fileKeyword));

                return targetFile.toAbsolutePath().toString();
            }
        }

    }

    /**
     * Process sourceFilePath
     *
     * @param sourceFilePath
     * @param errorFile
     * @return File status, either Found/NotFound
     * @throws IOException
     */
    protected int processOwlFile(String sourceFilePath, String errorFile) throws IOException {
        int notFound = 0;
        String owlContent = readFile(sourceFilePath);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(errorFile))) {
            // Process the OWL content to extract class declarations and annotations
            List<RxnormData> rxnormConcepts = RxnormUtility.extractRxnormData(owlContent);
            LOG.info("Found " + rxnormConcepts.size() + " class declarations in the OWL file");

            // Compare concepts for each class
            for (RxnormData rxnormConcept : rxnormConcepts) {
                if (!assertOwlElement(rxnormConcept)) {
                    notFound++;
                    LOG.info("Element rxnormConcept NOT Found: " + rxnormConcept.toString());
                    bw.write(rxnormConcept.toString());
                    bw.newLine();
                }
            }
        }

        LOG.info("We found file: " + sourceFilePath);
        LOG.info("RxNormConcepts Not Found: " + notFound);
        return notFound;
    }

    /**
     *
     * @param fileName
     * @return Content of file into a String
     * @throws IOException
     */
    private String readFile(String fileName) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }

    protected UUID uuid(String id) {
        return RxnormUtility.generateUUID(UUID.fromString(namespaceString), id);
    }

    protected abstract boolean assertOwlElement(RxnormData rxnormData);
}
