package dev.ikm.tinkar.rxnorm.integration;

import dev.ikm.maven.RxnormData;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StampPositionRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RxnormConceptSemanticIT extends AbstractIntegrationTest {

    /**
     * Test RxnormConcepts Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertOwlElement().
     */
    @Test
    @Disabled // TODO
    public void testRxnormConceptSemantics() throws IOException {
        String errorFile = "target/failsafe-reports/Rxnorm_Concepts_not_found.txt";
        String absolutePath = rxnormOwlFileName;
        int notFound = processOwlFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Rxnorm Concept semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertOwlElement(RxnormData rxnormData) {
        if (rxnormData.getId() != null) {
            // Generate UUID based on RxNorm ID
            UUID conceptUuid = conceptUuid(rxnormData.getId());
            StateSet state = StateSet.ACTIVE;
            StampPositionRecord stampPosition = StampPositionRecord.make(timeForStamp, TinkarTerm.DEVELOPMENT_PATH.nid());
            StampCalculator stampCalc = StampCoordinateRecord.make(state, stampPosition).stampCalculator();
            ConceptRecord entity = EntityService.get().getEntityFast(conceptUuid);
            Latest<ConceptVersionRecord> latest = stampCalc.latest(entity);

            return latest.isPresent();
        }
        return false;
    }

}
