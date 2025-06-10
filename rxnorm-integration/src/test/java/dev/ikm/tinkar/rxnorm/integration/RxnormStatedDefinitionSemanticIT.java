package dev.ikm.tinkar.rxnorm.integration;

import dev.ikm.maven.RxnormData;
import dev.ikm.maven.RxnormUtility;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.PatternEntityVersion;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RxnormStatedDefinitionSemanticIT extends AbstractIntegrationTest {

    /**
     * Test RxnormStatedDefinition Semantics.
     *
     * @result Reads content from file and validates StatedDefinition of Semantics by calling private method assertOwlElement().
     */
    @Test
    public void testRxnormStatedDefinitionSemantics() throws IOException {
        String errorFile = "target/failsafe-reports/Rxnorm_StatedDefinitions_not_found.txt";
        String absolutePath = rxnormOwlFileName;
        int notFound = processOwlFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Rxnorm StatedDefinition semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertOwlElement(RxnormData rxnormData) {
        if (rxnormData.getId() != null) {
            if (rxnormData.getEquivalentClassesStr().isEmpty()) {
                return true;
            }

            // Generate UUID based on RxNorm ID
            UUID rxnormUuid = conceptUuid(rxnormData.getId());
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(rxnormUuid));
            StateSet stateActive = StateSet.ACTIVE;
            StampCalculator stampCalcActive = StampCalculatorWithCache
                    .getCalculator(StampCoordinateRecord.make(stateActive, Coordinates.Position.LatestOnDevelopment()));
            PatternEntityVersion latestAxiomPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest().latest(TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN).get();
            String owlExpression = RxnormUtility.transformOwlString(UUID.fromString(namespaceString), rxnormData.getEquivalentClassesStr());
            AtomicBoolean matchedOwlExpression = new AtomicBoolean(false);

            EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN.nid(), semanticEntity -> {
                Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
                if (latestActive.isPresent()) {
                    String axiomSyntaxText = latestAxiomPattern.getFieldWithMeaning(TinkarTerm.AXIOM_SYNTAX, latestActive.get());
                    if (owlExpression.equals(axiomSyntaxText)) {
                        matchedOwlExpression.set(true);
                    }
                }
            });

            return (matchedOwlExpression.get());
        }
        return false;
    }
}
