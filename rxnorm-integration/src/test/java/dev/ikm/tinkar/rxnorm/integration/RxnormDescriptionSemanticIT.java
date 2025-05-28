package dev.ikm.tinkar.rxnorm.integration;

import dev.ikm.maven.RxnormData;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.component.Component;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.PatternEntityVersion;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_CASE_SIGNIFICANCE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RxnormDescriptionSemanticIT extends AbstractIntegrationTest {

    /**
     * Test RxnormDescriptions Semantics.
     *
     * @result Reads content from file and validates Description of Semantics by calling private method assertOwlElement().
     */
    @Test
    @Disabled // TODO
    public void testRxnormDescriptionSemantics() throws IOException {
        String errorFile = "target/failsafe-reports/Rxnorm_Descriptions_not_found.txt";
        String absolutePath = rxnormOwlFileName;
        int notFound = processOwlFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Rxnorm Description semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertOwlElement(RxnormData rxnormData) {
        if (rxnormData.getId() != null) {
            // Generate UUID based on RxNorm ID
            UUID rxnormUuid = conceptUuid(rxnormData.getId());
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(rxnormUuid));
            StateSet stateActive = StateSet.ACTIVE;
            StampCalculator stampCalcActive = StampCalculatorWithCache
                    .getCalculator(StampCoordinateRecord.make(stateActive, Coordinates.Position.LatestOnDevelopment()));
            PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest().latest(TinkarTerm.DESCRIPTION_PATTERN).get();
            AtomicBoolean rxnormName = new AtomicBoolean(!rxnormData.getRxnormName().isEmpty());
            AtomicBoolean rxnormSynonym = new AtomicBoolean(!rxnormData.getRxnormSynonym().isEmpty());
            AtomicBoolean rxnormPrescribableSynonym = new AtomicBoolean(!rxnormData.getPrescribableSynonym().isEmpty());
            AtomicReference<List<String>> rxnormTallmanSynonym = new AtomicReference<>();
            AtomicBoolean matchedName = new AtomicBoolean(!rxnormName.get());
            AtomicBoolean matchedSynonym = new AtomicBoolean(!rxnormSynonym.get());
            AtomicBoolean matchedPrescribableSynonym = new AtomicBoolean(!rxnormPrescribableSynonym.get());
            AtomicInteger matchedTallmanSynonyms = new AtomicInteger(0);

            if (!rxnormData.getTallmanSynonyms().isEmpty()) {
                rxnormTallmanSynonym.set(rxnormData.getTallmanSynonyms());
            } else {
                rxnormTallmanSynonym.set(new ArrayList<>());
            }

            EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.DESCRIPTION_PATTERN.nid(), semanticEntity -> {
                Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);

                if (latestActive.isPresent()) {
                    String textForDesc = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.TEXT_FOR_DESCRIPTION, latestActive.get());
                    Component descCaseSignificance = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.DESCRIPTION_CASE_SIGNIFICANCE, latestActive.get());
                    Component descType = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.DESCRIPTION_TYPE, latestActive.get());

                    if (rxnormName.get()) {
                        if (textForDesc.equals(rxnormData.getRxnormName())
                                && descCaseSignificance.equals(DESCRIPTION_NOT_CASE_SENSITIVE)
                                && descType.equals(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE)) {
                            matchedName.set(true);
                            rxnormName.set(false);
                        }
                    }

                    if (rxnormSynonym.get()) {
                        if (textForDesc.equals(rxnormData.getRxnormSynonym())
                                && descCaseSignificance.equals(DESCRIPTION_NOT_CASE_SENSITIVE)
                                && (descType.equals(REGULAR_NAME_DESCRIPTION_TYPE))) {
                            matchedSynonym.set(true);
                            rxnormSynonym.set(false);
                        }
                    }

                    if (rxnormPrescribableSynonym.get()) {
                        if (textForDesc.equals(rxnormData.getPrescribableSynonym())
                                && descCaseSignificance.equals(DESCRIPTION_NOT_CASE_SENSITIVE)
                                && (descType.equals(REGULAR_NAME_DESCRIPTION_TYPE))) {
                            matchedPrescribableSynonym.set(true);
                            rxnormPrescribableSynonym.set(false);
                        }
                    }

                    List<String> synonyms = rxnormTallmanSynonym.get();
                    if (!synonyms.isEmpty()) {
                        synonyms.forEach(synonym -> {
                            if (textForDesc.equals(synonym)
                                    && descCaseSignificance.equals(DESCRIPTION_CASE_SENSITIVE)
                                    && descType.equals(REGULAR_NAME_DESCRIPTION_TYPE)) {
                                matchedTallmanSynonyms.incrementAndGet();
                            }
                        });
                    }
                }
            });

            return (matchedName.get() && matchedSynonym.get() && matchedPrescribableSynonym.get() && matchedTallmanSynonyms.get() == rxnormTallmanSynonym.get().size());
        }
        return false; // TOTAL 28
    }
}
