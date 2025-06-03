package dev.ikm.tinkar.rxnorm.integration;

import dev.ikm.maven.RxnormData;
import dev.ikm.maven.RxnormUtility;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.component.Component;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RxnormIdentifierSemanticIT extends AbstractIntegrationTest {

    /**
     * Test RxnormIdentifier Semantics.
     *
     * @result Reads content from file and validates Identifier Semantics by calling private method assertOwlElement().
     */
    @Test
    public void testRxnormIdentifierSemantics() throws IOException {
        String errorFile = "target/failsafe-reports/Rxnorm_Identifiers_not_found.txt";
        String absolutePath = rxnormOwlFileName; 
        int notFound = processOwlFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Rxnorm Identifier semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertOwlElement(RxnormData rxnormData) {
        String rxnormId = rxnormData.getId();

        int snomedCount = 0;
        int rxCuidCount = 0;
        int vuidCount = 0;
        int ndcCount = 0;
        
        AtomicInteger innerSnomedCount = new AtomicInteger(0);
        AtomicInteger innerRxCuidCount = new AtomicInteger(0);
        AtomicInteger innerVuidCount = new AtomicInteger(0);
        AtomicInteger innerNdcCount = new AtomicInteger(0);
        
        if (!rxnormData.getSnomedCtId().isEmpty()) {
            snomedCount++;
        }
        if (!rxnormData.getRxCuiId().isEmpty()) {
            rxCuidCount++;
        }
        if (!rxnormData.getVuidId().isEmpty()) {
            vuidCount++;
        }
        if(!rxnormData.getNdcCodesWithEndDates().isEmpty()){
			 for (Map.Entry<String, String> entry : rxnormData.getNdcCodesWithEndDates().entrySet()) {
		          ndcCount++;
			 }
        }
           
        StateSet stateActive = StateSet.ACTIVE;
        StateSet stateInActive = StateSet.INACTIVE;
        
		PatternEntityVersion latestIdentifierPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.IDENTIFIER_PATTERN).get();
		
		EntityProxy.Concept concept;
		
		if(rxnormId != null) {
	        StampCalculator stampCalcActive = StampCalculatorWithCache
	               .getCalculator(StampCoordinateRecord.make(stateActive, Coordinates.Position.LatestOnDevelopment()));

	        StampCalculator stampCalcInActive = StampCalculatorWithCache
                   .getCalculator(StampCoordinateRecord.make(stateInActive, Coordinates.Position.LatestOnDevelopment()));
	        
			concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid(rxnormId)));
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.IDENTIFIER_PATTERN.nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	Latest<SemanticEntityVersion> latestInActive = stampCalcInActive.latest(semanticEntity);
	        	
	        	if (latestActive.isPresent()) {
	        		if (!rxnormData.getSnomedCtId().isEmpty()) {
                        Component component = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_SOURCE, latestActive.get());
                        String value = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_VALUE, latestActive.get());
                        if (rxnormData.getSnomedCtId().equals(value) && RxnormUtility.getSnomedIdentifierConcept().equals(component)) {
                            innerSnomedCount.addAndGet(1);
                        }
                    }
	        		
	        		if(!rxnormData.getRxCuiId().isEmpty()){
                        Component component = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_SOURCE, latestActive.get());
                        String value = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_VALUE, latestActive.get());
                        if (rxnormData.getRxCuiId().equals(value) && RxnormUtility.getRxcuidConcept().equals(component)) {
                            innerRxCuidCount.addAndGet(1);
                        }
                    }
	        		
	        		if(!rxnormData.getVuidId().isEmpty()){
                        Component component = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_SOURCE, latestActive.get());
                        String value = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_VALUE, latestActive.get());
                        if (rxnormData.getVuidId().equals(value) && RxnormUtility.getVuidConcept().equals(component)) {
                            innerVuidCount.addAndGet(1);
                        }
                    }
	        		
	        		if(!rxnormData.getNdcCodesWithEndDates().isEmpty()) {
	        			for (Map.Entry<String, String> entry : rxnormData.getNdcCodesWithEndDates().entrySet()) {
	        				
	                        String ndcCode = entry.getKey();
	                        
	                        Component component = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_SOURCE, latestActive.get());
	                        String value = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_VALUE, latestActive.get());
	                        if (ndcCode.equals(value) && RxnormUtility.getNdcIdentifierConcept().equals(component)) {
	                            innerNdcCount.addAndGet(1);
	                        }
	        			}       			
                   }
	        	} 
	        	
	        	if (latestInActive.isPresent()) {
	        		 if(!rxnormData.getNdcCodesWithEndDates().isEmpty()) {
	        			for (Map.Entry<String, String> entry : rxnormData.getNdcCodesWithEndDates().entrySet()) {
	        				
	                        String ndcCode = entry.getKey();
	                        
	                        Component component = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_SOURCE, latestInActive.get());
	                        String value = latestIdentifierPattern.getFieldWithMeaning(TinkarTerm.IDENTIFIER_VALUE, latestInActive.get());
	                        if (ndcCode.equals(value) && RxnormUtility.getNdcIdentifierConcept().equals(component)) {
	                            innerNdcCount.addAndGet(1);
	                        }
	        			}       			
                    }   
	        	} 
	        });    
		}
		
		if (snomedCount != innerSnomedCount.get()) {
		    LOG.error("snomedCtId [owlCount={},dbCount={}]", snomedCount, innerSnomedCount.get());
		}
		
		if (rxCuidCount != innerRxCuidCount.get()) {
		    LOG.error("rxCuidCount [owlCount={},dbCount={}]", rxCuidCount, innerRxCuidCount.get());
		}
		
		if (vuidCount != innerVuidCount.get()) {
		    LOG.error("vuidCount [owlCount={},dbCount={}]", vuidCount, innerVuidCount.get());
		}
		
		if (ndcCount != innerNdcCount.get()) {
		    LOG.error("ndcCount [owlCount={},dbCount={}]", ndcCount, innerNdcCount.get());
		}
		
        return snomedCount == innerSnomedCount.get() && rxCuidCount == innerRxCuidCount.get() && vuidCount == innerVuidCount.get() && ndcCount == innerNdcCount.get();  
    }    
}
