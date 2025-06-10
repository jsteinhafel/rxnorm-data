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
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.PREFERRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RxnormPatternSemanticIT extends AbstractIntegrationTest {

    /**
     * Test RxnormPattern Semantics.
     *
     * @result Reads content from file and validates Pattern Semantics by calling private method assertOwlElement().
     */
    @Test
    public void testRxnormPatternSemantics() throws IOException {
        String errorFile = "target/failsafe-reports/Rxnorm_Patterns_not_found.txt";
        String absolutePath = rxnormOwlFileName; 
        int notFound = processOwlFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Rxnorm Pattern semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertOwlElement(RxnormData rxnormData) {
        String rxnormId = rxnormData.getId();

        int countQualitativeDistinction = 0;
        int countQuantity = 0;
        int countSchedule = 0;
        int countHumanDrug = 0;
        int countVetDrug = 0;
        int countTallmanSynonym = 0;
        
        AtomicInteger innerQualitativeDistinctionCount = new AtomicInteger(0);
        AtomicInteger innerQuantityCount = new AtomicInteger(0);
        AtomicInteger innerScheduleCount = new AtomicInteger(0);
        AtomicInteger innerHumanDrugCount = new AtomicInteger(0);
        AtomicInteger innerVetDrugCount = new AtomicInteger(0);
        AtomicInteger innerTallmanSynonymCount = new AtomicInteger(0);
        
        if(!rxnormData.getQualitativeDistinction().isEmpty()) {
        	countQualitativeDistinction++;
        }
        
        if(!rxnormData.getQuantity().isEmpty()) {
        	countQuantity++;
        }
        
        if(!rxnormData.getSchedule().isEmpty()) {
            countSchedule++;
        }
        
        if(!rxnormData.getHumanDrug().isEmpty()) {
		    countHumanDrug++;
        }
        
        if(!rxnormData.getVetDrug().isEmpty()) {
        	countVetDrug++;
        }
        
        countTallmanSynonym = rxnormData.getTallmanSynonyms().size();
           
        StateSet stateActive = StateSet.ACTIVE;
       
		
		EntityProxy.Concept concept;
		
		if(rxnormId != null) {
	        StampCalculator stampCalcActive = StampCalculatorWithCache
		               .getCalculator(StampCoordinateRecord.make(stateActive, Coordinates.Position.LatestOnDevelopment()));
		        
			concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid(rxnormId)));
			
			PatternEntityVersion latestQualitativeDistinctionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getQualitativeDistinctionPattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getQualitativeDistinctionPattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getQualitativeDistinction().isEmpty()) {
                        String source = latestQualitativeDistinctionPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.QUALITATIVE_PATTERN_DISTINCTION_MEANING))), latestActive.get());
                        Component componentValue = latestQualitativeDistinctionPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.QUALITATIVE_PATTERN_LANGUAGE_MEANING))), latestActive.get());
                        
                        if (rxnormData.getQualitativeDistinction().equals(source) && ENGLISH_LANGUAGE.equals(componentValue)) {
                            innerQualitativeDistinctionCount.addAndGet(1);
                        }
                    }
	        	} 
	        });      
	        
			PatternEntityVersion latestQuantityPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getQuantityPattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getQuantityPattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getQuantity().isEmpty()) {
	        			String source = latestQuantityPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.QUANTITY_PATTERN_DRUG_MEANING))), latestActive.get());
                        
                        if (rxnormData.getQuantity().equals(source)) {
                           innerQuantityCount.addAndGet(1);
                        }
                    }
	        	} 
	        });      	        	        
	        
			PatternEntityVersion latestSchedulePattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getSchedulePattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getSchedulePattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getSchedule().isEmpty()) {
                        String source = latestSchedulePattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.SCHEDULE_PATTERN_DRUG_MEANING))), latestActive.get());
                        Component componentValue = latestSchedulePattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.SCHEDULE_PATTERN_LANGUAGE_MEANING))), latestActive.get());
                        
                        if (rxnormData.getSchedule().equals(source) && ENGLISH_LANGUAGE.equals(componentValue)) {
                            innerScheduleCount.addAndGet(1);
                        }
                    }
	        	} 
	        });      	        
			
			PatternEntityVersion latestHumanDrugPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getHumanDrugPattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getHumanDrugPattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	
	        	EntityProxy.Concept humanDrugConcept = RxnormUtility.makeConceptProxy(UUID.fromString(namespaceString), rxnormData.getHumanDrug());
	        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getHumanDrug().isEmpty()) {
	        			
						Concept source = (Concept) latestHumanDrugPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(
								UUID.fromString(RxnormUtility.HUMAN_DRUG_PATTERN_LANGUAGE_MEANING))), latestActive.get());
						 
						if (humanDrugConcept.equals(source)) { 
							innerHumanDrugCount.addAndGet(1); 
						}
                    }
	        	} 
	        });      	        			

			PatternEntityVersion latestVetDrugPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getVetDrugPattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getVetDrugPattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        	
	        	EntityProxy.Concept vetDrugConcept = RxnormUtility.makeConceptProxy(UUID.fromString(namespaceString), rxnormData.getVetDrug());
	        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getVetDrug().isEmpty()) {
	        			        			
						Concept source = (Concept) latestVetDrugPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(
								UUID.fromString(RxnormUtility.VETERINARIAN_DRUG_PATTERN_LANGUAGE_MEANING))), latestActive.get());
						 
						if (vetDrugConcept.equals(source)) { 
							innerVetDrugCount.addAndGet(1); 
						}						
                    }
	        	} 
	        }); 	        
        
			PatternEntityVersion latestTallmanSynonymPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(RxnormUtility.getTallmanSynonymPattern()).get();
			
	        EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), RxnormUtility.getTallmanSynonymPattern().nid(), semanticEntity -> {
	        	Latest<SemanticEntityVersion> latestActive = stampCalcActive.latest(semanticEntity);
	        		        	
	        	if (latestActive.isPresent()) {
	        		if(!rxnormData.getTallmanSynonyms().isEmpty()) {
	    	        	Component source = latestTallmanSynonymPattern.getFieldWithMeaning(EntityProxy.Concept.make(PublicIds.of(UUID.fromString(RxnormUtility.TALLMAN_SYNONYM_PATTERN_LANGUAGE_MEANING))), latestActive.get());
	                        
	                    if (PREFERRED.equals(source)) {
	                        innerTallmanSynonymCount.addAndGet(1);
	                    }	    		        				
                    }
	        	} 
	        }); 	       
		}
		
		if (countQualitativeDistinction != innerQualitativeDistinctionCount.get()) {
		    LOG.error("QualitativeDistinction Pattern [owlCount={},dbCount={}]", countQualitativeDistinction, innerQualitativeDistinctionCount.get());
		}

		if (countHumanDrug != innerHumanDrugCount.get()) {
		    LOG.error("HumanDrug Pattern [owlCount={},dbCount={}]", countHumanDrug, innerHumanDrugCount.get());
		}
		
		if (countQuantity != innerQuantityCount.get()) {
		    LOG.error("Quantity Pattern [owlCount={},dbCount={}]", countQuantity, innerQuantityCount.get());
		}
		
		if (countSchedule != innerScheduleCount.get()) {
		    LOG.error("Schedule Pattern [owlCount={},dbCount={}]", countSchedule, innerScheduleCount.get());
		}		

		if (countTallmanSynonym != innerTallmanSynonymCount.get()) {
		    LOG.error("TallmanSynonym Pattern [owlCount={},dbCount={}]", countTallmanSynonym, innerTallmanSynonymCount.get());
		}

		if (countVetDrug != innerVetDrugCount.get()) {
		    LOG.error("VetDrug Pattern [owlCount={},dbCount={}]", countVetDrug, innerVetDrugCount.get());
		}
		
        return  countQualitativeDistinction == innerQualitativeDistinctionCount.get() 
        		&& countHumanDrug == innerHumanDrugCount.get()
        		&& countQuantity == innerQuantityCount.get() 
        		&& countSchedule == innerScheduleCount.get()
        		&& countTallmanSynonym == innerTallmanSynonymCount.get() 
        		&& countVetDrug == innerVetDrugCount.get();
    }  
}
