package dev.ikm.maven;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RxnormData {
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
    private String rdfsLabel = "";
    private String subClassOfStr = "";

    public RxnormData(String uri) {
        this.uri = uri;
        if (uri.startsWith("http://mor.nlm.nih.gov/RXNORM/")) {
            this.id = uri.substring("http://mor.nlm.nih.gov/RXNORM/".length());
        } else {
            this.id = uri.substring("http://snomed.info/id/".length());
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

    public void setRdfsLabel(String rdfsLabel) {
        this.rdfsLabel = rdfsLabel;
    }

    public void setSubClassOfStr(String subClassOfStr) {
        this.subClassOfStr = subClassOfStr;
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

    public String getRdfsLabel() {
        return rdfsLabel;
    }

    public String getSubClassOfStr() {
        return subClassOfStr;
    }

    @Override
    public String toString() {
        return this.id +" - "+ this.uri +" - "+ this.rxnormName +" - "+ this.snomedCtId +" - "+ this.rxCuiId +" - "+ this.vuidId;
    }

}
