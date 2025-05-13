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

    public String getEquivalentClassesStr() {
        return equivalentClassesStr;
    }

    @Override
    public String toString() {
        return this.id +" - "+ this.uri +" - "+ this.rxnormName +" - "+ this.snomedCtId +" - "+ this.rxCuiId +" - "+ this.vuidId;
    }

}
