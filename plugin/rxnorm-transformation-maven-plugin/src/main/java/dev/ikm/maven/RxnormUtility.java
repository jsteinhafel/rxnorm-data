package dev.ikm.maven;

import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;

import java.util.UUID;

public class RxnormUtility {
    private static EntityProxy.Pattern makePatternProxy(UUID namespace, String description) {
        return EntityProxy.Pattern.make(description, UuidT5Generator.get(namespace, description));
    }

    public static EntityProxy.Concept makeConceptProxy(UUID namespace, String description) {
        return EntityProxy.Concept.make(description, UuidT5Generator.get(namespace, description));
    }
}
