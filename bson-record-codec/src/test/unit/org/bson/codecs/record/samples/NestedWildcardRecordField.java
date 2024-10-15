package org.bson.codecs.record.samples;


public record NestedWildcardRecordField<V extends NestedWildcardRecordNestedField<?>>(V nestedField) {
}
