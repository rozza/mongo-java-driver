package org.bson.codecs.record.samples;

import java.util.List;

public record NestedWildcardRecord(List<? extends NestedWildcardRecordField<?>> listValue) {
}
