package com.tmax.tibero.hibernate.tool.schema.extract.internal;

import org.hibernate.tool.schema.extract.internal.SequenceInformationExtractorLegacyImpl;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SequenceInformationExtractorTiberoDatabaseImpl
        extends SequenceInformationExtractorLegacyImpl {
    public static final SequenceInformationExtractorTiberoDatabaseImpl INSTANCE = new SequenceInformationExtractorTiberoDatabaseImpl();

    protected String sequenceCatalogColumn() {
        return null;
    }

    protected String sequenceSchemaColumn() {
        return null;
    }

    protected String sequenceStartValueColumn() {
        return null;
    }

    protected String sequenceMinValueColumn() {
        return "min_value";
    }

    protected Long resultSetMaxValue(ResultSet resultSet)
            throws SQLException {
        return Long.valueOf(resultSet.getBigDecimal("max_value").longValue());
    }

    protected String sequenceIncrementColumn() {
        return "increment_by";
    }
}
