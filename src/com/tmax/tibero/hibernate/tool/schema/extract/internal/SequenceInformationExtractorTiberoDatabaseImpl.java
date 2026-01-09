package com.tmax.tibero.hibernate.tool.schema.extract.internal;

import org.hibernate.tool.schema.extract.internal.SequenceInformationExtractorLegacyImpl;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * select * from all_sequences 결과에서 시퀀스 메타데이터를 읽을 때 사용하는 클래스
 * 스키마 검증 및 hbm2ddl update할 때 기존 시퀀스 상태를 확인하는 용도
 */
public class SequenceInformationExtractorTiberoDatabaseImpl extends SequenceInformationExtractorLegacyImpl {

    // 싱글톤
    public static final SequenceInformationExtractorTiberoDatabaseImpl INSTANCE = new SequenceInformationExtractorTiberoDatabaseImpl();

    /**
     * all_sequences view의 컬럼명 매핑 메서드
     */
    @Override
    protected String sequenceCatalogColumn() {
        return null;
    }
    @Override
    protected String sequenceSchemaColumn() {
        return null;
    }
    @Override
    protected String sequenceStartValueColumn() {
        return null;
    }
    @Override
    protected String sequenceMinValueColumn() {
        return "min_value";
    }
    @Override
    protected String sequenceMaxValueColumn() {
        return "max_value";
    }
    @Override
    protected String sequenceIncrementColumn() {
        return "increment_by";
    }

    /**
     * 값 추출 메서드
     */
    @Override
    protected Number resultSetMinValue(ResultSet resultSet) throws SQLException {
        return resultSet.getBigDecimal(sequenceMinValueColumn());
    }
    @Override
    protected Number resultSetMaxValue(ResultSet resultSet) throws SQLException {
        return resultSet.getBigDecimal(sequenceMaxValueColumn());
    }
    @Override
    protected Number resultSetIncrementValue(ResultSet resultSet) throws SQLException {
        return resultSet.getBigDecimal(sequenceIncrementColumn());
    }

}
