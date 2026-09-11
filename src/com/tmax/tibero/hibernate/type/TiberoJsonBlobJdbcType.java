package com.tmax.tibero.hibernate.type;

import org.hibernate.dialect.Dialect;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.type.descriptor.converter.spi.BasicValueConverter;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.AggregateJdbcType;
import org.hibernate.type.descriptor.jdbc.OracleJsonBlobJdbcType;

/**
 * JSON을 BLOB로 바인딩하는 경로는 Hibernate의 {@link OracleJsonBlobJdbcType}를 그대로 쓰되,
 * Oracle 전용 체크 제약만 걷어낸다.
 *
 * <p>{@code OracleJsonBlobJdbcType.getCheckCondition()}은 {@code "<col> is json"}을 돌려주는데
 * Tibero는 {@code IS JSON} 술어를 지원하지 않아 {@code create table}이 실패한다.
 * (ps06: {@code JDBC-8014 Missing NULL keyword} / develop: {@code JDBC-8187 Token 'JSON' is not valid after IS or IS NOT})
 * 컬럼 타입 {@code json} 자체와 일반 {@code check}는 정상이므로 술어만 제거하면 된다.
 *
 * <p>{@code null} 반환은 "체크 제약 없음"의 공식 계약이다 —
 * {@code JdbcType.getCheckCondition()} 기본 구현이 {@code null}이고
 * {@code BasicValue}가 {@code if (checkCondition != null)} 로 건너뛴다.
 */
public class TiberoJsonBlobJdbcType extends OracleJsonBlobJdbcType {

    public static final TiberoJsonBlobJdbcType INSTANCE = new TiberoJsonBlobJdbcType(null);

    protected TiberoJsonBlobJdbcType(EmbeddableMappingType embeddableMappingType) {
        super(embeddableMappingType);
    }

    @Override
    public String getCheckCondition(
            String columnName,
            JavaType<?> javaType,
            BasicValueConverter<?, ?> converter,
            Dialect dialect) {
        // Tibero는 IS JSON 술어 미지원
        return null;
    }

    /**
     * 이 오버라이드를 빼면 임베더블을 JSON 집계 컬럼으로 매핑할 때
     * 부모 구현이 {@code new OracleJsonBlobJdbcType(mappingType)} 을 돌려주어
     * {@code is json} 체크가 되살아난다.
     */
    @Override
    public AggregateJdbcType resolveAggregateJdbcType(
            EmbeddableMappingType mappingType,
            String sqlType,
            RuntimeModelCreationContext creationContext) {
        return new TiberoJsonBlobJdbcType(mappingType);
    }
}
