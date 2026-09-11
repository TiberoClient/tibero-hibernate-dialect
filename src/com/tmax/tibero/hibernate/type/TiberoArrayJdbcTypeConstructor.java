package com.tmax.tibero.hibernate.type;

import org.hibernate.dialect.Dialect;
import org.hibernate.tool.schema.extract.spi.ColumnTypeInformation;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcTypeConstructor;
import org.hibernate.type.spi.TypeConfiguration;

import java.sql.Types;

/**
 * 배열 필드마다 {@link TiberoArrayJdbcType} 을 만들어 주는 팩토리.
 *
 * <p>Hibernate 기본 {@code ArrayJdbcTypeConstructor} 는 <b>타입 이름 없이</b>
 * {@code new ArrayJdbcType(elementType)} 만 만든다. 그러면 {@code createArrayOf} 에
 * 넘길 VARRAY 이름을 알 수 없어 tbjdbc 가 거부한다. 여기서 이름을 결정해 넣는다.
 *
 * <p>기존 스키마를 읽어 매핑하는 경우({@code ColumnTypeInformation} 이 채워진 경우)에는
 * <b>카탈로그의 실제 타입명을 그대로</b> 쓴다. 이름 규칙으로 유추하면 기존 스키마의
 * 이름과 어긋날 수 있기 때문이다.
 */
public class TiberoArrayJdbcTypeConstructor implements JdbcTypeConstructor {

    public static final TiberoArrayJdbcTypeConstructor INSTANCE = new TiberoArrayJdbcTypeConstructor();

    @Override
    public JdbcType resolveType(
            TypeConfiguration typeConfiguration, Dialect dialect,
            BasicType<?> elementType, ColumnTypeInformation columnTypeInformation) {
        String typeName = columnTypeInformation == null ? null : columnTypeInformation.getTypeName();
        if (typeName == null || typeName.isBlank()) {
            typeName = TiberoArrayJdbcType.arrayTypeNameFor(elementType, dialect);
        }
        return new TiberoArrayJdbcType(elementType.getJdbcType(), typeName);
    }

    @Override
    public JdbcType resolveType(
            TypeConfiguration typeConfiguration, Dialect dialect,
            JdbcType elementType, ColumnTypeInformation columnTypeInformation) {
        String typeName = columnTypeInformation == null ? null : columnTypeInformation.getTypeName();
        if (typeName == null || typeName.isBlank()) {
            final Integer precision = columnTypeInformation == null ? null : columnTypeInformation.getColumnSize();
            final Integer scale = columnTypeInformation == null ? null : columnTypeInformation.getDecimalDigits();
            typeName = TiberoArrayJdbcType.arrayTypeNameFor(
                    elementType.getJdbcRecommendedJavaTypeMapping(precision, scale, typeConfiguration),
                    elementType, dialect);
        }
        return new TiberoArrayJdbcType(elementType, typeName);
    }

    @Override
    public int getDefaultSqlTypeCode() {
        return Types.ARRAY;
    }
}
