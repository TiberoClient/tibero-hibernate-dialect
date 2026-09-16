package com.tmax.tibero.hibernate.type;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.mapping.UserDefinedObjectType;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.AggregateJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * {@code @Struct} 임베더블 값을 JDBC 로 넣고 빼는 방식.
 */
public class TiberoStructJdbcType extends org.hibernate.dialect.StructJdbcType {

    /**
     * 등록용 인스턴스.
     *
     * <p>이 인스턴스 자체는 <b>읽기 전용 원형</b>이다({@code typeName} 이 {@code null}).
     * 실제로 쓰이는 인스턴스는 임베더블마다 {@link #resolveAggregateJdbcType} 이 만들어 준다.
     */
    public static final AggregateJdbcType INSTANCE = new TiberoStructJdbcType(null, null, null);

    public TiberoStructJdbcType(EmbeddableMappingType mappingType, String typeName, int[] orderMapping) {
        super(mappingType, typeName, orderMapping);
    }

    /**
     * 임베더블 하나에 대응하는 인스턴스를 만든다. 부팅 중 {@code @Struct} 마다 한 번씩 불린다.
     *
     * <p>{@code orderMapping} 은 <b>자바 필드 순서와 DB object 타입의 속성 순서가 다를 때</b>
     * 둘을 잇는 색인이다. Hibernate 는 내부적으로 필드를 이름순으로 정렬하는데 DDL 의 속성 순서는
     * 그와 다를 수 있어서, 값을 넣고 뺄 때 이 색인으로 자리를 맞춘다. 부트 모델의 UDT 정의에서
     * 가져온다.
     *
     * <p>부모 구현과 같은 일을 하되 <b>우리 타입으로</b> 만드는 것이 유일한 차이다.
     * 이렇게 하지 않으면 중첩 struct 의 안쪽이 Hibernate 기본 타입으로 만들어져
     * 위의 두 오버라이드가 적용되지 않는다.
     */
    @Override
    public AggregateJdbcType resolveAggregateJdbcType(
            EmbeddableMappingType mappingType,
            String sqlType,
            RuntimeModelCreationContext creationContext) {
        final UserDefinedObjectType udt = creationContext.getBootModel()
                .getDatabase()
                .getDefaultNamespace()
                .locateUserDefinedType(Identifier.toIdentifier(sqlType));
        if (udt == null) {
            // @Struct(name=...) 이 가리키는 UDT 가 부트 모델에 없다는 뜻이다.
            // 기본 구현은 여기서 NullPointerException 을 던져 원인을 알기 어렵다.
            throw new org.hibernate.MappingException(
                    "@Struct type '" + sqlType + "' is not defined in the mapping. "
                            + "Check the @Struct(name=...) value, or whether the embeddable is "
                            + "reachable from a mapped entity.");
        }
        rejectUnsupportedAttributeTypes(sqlType, udt);
        return new TiberoStructJdbcType(mappingType, sqlType, udt.getOrderMapping());
    }

    /**
     * tbjdbc 가 STRUCT 안에서 다루지 못하는 속성 타입을 <b>기동 시점에</b> 걸러낸다.
     *
     * <p>속성 타입별로 {@code createStruct} / {@code Struct.getAttributes()} 를 실측한 결과다.
     * SQL 리터럴({@code TYPE(1.5)})로 넣는 것은 되므로 <b>DB 제약이 아니라 드라이버 한계</b>다.
     *
     * <table>
     *   <caption>STRUCT 속성 타입 지원 (ps06 · tibero7-jdbc-11 실측)</caption>
     *   <tr><th>타입</th><th>결과</th></tr>
     *   <tr><td>{@code varchar2} · {@code nvarchar2} · {@code number} · {@code date} ·
     *           {@code timestamp} · {@code raw}</td><td>정상</td></tr>
     *   <tr><td>{@code binary_double} · {@code binary_float}</td>
     *       <td>{@code JDBC-590703 Unsupported data type}</td></tr>
     *   <tr><td>{@code clob} · {@code blob}</td>
     *       <td>{@code JDBC-90651 Failed to convert given data}</td></tr>
     * </table>
     *
     * <p>검사하지 않으면 DDL 은 멀쩡히 만들어지고 <b>첫 INSERT 에서야</b> 드라이버 오류가 나
     * 원인을 찾기 어렵다. 어느 필드가 문제인지 짚어 기동 때 알린다.
     */
    private static void rejectUnsupportedAttributeTypes(String structName, UserDefinedObjectType udt) {
        for (org.hibernate.mapping.Column column : udt.getColumns()) {
            final String reason = unsupportedReason(column.getSqlTypeCode());
            if (reason != null) {
                throw new org.hibernate.MappingException(
                        "@Struct type '" + structName + "' has attribute '" + column.getName() + "' "
                                + reason + ". The Tibero JDBC driver cannot bind or read that type "
                                + "inside a STRUCT, so the mapping would fail on the first insert. "
                                + "Use a supported attribute type "
                                + "(varchar2 / nvarchar2 / number / date / timestamp / raw), "
                                + "or map the embeddable with @Embedded instead of @Struct.");
            }
        }
    }

    /** 지원하지 않는 타입이면 사람이 읽을 이유, 아니면 {@code null}. */
    private static String unsupportedReason(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case SqlTypes.DOUBLE:
            case SqlTypes.FLOAT:
            case SqlTypes.REAL:
                // TiberoDialect 는 IEEE 정밀도를 지키려고 Double/Float 을
                // binary_double / binary_float 으로 매핑한다. BigDecimal 로 바꾸면 된다.
                return "mapped to binary_double/binary_float (JDBC-590703)";
            case SqlTypes.BLOB:
            case SqlTypes.CLOB:
            case SqlTypes.NCLOB:
            case SqlTypes.MATERIALIZED_BLOB:
            case SqlTypes.MATERIALIZED_CLOB:
            case SqlTypes.MATERIALIZED_NCLOB:
                // @Lob 을 떼면 varchar2 / raw 로 떨어져 정상 동작한다.
                return "mapped to a LOB (JDBC-90651)";
            default:
                return null;
        }
    }


    @Override
    public Object[] extractJdbcValues(Object rawJdbcValue, WrapperOptions options) throws SQLException {
        if (rawJdbcValue == null) {
            return null;
        }
        return super.extractJdbcValues(rawJdbcValue, options);
    }

    @Override
    public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
        return new BasicBinder<X>(javaType, this) {

            @Override
            protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
                    throws SQLException {
                st.setObject(index, createJdbcValue(value, options));
            }

            @Override
            protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
                    throws SQLException {
                st.setObject(name, createJdbcValue(value, options));
            }

            /**
             * 중첩 {@code @Struct} 의 안쪽 값이 이 경로로 나간다.
             * 기본 구현은 도메인 객체를 그대로 돌려주어 tbjdbc 가 {@code JDBC-90651} 을 던진다.
             */
            @Override
            public Object getBindValue(X value, WrapperOptions options) throws SQLException {
                return createJdbcValue(value, options);
            }

            /** tbjdbc 는 타입명 없는 {@code setNull(STRUCT)} 을 거부한다. */
            @Override
            protected void doBindNull(PreparedStatement st, int index, WrapperOptions options)
                    throws SQLException {
                st.setNull(index, SqlTypes.STRUCT, getStructTypeName());
            }

            @Override
            protected void doBindNull(CallableStatement st, String name, WrapperOptions options)
                    throws SQLException {
                st.setNull(name, SqlTypes.STRUCT, getStructTypeName());
            }
        };
    }
}
