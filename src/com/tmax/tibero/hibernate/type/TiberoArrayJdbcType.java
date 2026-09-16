package com.tmax.tibero.hibernate.type;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.mapping.UserDefinedArrayType;
import org.hibernate.type.descriptor.converter.spi.JpaAttributeConverter;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.converter.spi.BasicValueConverter;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.ArrayJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.SqlTypedJdbcType;
import org.hibernate.type.descriptor.jdbc.StructJdbcType;
import org.hibernate.type.internal.BasicTypeImpl;
import org.hibernate.type.spi.TypeConfiguration;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

import static org.hibernate.type.SqlTypes.ARRAY;

/**
 * 배열 필드({@code String[]} · {@code Integer[]} …)를 Tibero 의 <b>VARRAY 컬럼</b>에 넣고 뺀다.
 */
public class TiberoArrayJdbcType extends ArrayJdbcType implements SqlTypedJdbcType {

    /** DDL 에 적힌 그대로의 이름. 로그·진단용 */
    private final String typeName;
    /** 카탈로그에 저장된 형태. tbjdbc 가 요구하는 형태이기도 하다 */
    private final String upperTypeName;

    public TiberoArrayJdbcType(JdbcType elementJdbcType, String typeName) {
        super(elementJdbcType);
        this.typeName = typeName;
        this.upperTypeName = typeName == null ? null : typeName.toUpperCase(Locale.ROOT);
    }

    /**
     * {@code createArrayOf} 에 넘길 타입명.
     *
     * <p>부모는 <b>요소</b>의 DDL 타입명({@code varchar2(255 char)})을 돌려준다 —
     * JDBC 규약이 그렇게 읽히기 때문이다. tbjdbc 는 <b>컬렉션 타입명</b>을 요구하며
     * 대소문자도 카탈로그와 일치해야 한다(실측).
     *
     * <pre>
     * createArrayOf("STRINGARRAY", …)  OK
     * createArrayOf("StringArray", …)  FAIL  JDBC-90664 Failed to read in user-defined metadata
     * createArrayOf("varchar2(255)", …) FAIL  JDBC-90664
     * </pre>
     */
    @Override
    protected String getElementTypeName(JavaType<?> javaType, SharedSessionContractImplementor session) {
        return upperTypeName != null ? upperTypeName : super.getElementTypeName(javaType, session);
    }

    @Override
    public String getSqlTypeName() {
        return typeName;
    }

    @Override
    public void registerOutParameter(CallableStatement st, String name) throws SQLException {
        st.registerOutParameter(name, ARRAY, upperTypeName);
    }

    @Override
    public void registerOutParameter(CallableStatement st, int index) throws SQLException {
        st.registerOutParameter(index, ARRAY, upperTypeName);
    }

    /**
     * {@code null} 배열은 타입명과 함께 바인딩해야 한다.
     *
     * <p>부모의 {@code BasicBinder} 는 {@code setNull(index, Types.ARRAY)} 를 부르는데
     * tbjdbc 가 {@code JDBC-590703 Unsupported data type. - VARRAY} 로 거부한다.
     * {@code @Struct} 쪽의 null 바인딩과 같은 성격의 문제다.
     *
     * <p>값이 있을 때는 부모 바인더에 그대로 넘긴다 — {@code createArrayOf} 조립 로직을
     * 다시 쓰지 않기 위해서다.
     */
    @Override
    public <X> ValueBinder<X> getBinder(JavaType<X> javaTypeDescriptor) {
        final ValueBinder<X> delegate = super.getBinder(javaTypeDescriptor);
        return new ValueBinder<X>() {
            @Override
            public void bind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
                if (value == null) {
                    st.setNull(index, ARRAY, upperTypeName);
                }
                else {
                    delegate.bind(st, value, index, options);
                }
            }

            @Override
            public void bind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
                if (value == null) {
                    st.setNull(name, ARRAY, upperTypeName);
                }
                else {
                    delegate.bind(st, value, name, options);
                }
            }

            @Override
            public Object getBindValue(X value, WrapperOptions options) throws SQLException {
                return delegate.getBindValue(value, options);
            }
        };
    }

    /**
     * 빈 배열을 되살린다.
     *
     * <p>tbjdbc 는 원소가 없는 VARRAY 를 읽을 때 {@code Array} 객체는 주면서
     * {@code Array.getArray()} 로는 {@code null} 을 돌려준다(실측). 부모는 그 값을 그대로
     * {@code wrap} 에 넘겨 {@code NullPointerException} 이 난다.
     *
     * <p>컬럼 자체가 SQL NULL 이면 {@code ResultSet.getArray()} 단계에서 {@code null} 이라
     * 이 메서드까지 오지 않는다. 즉 <b>여기 도달했다는 것은 "배열은 있는데 원소가 0개"</b>라는
     * 뜻이므로 빈 배열로 되살리는 것이 맞다 — null 과 빈 배열이 섞이지 않는다.
     */
    @Override
    protected <X> X getArray(BasicExtractor<X> extractor, java.sql.Array array, WrapperOptions options)
            throws SQLException {
        if (array != null && array.getArray() == null) {
            final Class<?> arrayClass = extractor.getJavaType().getJavaTypeClass();
            final Class<?> component = arrayClass.isArray() ? arrayClass.getComponentType() : Object.class;
            //noinspection unchecked
            return (X) extractor.getJavaType()
                    .wrap(java.lang.reflect.Array.newInstance(component, 0), options);
        }
        return super.getArray(extractor, array, options);
    }

    /**
     * 부트 모델에 VARRAY 타입을 등록해 exporter 가 DDL 을 낼 수 있게 한다.
     *
     * <p>이 등록이 없으면 컬럼 타입만 {@code StringArray} 로 적히고 그 타입을 만드는
     * {@code create or replace type} 이 나가지 않아 테이블 생성이 실패한다.
     *
     * <p>{@code @Array(length = n)} 이 없으면 상한을 <b>127</b> 로 둔다 — Oracle 과 같은 값이다.
     */
    @Override
    public void addAuxiliaryDatabaseObjects(
            JavaType<?> javaType, Size columnSize, Database database, TypeConfiguration typeConfiguration) {
        final JdbcType elementJdbcType = getElementJdbcType();
        if (elementJdbcType instanceof StructJdbcType) {
            // @Struct 배열(STRUCT_ARRAY)은 여기서 등록하지 않는다 — 이 시점에는 요소 object
            // 타입의 이름을 알 수 없다. 그 이름은 집계 매핑을 다 읽어야 정해지므로
            // TiberoAggregateSupport.aggregateAuxiliaryDatabaseObjects 가 등록한다.
            // Oracle 도 같은 구조다(OracleArrayJdbcType 에 같은 취지의 주석이 있다).
            return;
        }
        final Dialect dialect = database.getDialect();
        final BasicPluralJavaType<?> pluralJavaType = (BasicPluralJavaType<?>) javaType;
        final JavaType<?> elementJavaType = pluralJavaType.getElementJavaType();
        final String arrayTypeName = typeName == null
                ? arrayTypeNameFor(elementJavaType, elementJdbcType, dialect)
                : typeName;
        final String elementType = typeConfiguration.getDdlTypeRegistry().getTypeName(
                elementJdbcType.getDdlTypeCode(),
                dialect.getSizeStrategy().resolveSize(
                        elementJdbcType, elementJavaType,
                        columnSize.getPrecision(), columnSize.getScale(), columnSize.getLength()),
                new BasicTypeImpl<>(elementJavaType, elementJdbcType));

        final UserDefinedArrayType udt = database.getDefaultNamespace().createUserDefinedArrayType(
                Identifier.toIdentifier(arrayTypeName),
                name -> new UserDefinedArrayType("orm", database.getDefaultNamespace(), name));
        udt.setArraySqlTypeCode(getDdlTypeCode());
        udt.setElementTypeName(elementType);
        udt.setElementSqlTypeCode(elementJdbcType.getDefaultSqlTypeCode());
        udt.setArrayLength(columnSize.getArrayLength() == null ? 127 : columnSize.getArrayLength());
    }

    // ------------------------------------------------------------------
    // VARRAY 타입 이름 규칙 — Oracle 과 같게 둔다
    // ------------------------------------------------------------------

    /**
     * 요소 타입에서 VARRAY 타입 이름을 만든다 — {@code String[]} → {@code StringArray}.
     *
     * <p>Oracle 과 같은 규칙을 쓴다. 스키마에 <b>전역 이름</b>으로 만들어지므로 같은 요소
     * 타입을 쓰는 여러 엔티티가 하나의 VARRAY 타입을 공유한다.
     */
    public static String arrayTypeNameFor(BasicType<?> elementType, Dialect dialect) {
        // 요소에 AttributeConverter 가 걸려 있으면 도메인 타입이 아니라 컨버터 이름으로 짓는다.
        // 도메인 타입으로 지으면 같은 관계형 표현을 쓰는 서로 다른 컨버터가 한 VARRAY 타입을
        // 공유하게 되어, 한쪽 매핑을 바꿀 때 다른 쪽 DDL 까지 흔들린다. Oracle 과 같은 규칙이다.
        final BasicValueConverter<?, ?> converter = elementType.getValueConverter();
        if (converter != null) {
            final String simpleName = converter instanceof JpaAttributeConverter<?, ?>
                    ? ((JpaAttributeConverter<?, ?>) converter).getConverterJavaType()
                            .getJavaTypeClass().getSimpleName()
                    : converter.getClass().getSimpleName();
            return dialect.getArrayTypeName(simpleName, null, null);
        }
        return arrayTypeNameFor(elementType.getJavaTypeDescriptor(), elementType.getJdbcType(), dialect);
    }

    public static String arrayTypeNameFor(JavaType<?> elementJavaType, JdbcType elementJdbcType, Dialect dialect) {
        final String simpleName;
        if (elementJavaType.getJavaTypeClass().isArray()) {
            // 배열의 배열 — 안쪽 배열 타입 이름을 먼저 만들고 그 위에 다시 Array 를 붙인다
            simpleName = dialect.getArrayTypeName(
                    elementJavaType.getJavaTypeClass().getComponentType().getSimpleName(), null, null);
        }
        else if (elementJdbcType instanceof StructJdbcType) {
            simpleName = ((StructJdbcType) elementJdbcType).getStructTypeName();
        }
        else {
            final Class<?> preferred = elementJdbcType.getPreferredJavaTypeClass(null);
            final Class<?> actual = elementJavaType.getJavaTypeClass();
            // 자바 타입과 드라이버가 선호하는 타입이 다르면 둘을 이어 붙여 이름이 겹치지 않게 한다
            simpleName = (preferred == null || preferred == actual)
                    ? actual.getSimpleName()
                    : actual.getSimpleName() + preferred.getSimpleName();
        }
        return dialect.getArrayTypeName(simpleName, null, null);
    }

    @Override
    public String getFriendlyName() {
        return typeName;
    }

    @Override
    public String toString() {
        return "TiberoArrayJdbcType(" + typeName + ")";
    }
}
