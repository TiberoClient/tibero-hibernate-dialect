package com.tmax.tibero.hibernate.tool.schema.internal;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.UserDefinedArrayType;
import org.hibernate.tool.schema.internal.StandardUserDefinedTypeExporter;

import static org.hibernate.type.SqlTypes.TABLE;

/**
 * 사용자 정의 타입(UDT)의 DDL 을 만든다 — object 타입과 array 타입 둘 다.
 *
 * <h2>왜 필요한가</h2>
 * Hibernate 기본 {@link StandardUserDefinedTypeExporter} 는 object UDT 만 처리하고
 * <b>array UDT 에서는 예외를 던진다.</b>
 *
 * <pre>IllegalArgumentException: Exporter does not support name array types.</pre>
 *
 * <p>그래서 배열 컬럼을 지원하려면 반드시 교체해야 한다. object UDT 경로({@code @Struct})는
 * 상위 구현을 그대로 물려받으므로 이 클래스는 <b>array 부분만</b> 채운다.
 *
 * <h2>Oracle 처럼 PL/SQL 헬퍼를 만들지 않는다 — 의도된 축소</h2>
 * Hibernate {@code OracleUserDefinedTypeExporter} 는 배열 타입마다 PL/SQL 함수 <b>18개</b>를
 * 함께 만든다({@code _length} · {@code _concat} · {@code _position} …). HQL 의
 * {@code array_*} 함수가 그것들을 호출하는 구조다.
 *
 * <p>그 헬퍼들을 Tibero 에 적용해 보았고, <b>DB 가 반복적으로 응답 불능에 빠졌다</b>.
 *
 * <ul>
 *   <li>{@code <타입>_concat} 은 컴파일은 되지만 <b>두 번째 호출부터 서버 워커가 멈춘다</b>.
 *       한 번 걸리면 그 세션의 모든 후속 문장이 응답하지 않고, 해당 함수는
 *       {@code drop} 도 {@code create or replace} 도 되지 않아 <b>인스턴스 재기동으로만</b>
 *       풀린다. 본문이 {@code select … bulk collect into … from (… union all …)} 로
 *       {@code table()} 을 5번 펼치는 형태다.</li>
 *   <li>{@code <타입>_positions} 는 Oracle Spatial 의 {@code sdo_ordinate_array} 를 반환하는데
 *       Tibero 에 그 타입이 없어 INVALID 로 남는다.</li>
 *   <li>배열 타입 3개분(51개 함수)을 한 번에 만들고 지우는 과정에서도
 *       {@code SchemaDropperImpl.dropUserDefinedTypes} 가 응답하지 않는 경우가 관측됐다.</li>
 * </ul>
 *
 * <p>애플리케이션에 노출하면 <b>DB 를 멈추게 하는</b> 종류의 고장이므로, 헬퍼 생성을 빼고
 * <b>배열 타입 선언만</b> 만든다. {@code TiberoDialect} 도 {@code array_*} 함수를 등록하지
 * 않아 HQL 파싱 단계에서 걸리게 한다 — 런타임에 DB 가 멈추는 것보다 낫다.
 *
 * <p>배열 컬럼 자체는 이 축소와 무관하게 온전히 동작한다 — 매핑·왕복·{@code table()} 언네스트
 * 모두 정상이다. 잃는 것은 HQL {@code array_*} 함수뿐이고, 네이티브 SQL 로는 {@code table()} 을
 * 써서 같은 일을 할 수 있다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#getUserDefinedTypeExporter
 * @see com.tmax.tibero.hibernate.type.TiberoArrayJdbcType
 */
public class TiberoUserDefinedTypeExporter extends StandardUserDefinedTypeExporter {

    public TiberoUserDefinedTypeExporter(Dialect dialect) {
        super(dialect);
    }

    /**
     * 배열 UDT 선언.
     *
     * <pre>
     * varying array   create or replace type StringArray as varying array(10) of varchar2(255 char)
     * nested table    create or replace type StringArray as table of varchar2(255 char)
     * </pre>
     *
     * <p>{@code arraySqlTypeCode} 가 비어 있거나 {@code TABLE} 이면 nested table 로 낸다 —
     * Hibernate 가 그렇게 구분한다. 현재 {@code TiberoArrayJdbcType} 은 varying array 만
     * 만들지만, nested table 경로를 막아둘 이유가 없어 상위 규약대로 처리한다.
     */
    @Override
    public String[] getSqlCreateStrings(
            UserDefinedArrayType userDefinedType, Metadata metadata, SqlStringGenerationContext context) {
        final String typeName = context.format(qualifiedName(userDefinedType));
        final Integer arraySqlTypeCode = userDefinedType.getArraySqlTypeCode();
        final String elementType = userDefinedType.getElementTypeName();
        if (arraySqlTypeCode == null || arraySqlTypeCode == TABLE) {
            return new String[]{"create or replace type " + typeName + " as table of " + elementType};
        }
        return new String[]{
                "create or replace type " + typeName
                        + " as varying array(" + userDefinedType.getArrayLength() + ") of " + elementType};
    }

    /**
     * 배열 UDT 삭제.
     *
     * <p>{@code force} 를 붙인다 — 그 타입을 컬럼으로 쓰는 테이블이 먼저 지워지지 않았을 때
     * 의존성 때문에 실패하는 것을 막는다. Oracle 도 같다.
     *
     * <h2>{@code if exists} 를 붙이는 이유</h2>
     * {@code hbm2ddl.auto=create-drop} 은 <b>만들기 전에 먼저 지운다.</b> 첫 실행에는 지울
     * 타입이 없으므로 {@code drop type X force} 가 {@code JDBC-7071} 로 실패한다. 평소에는
     * Hibernate 가 그 오류를 삼켜 넘어가지만, <b>{@code hbm2ddl.halt_on_error=true} 를 켜면
     * 첫 실행이 통째로 죽는다.</b>
     *
     * <p>그 설정은 DDL 문제를 조사할 때 반드시 켜야 하는 것이라(§4.2 의 check 제약 결함을
     * 그것 없이는 못 찾았다) 첫 실행에서 죽으면 곤란하다. Tibero 는 다행히
     * {@code drop type if exists} 를 받는다(실측).
     *
     * <pre>
     * drop type NOPE_T force              FAIL  JDBC-7071
     * drop type if exists NOPE_T force    OK
     * </pre>
     *
     * <p>object UDT 쪽은 Hibernate 가 이미 {@code Dialect.supportsIfExistsBeforeTypeName()}
     * 훅을 보고 붙여 준다. 배열 UDT 는 그 경로를 안 타므로 여기서 같은 훅을 읽어 맞춘다 —
     * 두 종류의 UDT 가 서로 다르게 동작하면 안 된다.
     */
    @Override
    public String[] getSqlDropStrings(
            UserDefinedArrayType userDefinedType, Metadata metadata, SqlStringGenerationContext context) {
        final String ifExists = dialect.supportsIfExistsBeforeTypeName() ? "if exists " : "";
        return new String[]{
                "drop type " + ifExists + context.format(qualifiedName(userDefinedType)) + " force"};
    }

    private static QualifiedName qualifiedName(UserDefinedArrayType udt) {
        return new QualifiedNameParser.NameParts(
                Identifier.toIdentifier(udt.getCatalog(), udt.isCatalogQuoted()),
                Identifier.toIdentifier(udt.getSchema(), udt.isSchemaQuoted()),
                udt.getNameIdentifier());
    }
}
