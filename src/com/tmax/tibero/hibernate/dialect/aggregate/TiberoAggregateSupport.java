package com.tmax.tibero.hibernate.dialect.aggregate;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.AuxiliaryDatabaseObject;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.dialect.aggregate.AggregateSupport;
import org.hibernate.dialect.aggregate.AggregateSupportImpl;
import org.hibernate.mapping.AggregateColumn;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.UserDefinedArrayType;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.type.BasicType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.ArrayJdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.StructJdbcType;

import org.hibernate.type.spi.TypeConfiguration;

import java.util.List;
import java.util.Locale;

import static org.hibernate.type.SqlTypes.ARRAY;
import static org.hibernate.type.SqlTypes.JSON;
import static org.hibernate.type.SqlTypes.STRUCT;
import static org.hibernate.type.SqlTypes.STRUCT_ARRAY;
import static org.hibernate.type.SqlTypes.STRUCT_TABLE;

/**
 * 집계(aggregate) 컬럼 — 여러 필드를 <b>컬럼 하나</b>에 담는 매핑의 SQL 조각을 만든다.
 *
 * <h2>집계 컬럼이 무엇인가</h2>
 * Hibernate 는 임베더블({@code @Embeddable})을 두 가지 방식으로 테이블에 앉힐 수 있다.
 *
 * <pre>
 * &#64;Embedded            &#64;Struct(name = "ADDR_T")
 *   ADDR_STREET  varchar    ADDR  ADDR_T      &lt;- 컬럼 하나. DB 쪽 object 타입
 *   ADDR_CITY    varchar
 *   (컬럼으로 펼쳐짐)        (집계 컬럼)
 * </pre>
 *
 * 뒤쪽이 <b>집계 컬럼</b>이다. 컬럼은 하나인데 그 안에 필드가 여러 개 들어 있으므로,
 * {@code where p.addr.city = ?} 같은 질의를 SQL 로 옮기려면 <b>"컬럼 안의 한 필드"를
 * 어떻게 쓰는지</b>를 dialect 가 알려주어야 한다. 그 역할이 이 클래스다.
 *
 * <p>Hibernate 는 JSON · XML · STRUCT 를 모두 집계로 취급한다. 이 구현은 <b>STRUCT 계열만</b>
 * 다룬다 — JSON 집계({@code @JdbcTypeCode(JSON)} 임베더블)는 별개의 기계가 필요하고
 * 6.6.1 범위 밖이다.
 *
 * <h2>이 클래스가 없으면 무슨 일이 나는가</h2>
 * Hibernate 기본값인 {@link AggregateSupportImpl} 은 아래 훅들이 전부
 * {@code UnsupportedOperationException} 을 던진다. 그래서 {@code @Struct} 엔티티가 하나라도
 * 있으면 <b>SessionFactory 기동 자체가 실패</b>했다.
 *
 * <pre>
 * UnsupportedOperationException: Dialect does not support
 *   aggregateComponentAssignmentExpression: org.hibernate.dialect.aggregate.AggregateSupportImpl
 * </pre>
 *
 * <h2>왜 이렇게 짧은가</h2>
 * Hibernate 의 {@code OracleAggregateSupport} 는 566 줄이지만 <b>그중 STRUCT 분기는 6 줄</b>이고
 * 나머지는 전부 JSON 집계 기계다(버전별 {@code JsonSupport} 판정, 타입별 강제변환).
 * STRUCT 는 DB 가 {@code obj.field} 표기를 네이티브로 이해하므로 문자열을 이어붙이면 끝난다.
 *
 * @see com.tmax.tibero.hibernate.type.TiberoStructJdbcType  값을 실제로 바인딩·추출하는 쪽
 * @see com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator#visitSetAssignment
 *      UPDATE 의 SET 대상을 렌더하는 쪽 — 여기서 만든 식만으로는 부족해 함께 손봐야 했다
 */
public class TiberoAggregateSupport extends AggregateSupportImpl {

    public static final AggregateSupport INSTANCE = new TiberoAggregateSupport();

    /**
     * SELECT · WHERE 에서 집계 컬럼 <b>안의 한 필드</b>를 읽는 식.
     *
     * <p>{@code template} 은 {@code "?"} 같은 자리표시자를 품은 틀이고, {@code placeholder}
     * 자리에 실제 식을 끼워 넣어 돌려준다. Tibero 는 Oracle 과 같이 {@code 별칭.컬럼.필드}
     * 점 표기를 그대로 이해하므로 이어붙이기만 하면 된다.
     *
     * <pre>
     * HQL : select p.addr.city from Person p where p.addr.zip = 6236
     * SQL : select p1_0.addr.city from PERSON p1_0 where p1_0.addr.zip=6236
     *                     ^^^^^^^^^ 이 조각을 여기서 만든다
     * </pre>
     */
    @Override
    public String aggregateComponentCustomReadExpression(
            String template,
            String placeholder,
            String aggregateParentReadExpression,
            String columnExpression,
            AggregateColumn aggregateColumn,
            Column column) {
        switch (aggregateColumn.getTypeCode()) {
            case STRUCT:
            case STRUCT_ARRAY:
            case STRUCT_TABLE:
                return template.replace(placeholder, aggregateParentReadExpression + "." + columnExpression);
            case JSON:
                return template.replace(placeholder,
                        jsonReadExpression(aggregateParentReadExpression, columnExpression, column));
        }
        throw new IllegalArgumentException(
                "Tibero dialect supports STRUCT and JSON aggregates, but got SQL type: "
                        + aggregateColumn.getTypeCode());
    }

    /**
     * JSON 집계 컬럼 안의 필드 하나를 읽는 식.
     *
     * <p>STRUCT 는 DB 가 {@code addr.city} 를 네이티브로 이해하지만 JSON 은 그렇지 않다.
     * 컬럼에 담긴 것은 문자열이므로 <b>경로로 찔러 꺼내고 타입까지 되돌려야</b> 한다.
     *
     * <pre>
     * STRUCT   p1_0.addr.city
     * JSON     cast(json_value(p1_0.addr,'$.city') as varchar2(255 char))
     * </pre>
     *
     * <p>타입마다 되돌리는 방법이 다르다. Tibero 는 Oracle 이 쓰는 형태를
     * <b>한 글자도 고치지 않고 전부 받는다</b>(실측) — 읽기 경로에는 Tibero 고유 차이가 없다.
     *
     * <table>
     *   <tr><th>필드 타입</th><th>생성되는 식</th></tr>
     *   <tr><td>정수 계열</td><td>{@code json_value(…,'$.zip' returning number(10,0))}</td></tr>
     *   <tr><td>BOOLEAN(number)</td><td>{@code decode(json_value(…),'true',1,'false',0,null)}</td></tr>
     *   <tr><td>DATE</td><td>{@code to_date(json_value(…),'YYYY-MM-DD')}</td></tr>
     *   <tr><td>TIMESTAMP</td><td>{@code to_timestamp(json_value(…),'YYYY-MM-DD"T"hh24:mi:ss.FF9')}</td></tr>
     *   <tr><td>이진</td><td>{@code hextoraw(json_value(…))} — JSON 에는 hex 로 담긴다</td></tr>
     *   <tr><td>LOB</td><td>{@code (select * from json_table(…))} — {@code json_value} 로는 못 꺼낸다</td></tr>
     *   <tr><td>중첩 임베더블</td><td>{@code json_query(…,'$.inner' returning json)}</td></tr>
     *   <tr><td>그 외</td><td>{@code cast(json_value(…) as <타입>)}</td></tr>
     * </table>
     *
     * <p>⚠️ 여기 쓰는 형식 문자열은 <b>쓰기 쪽과 짝이 맞아야 한다</b>
     * ({@link TiberoJsonAggregateWriter}). 한쪽만 바꾸면 값이 왕복하지 않는다.
     *
     * <p>배열 필드는 {@link #arrayReadExpression} 이 따로 맡는다. Oracle 은
     * {@code <타입>_from_json} PL/SQL 헬퍼를 스키마에 만들어 쓰는데 그 헬퍼 계열이
     * Tibero 서버를 멈추게 하므로, 헬퍼 없이 {@code json_table} + {@code multiset} 만으로
     * 푸는 경로를 따로 만들었다.
     */
    private static String jsonReadExpression(
            String parentReadExpression, String columnExpression, Column column) {

        // 중첩 집계면 부모가 이미 json_query(...) 다. 그걸 벗겨 '$.inner.city' 처럼 경로
        // 하나로 이어 붙인다. Tibero 는 겹쳐 쓴 json_value(json_query(...),'$.city') 도
        // 받아 주므로(실측) 정확성 문제는 아니지만, 중첩이 깊어질수록 식이 한 겹씩
        // 불어나 읽기 어려워지고 파스도 비싸진다
        final String parent;
        if (parentReadExpression.startsWith(JSON_QUERY_START)
                && parentReadExpression.endsWith(JSON_QUERY_END)) {
            parent = parentReadExpression.substring(
                    JSON_QUERY_START.length(),
                    parentReadExpression.length() - JSON_QUERY_END.length()) + ".";
        }
        else {
            parent = parentReadExpression + ",'$.";
        }

        switch (column.getTypeCode()) {
            case SqlTypes.BOOLEAN:
                if (column.getTypeName().toLowerCase(Locale.ROOT).trim().startsWith("number")) {
                    return "decode(json_value(" + parent + columnExpression + "'),'true',1,'false',0,null)";
                }
                // number 가 아니면 정수 계열과 같게 처리한다
            case SqlTypes.TINYINT:
            case SqlTypes.SMALLINT:
            case SqlTypes.INTEGER:
            case SqlTypes.BIGINT:
                return "json_value(" + parent + columnExpression + "' returning " + column.getTypeName() + ')';
            case SqlTypes.DATE:
                return "to_date(json_value(" + parent + columnExpression + "'),'YYYY-MM-DD')";
            case SqlTypes.TIME:
                return "to_timestamp(json_value(" + parent + columnExpression + "'),'hh24:mi:ss')";
            case SqlTypes.TIMESTAMP:
                return "to_timestamp(json_value(" + parent + columnExpression
                        + "'),'YYYY-MM-DD\"T\"hh24:mi:ss.FF9')";
            case SqlTypes.TIMESTAMP_WITH_TIMEZONE:
            case SqlTypes.TIMESTAMP_UTC:
                return "to_timestamp_tz(json_value(" + parent + columnExpression
                        + "'),'YYYY-MM-DD\"T\"hh24:mi:ss.FF9TZH:TZM')";
            case SqlTypes.BINARY:
            case SqlTypes.VARBINARY:
            case SqlTypes.LONG32VARBINARY:
                // 이진 값은 JSON 에 hex 문자열로 담기므로 되돌려야 한다
                return "hextoraw(json_value(" + parent + columnExpression + "'))";
            case SqlTypes.CLOB:
            case SqlTypes.NCLOB:
            case SqlTypes.BLOB:
                // json_value 는 LOB 을 못 돌려준다. json_table 로 컬럼을 만들어 꺼낸다
                return "(select * from json_table(" + parentReadExpression + ",'$' columns ("
                        + columnExpression + " " + column.getTypeName()
                        + " path '$." + columnExpression + "')))";
            case SqlTypes.ARRAY:
                return arrayReadExpression(parent, columnExpression, column);
            case SqlTypes.JSON:
                return "json_query(" + parent + columnExpression + "' returning "
                        + jsonTypeNameOf(column) + ")";
            default:
                return "cast(json_value(" + parent + columnExpression + "') as " + column.getTypeName() + ')';
        }
    }

    /**
     * JSON 배열을 VARRAY 로 되돌리는 식 — <b>Oracle 과 방식이 다르다</b>.
     *
     * <h2>왜 Oracle 방식을 못 쓰나</h2>
     * Oracle 은 {@code json_value(w,'$.tags' returning StringArray)} 로 한 번에 꺼내지만
     * Tibero 는 {@code json_value} 의 {@code returning} 에 UDT 를 못 쓴다. 그렇다고
     * 캐스팅으로 우회할 수도 없다.
     *
     * <pre>
     * json_value(w,'$.tags' returning StringArray)   JDBC-8004  Syntax error
     * cast(json_value(w,'$.tags') as StringArray)    JDBC-11021 Error occurred during type casting
     * </pre>
     *
     * <p>대신 {@code json_table} 로 배열을 <b>행으로 펼친 뒤</b> {@code multiset} 으로 모아
     * VARRAY 로 캐스팅한다. 이건 동작한다.
     *
     * <pre>
     * cast(multiset(select jt.v from json_table(w,'$.tags[*]'
     *                 columns (v varchar2(4000) path '$')) jt) as StringArray)
     *   → StringArray('a','b')
     * </pre>
     *
     * <h2>왜 원소를 전부 {@code varchar2(4000)} 으로 읽나</h2>
     * 바깥 {@code cast} 가 VARRAY 의 진짜 원소 타입으로 다시 변환해 주기 때문이다
     * (ps06 실측 — {@code NumberArray}, {@code DoubleArray} 모두 정상). 덕분에 원소 타입별
     * DDL 이름을 따로 알아낼 필요가 없다. 이 훅에는 {@code TypeConfiguration} 이 넘어오지
     * 않아 원소 타입 이름을 계산하기가 번거로운데, 그 문제를 통째로 피한다.
     *
     * <p>키가 없으면 빈 VARRAY 가 나온다 — 예외가 아니다.
     *
     * <p>원소가 boolean 인 배열만 예외로 {@code decode} 를 한 겹 두른다. JSON 에는 참/거짓이
     * 들어 있지만 VARRAY 원소는 {@code number(1,0)} 이라 그대로 캐스팅하면
     * {@code JDBC-5074 Given string does not represent a number in proper format} 이 난다.
     * 원소 타입은 {@link #elementSqlTypeCodeOf(Column)} 으로 알아낸다.
     *
     * <p>실측으로 {@code String[]} · {@code Integer[]} · {@code Double[]} · {@code LocalDate[]} ·
     * {@code Boolean[]} 다섯 가지가 읽기·쓰기 양쪽 모두 왕복함을 확인했다.
     */
    private static String arrayReadExpression(String parent, String columnExpression, Column column) {
        final String element = elementSqlTypeCodeOf(column) == SqlTypes.BOOLEAN
                // JSON 에는 참/거짓이 "true"/"false" 문자열로 들어 있는데 VARRAY 원소는
                // number(1,0) 이다. 그대로 캐스팅하면 JDBC-5074 가 난다
                ? "decode(jt.v,'true',1,'false',0,null)"
                : "jt.v";
        return "cast(multiset(select " + element + " from json_table(" + parent + columnExpression
                + "[*]' columns (v varchar2(4000) path '$')) jt) as " + column.getTypeName() + ')';
    }

    /**
     * 배열 컬럼의 <b>원소</b> SQL 타입 코드. 알아낼 수 없으면 {@link SqlTypes#OTHER}.
     *
     * <p>이 훅에는 {@code TypeConfiguration} 이 넘어오지 않지만, 부트 모델의
     * {@code Column} 에서 매핑 타입을 거슬러 올라가면 원소의 JDBC 타입까지는 닿는다.
     */
    private static int elementSqlTypeCodeOf(Column column) {
        final org.hibernate.mapping.Value value = column.getValue();
        if (value instanceof org.hibernate.mapping.BasicValue) {
            final org.hibernate.type.descriptor.jdbc.JdbcType jdbcType =
                    ((org.hibernate.mapping.BasicValue) value).getResolution().getJdbcType();
            if (jdbcType instanceof org.hibernate.type.descriptor.jdbc.ArrayJdbcType) {
                return ((org.hibernate.type.descriptor.jdbc.ArrayJdbcType) jdbcType)
                        .getElementJdbcType().getDefaultSqlTypeCode();
            }
        }
        return SqlTypes.OTHER;
    }

    /** 중첩 집계 벗기기에 쓰는 접두/접미. */
    private static final String JSON_QUERY_START = "json_query(";
    private static final String JSON_QUERY_END = "' returning json)";

    /** 중첩 집계 컬럼의 {@code returning} 타입 — 명시가 없으면 Tibero 네이티브 {@code json}. */
    private static String jsonTypeNameOf(Column column) {
        final String sqlType = column.getSqlType();
        return sqlType == null ? "json" : sqlType;
    }

    /**
     * UPDATE 의 SET 왼쪽에서 집계 컬럼 <b>안의 한 필드</b>를 가리키는 식.
     *
     * <p>읽기와 달리 <b>부분 갱신이 가능한지</b>가 타입마다 다르다. JSON 은 객체를 통째로
     * 바꿔야 하지만(그래서 Oracle 은 JSON 일 때 부모 식을 그대로 돌려준다), STRUCT 는
     * 필드 하나만 지정해 바꿀 수 있다.
     *
     * <pre>
     * HQL : update Person p set p.addr.city = 'Busan'
     * SQL : update PERSON p1_0 set p1_0.addr.city='Busan'
     * </pre>
     *
     * <p>⚠️ 여기서 돌려주는 것은 {@code addr.city} 까지이고 <b>앞의 별칭은 붙지 않는다.</b>
     * 별칭 없이 나가면 Tibero 가 {@code JDBC-8026 Invalid identifier} 로 거부하므로
     * {@link com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator#visitSetAssignment}
     * 에서 별칭을 붙인다. 둘이 한 쌍이다.
     */
    @Override
    public String aggregateComponentAssignmentExpression(
            String aggregateParentAssignmentExpression,
            String columnExpression,
            AggregateColumn aggregateColumn,
            Column column) {
        switch (aggregateColumn.getTypeCode()) {
            case STRUCT:
            case STRUCT_ARRAY:
            case STRUCT_TABLE:
                return aggregateParentAssignmentExpression + "." + columnExpression;
            case JSON:
                // JSON 은 성분만 콕 집어 바꿀 수 없다 — 컬럼을 통째로 다시 쓴다.
                // 실제 오른쪽 식은 TiberoJsonAggregateWriter 가 만든다
                return aggregateParentAssignmentExpression;
        }
        throw new IllegalArgumentException(
                "Tibero dialect supports STRUCT and JSON aggregates, but got SQL type: "
                        + aggregateColumn.getTypeCode());
    }

    /**
     * 집계 컬럼을 갱신할 때 <b>전용 렌더러</b>가 필요한지.
     *
     * <p>{@code true} 면 Hibernate 가 {@code aggregateCustomWriteExpressionRenderer()} 로
     * 받아온 렌더러에게 SET 절 생성을 통째로 맡긴다. JSON 은 {@code json_mergepatch(...)} 같은
     * 함수로 감싸야 해서 그 경로가 필요하지만, STRUCT 는 위 {@code aggregateComponentAssignmentExpression}
     * 이 만든 {@code addr.city=?} 만으로 충분하다. Oracle 도 JSON 일 때만 {@code true} 다.
     *
     * <p>기본 구현이 이 메서드에서 throw 하므로 <b>값을 바꾸려는 게 아니라 기동을 살리려고</b>
     * 명시한다.
     */
    @Override
    public boolean requiresAggregateCustomWriteExpressionRenderer(int aggregateSqlTypeCode) {
        return aggregateSqlTypeCode == SqlTypes.JSON;
    }

    /**
     * JSON 집계의 SET 오른쪽을 통째로 그리는 렌더러.
     *
     * <p>{@code json_mergepatch(nvl(컬럼, 빈객체), 바꿀것만 returning json)} 을 만든다.
     * 자세한 설계와 Oracle 과 갈라지는 세 지점은 {@link TiberoJsonAggregateWriter} 에 있다.
     *
     * <p>STRUCT 는 이 경로를 타지 않는다 — 위 {@code requires…} 가 STRUCT 에는
     * {@code false} 를 돌려주므로 Hibernate 가 여기까지 오지 않는다. 혹시라도 오면
     * 조용히 잘못 도는 대신 명확한 메시지로 실패시킨다.
     */
    @Override
    public WriteExpressionRenderer aggregateCustomWriteExpressionRenderer(
            SelectableMapping aggregateColumn,
            SelectableMapping[] columnsToUpdate,
            TypeConfiguration typeConfiguration) {

        final int typeCode = aggregateColumn.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode();
        if (typeCode == SqlTypes.JSON) {
            return new TiberoJsonAggregateWriter.Root(aggregateColumn, columnsToUpdate, typeConfiguration);
        }
        throw new IllegalArgumentException(
                "Only JSON aggregates need a custom write renderer on Tibero, but got SQL type: " + typeCode);
    }

    /**
     * {@code @Struct} 임베더블의 <b>배열</b> 필드에 필요한 VARRAY 타입을 등록한다.
     *
     * <h2>무엇을 가능하게 하나</h2>
     * {@code @Struct} 값 객체를 배열로 가진 필드({@code Address[] stops})를 object UDT 의
     * VARRAY 컬럼 하나에 담는다.
     *
     * <pre>
     * create type SMT_ADDR      as object (city varchar2(255), zip number(10,0))
     * create type SMT_ADDRArray as varying array(127) of SMT_ADDR   &lt;- 이 줄이 여기서 나온다
     * create table TRIP (id number(19,0), stops SMT_ADDRArray)
     * </pre>
     *
     * <h2>왜 여기서 등록하나</h2>
     * 보통 배열 컬럼의 UDT 는 {@code TiberoArrayJdbcType.addAuxiliaryDatabaseObjects} 가
     * 등록한다. 그런데 <b>요소가 struct 이면 그쪽은 등록을 건너뛴다</b> — 그 시점에는
     * 요소 object 타입의 이름을 알 수 없기 때문이다. 요소 타입 이름은 집계 매핑을 다 읽고
     * 나야 정해지므로 {@code AggregateSupport} 가 맡는 것이 맞다. Oracle 도 같은 구조이며
     * {@code OracleArrayJdbcType} 에 <i>"OracleAggregateSupport will take care of
     * contributing the auxiliary database object"</i> 라는 주석으로 남아 있다.
     *
     * <h2>Tibero 지원 실측</h2>
     * object 타입의 VARRAY 는 전부 동작한다(ps06).
     *
     * <pre>
     * create type SA_OBJ as object (a number, b varchar2(20))            OK
     * create type SA_ARR as varying array(10) of SA_OBJ                  OK
     * create table SA_T (id number, v SA_ARR)                            OK
     * insert into SA_T values (1, SA_ARR(SA_OBJ(1,'x'), SA_OBJ(2,'y')))  OK
     * select ... from SA_T t, table(t.v) e where e.a = 1                 OK
     * </pre>
     *
     * <h2>⚠️ {@code STRUCT_TABLE}(nested table)은 등록하지 않는다</h2>
     * Oracle 은 여기서 {@code STRUCT_TABLE} 도 함께 처리하지만 <b>Tibero 는 nested table 을
     * 컬럼 타입으로 받지 않는다.</b> 타입 선언까지는 되는데 컬럼으로 쓰면 거부한다(실측).
     *
     * <pre>
     * create type NTA as table of number                  OK
     * create table T (id number, v NTA)                   FAIL  JDBC-7002 Unsupported DDL
     * create table T (v NTA) nested table v store as S    FAIL  JDBC-7002 Unsupported DDL
     * </pre>
     *
     * 등록해봐야 쓸 수 없는 DDL 이 나가므로 <b>일부러 {@code STRUCT_ARRAY} 만</b> 처리한다.
     * {@code STRUCT_TABLE} 은 타입이 만들어지지 않아 부팅 단계에서 드러나는데,
     * 조용히 잘못 도는 것보다 낫다.
     *
     * @see com.tmax.tibero.hibernate.type.TiberoArrayJdbcType#addAuxiliaryDatabaseObjects
     */
    @Override
    public List<AuxiliaryDatabaseObject> aggregateAuxiliaryDatabaseObjects(
            Namespace namespace,
            String aggregatePath,
            AggregateColumn aggregateColumn,
            List<Column> aggregatedColumns) {

        if (aggregateColumn.getTypeCode() == STRUCT_ARRAY) {
            registerStructArrayType(namespace, aggregateColumn);
        }
        return super.aggregateAuxiliaryDatabaseObjects(
                namespace, aggregatePath, aggregateColumn, aggregatedColumns);
    }

    /**
     * {@code varying array(n) of <object 타입>} 을 부트 모델에 넣는다.
     *
     * <p>상한을 안 적었으면 <b>127</b> 로 둔다 — 일반 배열 컬럼과 같은 기본값이고 Oracle 과도 같다.
     */
    private static void registerStructArrayType(Namespace namespace, AggregateColumn aggregateColumn) {
        final JdbcType jdbcType = ((BasicType<?>) aggregateColumn.getValue().getType()).getJdbcType();
        if (!(jdbcType instanceof ArrayJdbcType)) {
            return;
        }
        final JdbcType elementJdbcType = ((ArrayJdbcType) jdbcType).getElementJdbcType();
        if (!(elementJdbcType instanceof StructJdbcType)) {
            return;
        }
        final StructJdbcType elementStructType = (StructJdbcType) elementJdbcType;

        final UserDefinedArrayType arrayType = namespace.createUserDefinedArrayType(
                Identifier.toIdentifier(aggregateColumn.getSqlType()),
                name -> new UserDefinedArrayType("orm", namespace, name));
        arrayType.setArraySqlTypeCode(ARRAY);
        arrayType.setArrayLength(aggregateColumn.getArrayLength() == null
                ? 127 : aggregateColumn.getArrayLength());
        arrayType.setElementTypeName(elementStructType.getStructTypeName());
        arrayType.setElementSqlTypeCode(elementStructType.getDefaultSqlTypeCode());
    }


    /**
     * 집계 컬럼 <b>안의 성분</b>에 대한 check 제약을 만들지 않는다.
     *
     * <p>Hibernate 기본값은 {@code true} 이고, 임베더블에 {@code boolean} 이나
     * {@code @Enumerated(STRING)} 필드가 있으면 테이블에 이런 제약을 붙인다.
     *
     * <pre>
     * create table T (
     *   id number(19,0) not null, mix MIX_T, primary key (id),
     *   check (mix is null or ((mix.flag is null or mix.flag in (0,1))
     *                      and (mix.kind is null or mix.kind in ('HOME','WORK'))))
     * )
     * </pre>
     *
     * <p><b>Tibero 는 이 DDL 을 거부한다</b> — 정확히는 앞머리의 {@code mix is null} 이 문제다.
     * UDT 컬럼 자체를 식으로 참조하는 것을 check 제약 안에서 허용하지 않는다.
     *
     * <pre>
     * check (mix.flag is null or mix.flag in (0,1))   OK    성분만 참조
     * check (mix is null)                             FAIL  JDBC-8147
     *   Expressions that return a user-defined type is not allowed in the current context.
     * </pre>
     *
     * <p>Hibernate 는 항상 {@code &lt;컬럼&gt; is null or (...)} 로 감싸고 그 부분만 빼는 훅은
     * 없으므로 <b>제약 생성 자체를 끈다.</b> 켜 두면 {@code create table} 이 통째로 실패하는데,
     * {@code hbm2ddl.auto} 는 기본적으로 DDL 오류를 삼키므로 <b>테이블이 조용히 만들어지지 않고</b>
     * 첫 질의에서 {@code JDBC-8033 Specified schema object was not found} 이 난다 —
     * 원인을 찾기 매우 어려운 형태다.
     *
     * <p><b>JSON 집계는 사정이 다르다</b> — Hibernate 가 만드는 DDL 이 Tibero 에서 그대로
     * 동작한다(ps06 실측). 그런데 이 훅은 <b>타입별이 아니라 dialect 전역</b>이라
     * ({@code supportsComponentCheckConstraints()} 에 인자가 없다) JSON 만 켤 수가 없다.
     * STRUCT 와 JSON 을 한 애플리케이션에서 같이 쓰면 STRUCT 쪽 {@code create table} 이
     * 깨지므로, 더 안전한 쪽인 <b>끔</b>을 택한다.
     *
     * <pre>
     * JSON   check (addr is null or (cast(json_value(addr,'$.city') as varchar2(255 char)) is not null))
     *        → 생성 OK, 위반 INSERT 는 JDBC-10006 으로 막히고, addr 자체가 null 인 행은 통과 ✔
     * STRUCT check (addr is null or (addr.city is not null))        → JDBC-8147 로 생성 실패 ✘
     *        check (addr.city is not null)      (is null 가드 없이)  → 생성은 되지만
     *        addr 컬럼이 null 인 행까지 막아버려 의미가 달라진다 ✘
     * </pre>
     *
     * <p>잃는 것은 boolean · enum 성분의 <b>DB 수준 검증</b>뿐이다. 애플리케이션이
     * Hibernate 를 통해 쓰는 값은 어차피 매핑이 보장하며, {@code @Embedded} 로 펼친 경우에는
     * 컬럼별 제약이 정상 생성된다.
     */
    @Override
    public boolean supportsComponentCheckConstraints() {
        return false;
    }
}
