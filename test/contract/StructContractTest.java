package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport;
import com.tmax.tibero.hibernate.type.TiberoStructJdbcType;
import org.hibernate.dialect.aggregate.AggregateSupport;
import org.hibernate.mapping.AggregateColumn;
import org.hibernate.mapping.Column;
import org.hibernate.tool.schema.internal.StandardUserDefinedTypeExporter;
import org.hibernate.type.SqlTypes;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code @Struct} 지원의 계약 (DB 불필요).
 *
 * <p>동작 검증은 {@code capability.StructMappingTest} 가 한다. 여기서는 <b>지원 범위의 경계</b>를
 * 고정한다 — 특히 JSON 집계처럼 <b>아직 안 하는 것</b>이 조용히 통과하지 않도록 한다.
 */
public class StructContractTest {

    private final TiberoDialect dialect = new TiberoDialect();
    private final AggregateSupport support = TiberoAggregateSupport.INSTANCE;

    @Test
    public void dialect_wiresStructSupport() {
        assertTrue(dialect.getAggregateSupport() instanceof TiberoAggregateSupport);
        assertEquals("object", dialect.getCreateUserDefinedTypeKindString());
        assertNotNull(TiberoStructJdbcType.INSTANCE);
        assertEquals(SqlTypes.STRUCT, TiberoStructJdbcType.INSTANCE.getJdbcTypeCode());
    }

    /** object UDT DDL 은 표준 exporter 로 충분하다 — array UDT 를 할 때만 전용 exporter 가 필요하다. */
    @Test
    public void userDefinedTypeExporter_staysStandard() {
        assertTrue(dialect.getUserDefinedTypeExporter() instanceof StandardUserDefinedTypeExporter);
    }

    /**
     * 전용 write 렌더러는 <b>JSON 에만</b> 필요하다.
     *
     * <p>{@code true} 를 돌려주면 Hibernate 가 SET 절 생성을
     * {@code aggregateCustomWriteExpressionRenderer()} 에게 통째로 맡긴다.
     *
     * <ul>
     *   <li><b>STRUCT</b> — DB 가 {@code addr.city = ?} 를 네이티브로 이해하므로 불필요하다.
     *       켜 두면 렌더러가 STRUCT 를 JSON 처럼 그려 UPDATE 가 깨진다.</li>
     *   <li><b>JSON</b> — 컬럼에 담긴 것은 문자열 한 덩어리라 성분만 콕 집어 바꿀 수 없다.
     *       {@code json_mergepatch(...)} 로 객체를 새로 만들어 덮어써야 한다.</li>
     * </ul>
     *
     * <p>Oracle 도 같은 구분을 쓴다.
     */
    @Test
    public void onlyJsonRequiresCustomWriteRenderer() {
        assertFalse("STRUCT 는 점 표기로 충분하다",
                support.requiresAggregateCustomWriteExpressionRenderer(SqlTypes.STRUCT));
        assertFalse(support.requiresAggregateCustomWriteExpressionRenderer(SqlTypes.STRUCT_ARRAY));
        assertTrue("JSON 은 mergepatch 로 통째로 다시 써야 한다",
                support.requiresAggregateCustomWriteExpressionRenderer(SqlTypes.JSON));
    }

    /**
     * JSON 집계는 이제 <b>지원된다</b> — 이 테스트는 예전에 "거부되는가"를 보던 자리다.
     *
     * <p>{@code @JdbcTypeCode(JSON)} 을 임베더블에 붙이면 이 경로로 온다. 세 훅이 각각
     * 무엇을 돌려주는지 고정한다. 실제 동작 검증은 {@code capability.JsonAggregateMappingTest}
     * 가 DB 에 붙어서 한다.
     */
    @Test
    public void jsonAggregate_isSupported() {
        final AggregateColumn jsonColumn = aggregateColumn("payload", SqlTypes.JSON);

        // 쓰기: 성분만 따로 지정할 수 없으므로 부모 식을 그대로 돌려준다.
        // 실제 오른쪽 식은 TiberoJsonAggregateWriter 가 만든다
        assertEquals("p1_0.payload",
                support.aggregateComponentAssignmentExpression("p1_0.payload", "city", jsonColumn, null));

        // 읽기: json_value 로 꺼내고 컬럼 타입으로 캐스팅한다
        final String read = support.aggregateComponentCustomReadExpression(
                "?", "?", "p1_0.payload", "city", jsonColumn, varcharColumn("city"));
        assertTrue("json_value 로 꺼내야 함: " + read, read.contains("json_value(p1_0.payload,'$.city')"));
    }

    /**
     * 중첩 임베더블의 읽기 경로가 <b>한 겹으로 납작해지는지</b>.
     *
     * <p>중첩 성분을 읽을 때 부모 식은 이미 {@code json_query(w,'$.inner' returning json)} 이다.
     * 그대로 감싸면 {@code json_value(json_query(w,'$.inner' returning json),'$.label')} 이 되고,
     * 3단이면 {@code json_query} 가 두 겹 쌓인다.
     *
     * <p>Tibero 는 겹쳐 쓴 형태도 <b>정상 동작한다</b>(실측). 그래서 이건 정확성이 아니라
     * 식 크기 문제다 — 중첩 깊이마다 한 겹씩 불어나면 SQL 이 읽기 어려워지고 파스도 비싸진다.
     * 동작으로는 드러나지 않으니 캡처된 SQL 로 고정한다.
     */
    @Test
    public void nestedJsonRead_flattensThePath() {
        final AggregateColumn jsonColumn = aggregateColumn("payload", SqlTypes.JSON);
        final String parent = "json_query(p1_0.payload,'$.inner' returning json)";

        final String read = support.aggregateComponentCustomReadExpression(
                "?", "?", parent, "label", jsonColumn, varcharColumn("label"));

        assertTrue("경로가 '$.inner.label' 한 번으로 합쳐져야 함: " + read,
                read.contains("json_value(p1_0.payload,'$.inner.label')"));
        assertFalse("json_query 가 남아 겹치면 안 됨: " + read, read.contains("json_query"));
    }

    /**
     * 지원하지 않는 집계 타입은 <b>조용히 통과하면 안 된다</b>.
     *
     * <p>STRUCT 와 JSON 외의 코드가 들어오면 무엇이 잘못됐는지 알 수 있는 메시지와 함께
     * 실패해야 한다. 잘못된 SQL 을 만들어 내보내는 것보다 낫다.
     */
    @Test
    public void unsupportedAggregateType_failsWithClearMessage() {
        final AggregateColumn odd = aggregateColumn("payload", SqlTypes.XML_ARRAY);
        try {
            support.aggregateComponentAssignmentExpression("p1_0.payload", "city", odd, null);
            fail("지원하지 않는 타입이 통과함");
        }
        catch (IllegalArgumentException expected) {
            assertTrue("메시지에 지원 범위가 드러나야 함: " + expected.getMessage(),
                    expected.getMessage().contains("STRUCT") && expected.getMessage().contains("JSON"));
        }
    }

    /** STRUCT 계열 세 코드는 모두 같은 점 표기를 쓴다. */
    @Test
    public void structFamily_rendersDotNotation() {
        for (int code : new int[]{SqlTypes.STRUCT, SqlTypes.STRUCT_ARRAY, SqlTypes.STRUCT_TABLE}) {
            AggregateColumn col = aggregateColumn("addr", code);
            assertEquals("p1_0.addr.city",
                    support.aggregateComponentAssignmentExpression("p1_0.addr", "city", col, null));
            assertEquals("p1_0.addr.city",
                    support.aggregateComponentCustomReadExpression("?", "?", "p1_0.addr", "city", col, null));
        }
    }

    /**
     * {@code @Struct} 배열(STRUCT_ARRAY)은 이제 <b>지원된다</b>.
     *
     * <p>이름 규칙과 exporter 는 네이티브 배열 컬럼 작업에서 이미 들어와 있었고, 마지막으로
     * 빠져 있던 <b>UDT 등록</b>을 {@code TiberoAggregateSupport.aggregateAuxiliaryDatabaseObjects}
     * 가 맡으면서 완성됐다.
     *
     * <p>동작 검증은 {@code capability.StructArrayMappingTest} 가 한다. 여기서는
     * 이름 규칙만 고정한다.
     */
    @Test
    public void structArray_nameRuleIsStable() {
        assertEquals("요소 object 타입 이름 + Array",
                "SMT_ADDRArray", dialect.getArrayTypeName(null, "SMT_ADDR", null));
    }

    /**
     * ⚠️ {@code STRUCT_TABLE}(nested table)은 <b>등록하지 않는다</b> — 일부러 그렇다.
     *
     * <p>Tibero 는 nested table 을 <b>컬럼 타입으로 받지 않는다</b>(실측).
     *
     * <pre>
     * create type NTA as table of number                  OK
     * create table T (id number, v NTA)                   FAIL  JDBC-7002 Unsupported DDL
     * create table T (v NTA) nested table v store as S    FAIL  JDBC-7002 Unsupported DDL
     * </pre>
     *
     * <p>Oracle 은 {@code STRUCT_ARRAY} 와 {@code STRUCT_TABLE} 을 함께 처리하지만, 우리가
     * 따라 하면 쓸 수 없는 DDL 이 나가 {@code create table} 이 깨진다. 그래서 등록을
     * {@code STRUCT_ARRAY} 로 한정했다.
     *
     * <p>이 테스트는 그 <b>의도적 제외</b>를 고정한다 — 나중에 누가 "Oracle 은 둘 다 하는데"
     * 하며 {@code STRUCT_TABLE} 을 추가하면, 먼저 Tibero 가 nested table 컬럼을 받게
     * 되었는지부터 확인해야 한다.
     */
    @Test
    public void structTable_isDeliberatelyNotRegistered() {
        final AggregateColumn nestedTable = aggregateColumn("stops", SqlTypes.STRUCT_TABLE);
        final List<?> objects = support.aggregateAuxiliaryDatabaseObjects(
                null, "stops", nestedTable, List.of());
        assertTrue("nested table 은 부가 객체를 만들지 않는다", objects.isEmpty());
    }

    /** {@code AggregateColumn} 은 원본 컬럼을 감싸는 구조라 두 단계로 만든다. */
    private static AggregateColumn aggregateColumn(String name, int sqlTypeCode) {
        Column base = new Column(name);
        base.setSqlTypeCode(sqlTypeCode);
        AggregateColumn col = new AggregateColumn(base, null);
        col.setSqlTypeCode(sqlTypeCode);
        return col;
    }

    /** JSON 읽기 식은 성분 컬럼의 타입을 보므로 평범한 varchar2 컬럼 하나를 만들어 준다. */
    private static Column varcharColumn(String name) {
        final Column col = new Column(name);
        col.setSqlTypeCode(SqlTypes.VARCHAR);
        col.setSqlType("varchar2(255 char)");
        return col;
    }
}
