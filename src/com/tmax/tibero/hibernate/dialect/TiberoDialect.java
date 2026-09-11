package com.tmax.tibero.hibernate.dialect;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.hibernate.cfg.BatchSettings.BATCH_VERSIONED_DATA;
import static org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtractor.extractUsingTemplate;
import static org.hibernate.type.SqlTypes.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tmax.tibero.hibernate.dialect.identity.TiberoIdentityColumnSupport;
import com.tmax.tibero.hibernate.dialect.pagination.TiberoLimitHandler;
import com.tmax.tibero.hibernate.dialect.sequence.TiberoSequenceSupport;
import com.tmax.tibero.hibernate.tool.schema.extract.internal.SequenceInformationExtractorTiberoDatabaseImpl;
import com.tmax.tibero.hibernate.procedure.TiberoCallableStatementSupport;
import com.tmax.tibero.hibernate.type.TiberoBinaryDoubleJdbcType;
import com.tmax.tibero.hibernate.type.TiberoBinaryFloatJdbcType;
import com.tmax.tibero.hibernate.type.TiberoJsonBlobJdbcType;
import com.tmax.tibero.hibernate.type.TiberoStructJdbcType;
import com.tmax.tibero.hibernate.type.TiberoArrayJdbcTypeConstructor;
import com.tmax.tibero.hibernate.tool.schema.internal.TiberoUserDefinedTypeExporter;
import org.hibernate.mapping.UserDefinedType;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.sql.internal.ArrayDdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport;
import org.hibernate.dialect.aggregate.AggregateSupport;
import jakarta.persistence.TemporalType;
import org.hibernate.LockOptions;
import org.hibernate.QueryTimeoutException;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.BooleanDecoder;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DmlTargetColumnQualifierSupport;
import org.hibernate.dialect.OracleBooleanJdbcType;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.RowLockStrategy;
import org.hibernate.dialect.SelectItemReferenceStrategy;
import org.hibernate.dialect.TimeZoneSupport;
import org.hibernate.dialect.unique.CreateTableUniqueDelegate;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelperBuilder;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayJavaType;
import java.sql.DatabaseMetaData;
import java.time.temporal.ChronoField;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsTimestampWithNanos;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.function.ModeStatsModeEmulation;
import org.hibernate.dialect.function.OracleTruncFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableKind;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.SemanticException;
import org.hibernate.query.sqm.FetchClauseType;
import org.hibernate.query.sqm.IntervalType;
import org.hibernate.query.sqm.TemporalUnit;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.type.descriptor.jdbc.OracleJsonBlobJdbcType;
import static org.hibernate.query.sqm.TemporalUnit.DAY;
import static org.hibernate.query.sqm.TemporalUnit.HOUR;
import static org.hibernate.query.sqm.TemporalUnit.MINUTE;
import static org.hibernate.query.sqm.TemporalUnit.MONTH;
import static org.hibernate.query.sqm.TemporalUnit.SECOND;
import static org.hibernate.query.sqm.TemporalUnit.YEAR;
import org.hibernate.query.sqm.CastType;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtractor;
import org.hibernate.exception.spi.ViolatedConstraintNameExtractor;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.procedure.internal.StandardCallableStatementSupport;
import org.hibernate.procedure.spi.CallableStatementSupport;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.produce.function.FunctionParameterType;
import org.hibernate.query.sqm.produce.function.StandardFunctionArgumentTypeResolvers;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.extract.spi.SequenceInformationExtractor;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.SqlTypedJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Tibero Hibernate Dialect (Hibernate 6.6).
 *
 * <p>Oracle Dialect 대비 의도적으로 오버라이드하지 않은 API
 * (미지원 / 미구현 / 기본값 적합) 판단은
 * {@code docs/dialect-decisions.md} 및
 * {@code DialectDecisionContractTest} / {@code DialectDecisionCapabilityTest} 를 본다.
 * diff만 보고 오버라이드를 추가하지 말 것.
 */
public class TiberoDialect extends Dialect {

    /** UDT DDL exporter. dialect 인스턴스에 묶여 있어 필드로 들고 있는다 */
    private final Exporter<UserDefinedType> userDefinedTypeExporter = new TiberoUserDefinedTypeExporter(this);
    private static final Pattern DISTINCT_KEYWORD_PATTERN = Pattern.compile("\\bdistinct\\b", CASE_INSENSITIVE);
    private static final Pattern GROUP_BY_KEYWORD_PATTERN = Pattern.compile("\\bgroup\\s+by\\b", CASE_INSENSITIVE);
    private static final Pattern ORDER_BY_KEYWORD_PATTERN = Pattern.compile("\\border\\s+by\\b", CASE_INSENSITIVE);
    private static final Pattern UNION_KEYWORD_PATTERN = Pattern.compile("\\bunion\\b", CASE_INSENSITIVE);

    private static final Pattern SQL_STATEMENT_TYPE_PATTERN =
        Pattern.compile("^(?:/\\*.*?\\*/)?\\s*(select|insert|update|delete)\\s+.*?", CASE_INSENSITIVE);

    // month/quarter/year add. %1$s = 개월 수, %2$s = 대상 값
    //
    // add_months 가 월말 클램프를 네이티브로 처리한다 — 1/31 + 1개월 = 2/29.
    // Hibernate OracleDialect.yqmSelect 는 trunc(x,'MONTH') 로 시작해 일자만 되더하는데,
    // 그 trunc 때문에 TIMESTAMP 의 시각이 사라진다(2024-03-15 10:20:30 → 04-15 00:00:00).
    // 다른 dialect(PostgreSQL·MySQL·SQL Server·H2 등)는 모두 DB 네이티브 함수를 써서
    // 시각을 보존하므로, 여기서는 Oracle 식을 따라가지 않고 시각을 보존한다.
    private static final String ADD_MONTHS_DATE = "add_months(%2$s,%1$s)";
    // add_months 는 DATE 를 돌려주므로 소수점 이하 초가 잘린다.
    // cast 로 TIMESTAMP 를 만든 뒤 잔여 소수부를 다시 더해 복원한다.
    // (DATE 대상에 쓰면 DATE-DATE 가 interval 이 아니라 숫자가 되어 JDBC-5011 로 깨지므로
    //  아래 monthsPattern 이 타입별로 갈라 준다)
    private static final String ADD_MONTHS_TIMESTAMP =
        "(cast(add_months(%2$s,%1$s) as timestamp) + (%2$s - cast(%2$s as date)))";

    private final SequenceSupport tiberoSequenceSupport = TiberoSequenceSupport.getInstance(this);
    private final UniqueDelegate uniqueDelegate = new CreateTableUniqueDelegate(this);

    public TiberoDialect() {
        super(DatabaseVersion.make(7));
        registerDefaultProperties();
    }

    // mandatory constructor
    public TiberoDialect(DialectResolutionInfo info) {
       super(info);
    }

    @Override
    protected void initDefaultProperties() {
        super.initDefaultProperties();
        registerDefaultProperties();
    }

    /**
     * Tibero 고유 예약어를 Hibernate 의 인용 대상 목록에 추가한다.
     *
     * <h2>먼저 알아야 할 것 — 이건 옵트인 기능이다</h2>
     * Hibernate 는 <b>기본적으로 예약어를 인용하지 않는다.</b>
     * {@code hibernate.auto_quote_keyword} 의 기본값이 {@code false} 다.
     *
     * <pre>
     * 설정 끔(기본)   create table ITEM (size number(10,0), ...)     JDBC-7001
     * 설정 켬         create table ITEM ("size" number(10,0), ...)   OK
     * </pre>
     *
     * 그래서 이 메서드가 고치는 것은 <b>"예약어 컬럼이 깨진다"가 아니라
     * "설정을 켜도 Tibero 예약어는 안 잡힌다"</b> 이다. 설정을 끈 상태의 동작은
     * Oracle 과 같고 Hibernate 가 문서화해 둔 기본값이므로 우리가 바꿀 일이 아니다
     * (dialect 가 사용자 설정을 임의로 켜서도 안 된다).
     *
     * <h2>설정을 켰을 때 무엇이 달라지나</h2>
     * 설정이 켜지면 Hibernate 는 인용 대상 목록을 두 군데서 모은다 —
     * <b>드라이버가 보고하는 {@code DatabaseMetaData.getSQLKeywords()}</b> 와
     * <b>dialect 가 {@link #registerKeyword} 로 등록한 것</b>. tbjdbc 는 34개만
     * 보고하고 거기에 {@code size} · {@code least} 같은 흔한 이름이 빠져 있다.
     *
     * <p>같은 엔티티를 {@code auto_quote_keyword=true} 로 돌린 실측 비교다.
     *
     * <pre>
     * OracleDialect (추가 등록 없음)
     *   create table QP2 (least ..., "number" ..., size ..., "comment" ...)
     *                     ^^^^^              ^^^^          ← 안 잡혀서 DDL 실패
     *
     * TiberoDialect (아래 71종 등록)
     *   create table QP2 ("least" ..., "number" ..., "size" ..., "comment" ...)
     * </pre>
     *
     * {@code number} · {@code comment} 는 tbjdbc 목록에 있어 원래도 잡혔고,
     * {@code size} · {@code least} 가 이 등록으로 새로 잡히는 부분이다.
     *
     * <h2>왜 Oracle 대조로는 안 보였나</h2>
     * {@code OracleDialect} 에도 {@code registerKeywords()} 호출이 없다. 그래서
     * "Oracle 이 하는데 우리가 안 한 것" 목록에는 절대 잡히지 않는다. 반면
     * MySQL · SQLServer · DB2 · HSQL · HANA · Sybase · Derby <b>7개 벤더는 자기
     * 예약어를 등록한다</b> — 벤더 대조를 하고 나서야 여기가 원래 벤더가 채우는
     * 자리라는 게 드러났다.
     *
     * <h2>목록을 어떻게 정했나</h2>
     * 추측하지 않고 <b>{@code V$RESERVED_WORDS} 전량(1,242개)을 실제로 컬럼명으로
     * 써 봤다.</b> Hibernate 가 이미 아는 단어를 빼고 식별자 형태인 1,064개에 대해
     * {@code create table X (<단어> number)} 를 돌린 결과가 아래 71개다.
     *
     * <pre>
     * RESERVED='Y' 인데 실패        58개
     * RESERVED='N' 인데 실패        13개   ← 카탈로그만 믿었으면 놓쳤을 것들
     * 인용해도 안 되는 것             0개   ← 전부 큰따옴표로 해결됨
     * </pre>
     *
     * {@code RESERVED='N'} 인데 실패한 13개가 이 방식의 값어치다. Tibero 카탈로그의
     * {@code RESERVED} 플래그만 믿었다면 {@code least} · {@code flashback} ·
     * {@code connect_by_root} 같은 단어가 빠졌을 것이다.
     *
     * <h2>한계</h2>
     * <ul>
     *   <li><b>{@code hibernate.auto_quote_keyword=true} 가 없으면 아무 효과가 없다.</b>
     *       사용자 문서에 이 설정을 안내해야 한다.</li>
     *   <li>이 목록은 <b>Tibero 7 ps06 기준</b>이다. 서버 버전이 올라가 예약어가
     *       늘면 다시 훑어야 한다. {@code capability.ReservedWordDdlTest} 가
     *       {@code V$RESERVED_WORDS} 를 다시 전수로 돌려 빠진 단어를 알려 준다.</li>
     *   <li>설정을 안 켜는 쪽을 택했다면 사용자가
     *       {@code @Column(name = "\"size\"")} 처럼 직접 인용하면 된다.</li>
     * </ul>
     *
     * @see #KEYWORDS 실측으로 확정한 71개
     */
    /**
     * 다중 키 배치 로딩을 <b>500키씩</b> 나눈다.
     *
     * <h2>왜 이 값이 필요한가 — IN 한도가 아니라 파스 비용</h2>
     * Tibero 는 IN 원소 개수에 <b>한도가 없다</b>. 단일 IN 리스트에 바인드를 50,000개
     * 넣어도 오류 없이 실행된다(ps06 실측). 그래서
     * {@link #getInExpressionCountLimit()} 은 기본값 {@code 0}(무제한)을 그대로 둔다 —
     * Oracle 의 1000 은 {@code ORA-01795} 를 피하려는 값이라 우리에겐 해당이 없다.
     *
     * <p>문제는 다른 데 있다. <b>서버 파스 시간이 IN 원소 수의 제곱으로 늘어난다.</b>
     *
     * <pre>
     * 바인드 수     최초 실행(파스 포함)     같은 문장 재실행
     *   1,000            0.16 초                0.8 ms
     *  10,000           15.6  초                5.4 ms
     *  30,000          138.6  초               15.3 ms
     *  50,000          387.6  초               20.2 ms        t(ms) ≈ 1.55e-4 × N²
     * </pre>
     *
     * 파스는 SQL 텍스트당 한 번만 든다(새 커넥션에서도 5.5ms — 서버측 공용 캐시).
     * 그런데 Hibernate 는 리스트 길이만큼 {@code ?} 를 찍으므로 <b>키가 하나만 달라도
     * 새 텍스트</b>가 되어 그 비용을 다시 문다.
     *
     * <h2>왜 {@code getInExpressionCountLimit} 으로는 못 고치나</h2>
     * 이름만 보면 그쪽이 맞을 것 같지만 아니다.
     * {@code AbstractSqlAstTranslator#visitInListPredicate} 는 그 값으로 <b>문장을 나누지
     * 않고</b>, 한 문장 안에서 {@code ) or x in (} 로 이어 붙인다.
     *
     * <pre>
     * limit = 0      where id in (?,?,… 10000개 …)
     * limit = 1000   where (id in (?×1000) or id in (?×1000) or … 10벌 …)
     *                 ↑ 여전히 한 문장, 바인드 총수 동일
     * </pre>
     *
     * 그래서 개선폭이 10,000개 기준 15.4초 → 4.6초(3.3배)에 그친다. 게다가 native query
     * 경로는 분할조차 하지 않고 {@code HHH000443}("will likely cause failures") 경고만
     * 찍는데, Tibero 는 실패하지 않으므로 오탐 로그만 쌓인다.
     *
     * <h2>이 값은 진짜로 문장을 나눈다</h2>
     * {@code Dialect.STANDARD_MULTI_KEY_LOAD_SIZING_STRATEGY} 가 이 값으로 배치 크기를
     * 정하고, {@code byMultipleIds} · {@code @BatchSize} 배치 페치 · 컬렉션 배치 페치 ·
     * {@code byMultipleNaturalId} 가 <b>실제로 여러 문장</b>으로 쪼갠다.
     *
     * <pre>
     * byMultipleIds 실측 (ps06, StatementInspector 로 문장 수 확인)
     *
     *   3,000키  limit=0     select 1개 (? 3,000개)   1,411 ms
     *   3,000키  limit=1000  select 3개 (? 1,000개)     171 ms
     * </pre>
     *
     * <h2>왜 하필 500인가</h2>
     * 파스가 제곱이므로 청크를 줄이면 그만큼 싸지지만, 대신 왕복 횟수가 는다. 그 균형점을
     * 실측했다(10,000키 / 30,000키).
     *
     * <pre>
     * chunk    10,000키    30,000키
     *   100      76 ms      101 ms     ← 왕복 횟수가 지배
     *   250      35 ms       65 ms     ← 최소
     *   500      57 ms       78 ms
     *   750     115 ms      119 ms
     *  1000     167 ms      178 ms
     *     0  15,490 ms   (측정 안 함)
     * </pre>
     *
     * 최솟값은 250 이지만 <b>500</b> 을 골랐다. 위 측정은 <b>localhost</b> 라 왕복 비용이
     * 사실상 0 이어서 작은 청크에 유리하게 치우쳐 있다. 실제 배포처럼 네트워크 지연이
     * 있으면 왕복이 늘수록 손해가 커지므로, 250~500 구간에서 <b>왕복이 절반인 쪽</b>이
     * 안전하다. 두 값의 차이는 어차피 수십 밀리초다.
     *
     * <h2>한계</h2>
     * <ul>
     *   <li>배치 로딩 경로에만 적용된다. <b>HQL 의 {@code in :list} 는 여전히 한 문장</b>
     *       이므로 큰 리스트를 넘기면 파스 비용을 그대로 문다. 그쪽은 애플리케이션이
     *       끊어 보내거나 {@code hibernate.query.in_clause_parameter_padding=true} 로
     *       텍스트 종류를 줄여야 한다.</li>
     *   <li>500 은 localhost 측정에 기반한 값이다. 네트워크 지연이 큰 환경에서 배치 로딩이
     *       느리다면 이 값을 올려 보는 것이 첫 번째 시도다.</li>
     *   <li>Hibernate 의 {@code SybaseDialect} 도 같은 비대칭을 쓴다 —
     *       IN 250,000 / 파라미터 2,000. 두 값을 다르게 두는 것이 이상한 조합이 아니다.</li>
     * </ul>
     *
     * @see #getInExpressionCountLimit() 이쪽은 기본값 0 을 유지한다
     */
    @Override
    public int getParameterCountLimit() {
        return 500;
    }

    /**
     * {@code drop type if exists} 를 쓴다.
     *
     * <h2>왜 필요한가</h2>
     * {@code hbm2ddl.auto=create-drop} 은 <b>만들기 전에 먼저 지운다.</b> 첫 실행에는 지울
     * 타입이 없으므로 {@code drop type X} 가 {@code JDBC-7071} 로 실패한다. 평소에는
     * Hibernate 가 삼켜 넘어가지만 <b>{@code hbm2ddl.halt_on_error=true} 를 켜면 첫 실행이
     * 통째로 죽는다.</b>
     *
     * <p>그 설정은 DDL 문제를 조사할 때 반드시 켜야 하는 것이다 — {@code @Struct} 성분
     * check 제약 결함은 그것 없이는 찾지 못했다. 첫 실행에서 죽으면 그 도구를 못 쓴다.
     *
     * <pre>
     * drop type NOPE_T force              FAIL  JDBC-7071
     * drop type if exists NOPE_T force    OK     (실측)
     * </pre>
     *
     * <p>Oracle 은 이 문법을 못 받아 {@code false} 를 쓴다. {@code drop table if exists} 와
     * 마찬가지로 <b>Tibero 가 Oracle 보다 나은 지점</b>이다.
     *
     * <p>이 값은 원래 {@code false} 였다 — {@code supportsIfExistsBeforeTableName} 은
     * 실측해서 {@code true} 로 두었는데 타입 쪽은 확인하지 않은 채 Hibernate 기본값을
     * 그대로 둔 것이었다. {@code @Struct} 배열 작업에서 UDT 를 다루다 드러났다.
     */
    @Override
    public boolean supportsIfExistsBeforeTypeName() {
        return true;
    }

    @Override
    protected void registerDefaultKeywords() {
        super.registerDefaultKeywords();
        for (String keyword : KEYWORDS) {
            registerKeyword(keyword);
        }
    }

    /**
     * Hibernate 기본 목록에 없으면서 Tibero 에서 컬럼명으로 쓸 수 없는 단어 71개.
     *
     * <p>{@code V$RESERVED_WORDS} 전량을 ps06 에 실제로 던져 확정했다. 손으로 고른
     * 목록이 아니므로 임의로 지우지 말 것 — 지우면
     * {@code capability.ReservedWordDdlTest} 가 실패한다.
     *
     * <p>알파벳 순. {@code binary_double_infinity} 처럼 값 리터럴로 쓰이는 것,
     * {@code connect_by_root} 처럼 계층 질의 연산자인 것, {@code hbase} ·
     * {@code ucache} 처럼 Tibero 고유 기능 이름인 것이 섞여 있다.
     */
    private static final String[] KEYWORDS = {
            "access", "asc", "audit",
            "binary_double_infinity", "binary_double_nan",
            "binary_float_infinity", "binary_float_nan",
            "bitmap", "btip", "cluster", "comment", "compress",
            "connect_by_iscycle", "connect_by_isleaf", "connect_by_root",
            "dblink_owner", "ddl", "desc", "exclusive", "externally",
            "file", "flashback", "hbase", "identified", "index",
            "index_partition", "index_subpartition", "infinite", "initial",
            "least", "level", "lock", "long", "maxextents", "minimize",
            "minus", "mode", "modify", "nan", "noaudit", "nocompress",
            "nominimize", "nowait", "number", "offline", "online", "option",
            "permanent", "prior", "privileges", "public", "raw", "rebalance",
            "records_per_block", "rename", "replication", "rowid", "rownum",
            "session", "share", "size", "successful", "synonym", "sysdate",
            "triggers", "ucache", "uid", "unprepare", "validate",
            "varchar2", "view"
    };

    @Override
    public int getDefaultStatementBatchSize() {
        return 15;
    }

    /**
     * Tibero JDBC는 컬럼명 명시해서 getGeneratedKeys() 호출하면 정상적으로 PK를 반환하지만
     * 컬럼명 없이 generated keys를 요청하면 PK가 아니라 ROWID를 반환
     *
     * 하지만 TiberoIdentityColumnSupport 클래스에서 inferredKeys=false로 설정했기 때문에
     * 해당 설정값을 true로 설정
     */
    @Override
    public boolean getDefaultUseGetGeneratedKeys() {
        return true;
    }

    protected void registerDefaultProperties() {
        // @Version(버전 컬럼)이 있는 엔티티도 JDBC 배치로 UPDATE/DELETE 할지 여부
        getDefaultProperties().setProperty(BATCH_VERSIONED_DATA, "false");
    }


    // 함수 등록 — Hibernate 6.6 CommonFunctionFactory 정렬 + Tibero 전용 유지
    @Override
    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);
        final TypeConfiguration typeConfiguration = functionContributions.getTypeConfiguration();
        final SqmFunctionRegistry registry = functionContributions.getFunctionRegistry();
        final BasicTypeRegistry basicTypeRegistry = typeConfiguration.getBasicTypeRegistry();

        CommonFunctionFactory functionFactory = new CommonFunctionFactory(functionContributions);
        functionFactory.ascii();
        functionFactory.char_chr();
        functionFactory.cosh();
        functionFactory.sinh();
        functionFactory.tanh();
        functionFactory.log();
        functionFactory.log10_log();
        functionFactory.soundex();
        functionFactory.trim2();
        functionFactory.initcap();
        functionFactory.instr();
        functionFactory.substr();
        functionFactory.substring_substr();
        functionFactory.leftRight_substr();
        functionFactory.translate();
        functionFactory.bitand();
        functionFactory.lastDay();
        functionFactory.toCharNumberDateTimestamp();
        functionFactory.ceiling_ceil();
        functionFactory.concat_pipeOperator();
        functionFactory.rownumRowid();
        functionFactory.sysdate();
        functionFactory.systimestamp();
        functionFactory.addMonths();
        functionFactory.monthsBetween();
        functionFactory.everyAny_minMaxCase();
        functionFactory.repeat_rpad();

        functionFactory.radians_acos();
        functionFactory.degrees_acos();

        functionFactory.median();
        functionFactory.stddev();
        functionFactory.stddevPopSamp();
        functionFactory.variance();
        functionFactory.varPopSamp();
        functionFactory.covarPopSamp();
        functionFactory.corr();
        functionFactory.regrLinearRegressionAggregates();
        functionFactory.characterLength_length("dbms_lob.getlength(?1)");
        functionFactory.octetLength_pattern("lengthb(?1)", "dbms_lob.getlength(?1)*2");
        functionFactory.bitLength_pattern("lengthb(?1)*8", "dbms_lob.getlength(?1)*16");

        functionFactory.coalesce();

        registry.patternDescriptorBuilder("bitor", "(?1+?2-bitand(?1,?2))")
                .setExactArgumentCount(2)
                .setArgumentTypeResolver(StandardFunctionArgumentTypeResolvers.ARGUMENT_OR_IMPLIED_RESULT_TYPE)
                .register();
        registry.patternDescriptorBuilder("bitxor", "(?1+?2-2*bitand(?1,?2))")
                .setExactArgumentCount(2)
                .setArgumentTypeResolver(StandardFunctionArgumentTypeResolvers.ARGUMENT_OR_IMPLIED_RESULT_TYPE)
                .register();

        registry.registerBinaryTernaryPattern(
                "locate",
                basicTypeRegistry.resolve(StandardBasicTypes.INTEGER),
                "instr(?2,?1)",
                "instr(?2,?1,?3)",
                FunctionParameterType.STRING, FunctionParameterType.STRING, FunctionParameterType.INTEGER,
                typeConfiguration
        ).setArgumentListSignature("(pattern, string[, start])");

        functionFactory.listagg(null);
        functionFactory.windowFunctions();
        functionFactory.hypotheticalOrderedSetAggregates();
        functionFactory.inverseDistributionOrderedSetAggregates();
        registry.register("mode", new ModeStatsModeEmulation(typeConfiguration));
        registry.register("trunc", new OracleTruncFunction(typeConfiguration));
        registry.registerAlternateKey("truncate", "trunc");

        // Tibero: ROWID는 문자열로 취급 (Factory 기본 long 덮어씀)
        registry.noArgsBuilder("rowid")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        // Tibero 전용 함수
        registry.register("instrb", new StandardSQLFunction("instrb", StandardBasicTypes.INTEGER));
        registry.register("substrb", new StandardSQLFunction("substrb", StandardBasicTypes.STRING));
        registry.register("nvl", new StandardSQLFunction("nvl"));
        registry.register("nvl2", new StandardSQLFunction("nvl2"));
        registry.register("next_day", new StandardSQLFunction("next_day", StandardBasicTypes.DATE));
        registry.noArgsBuilder("uid")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.INTEGER))
                .setUseParenthesesWhenNoArgs(false)
                .register();
        registry.noArgsBuilder("user")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
                .setUseParenthesesWhenNoArgs(false)
                .register();
        registry.register("str", new StandardSQLFunction("to_char", StandardBasicTypes.STRING));

        // mod/power/atan2 는 super.initializeFunctionRegistry 의 CommonFunctionFactory.math()·trigonometry()
        // 등록(인자 개수·타입 검증 포함, power/atan2 = double)을 그대로 사용함.
        // 재등록하면 double → float 로 좁아짐

        // ------------------------------------------------------------------
        // HQL array_* 함수는 등록하지 않는다 — 의도된 축소
        //
        // Hibernate 의 array_* 함수(Oracle 변종)는 배열 타입마다 만들어지는 PL/SQL 헬퍼
        // (StringArray_length, StringArray_concat …)를 호출한다. 그 헬퍼를 Tibero 에 적용해
        // 보았더니 DB 가 반복적으로 응답 불능에 빠졌다 — 특히 _concat 은 두 번째 호출부터
        // 서버 워커가 멈추고 인스턴스 재기동으로만 풀렸다.
        //
        // 등록해두면 HQL 은 통과하고 실행 시점에 DB 가 멈추므로, 아예 등록하지 않아
        // 파싱 단계에서 걸리게 한다. 배열 컬럼 매핑·왕복 자체는 이와 무관하게 동작하며,
        // 네이티브 SQL 의 table() 언네스트로 같은 일을 할 수 있다.
        //
        // 상세는 TiberoUserDefinedTypeExporter 의 javadoc.
        // ------------------------------------------------------------------
    }

    @Override
    public int getMaxVarcharLength() {
        return 65532;
    }

    @Override
    public int getMaxVarbinaryLength() {
        // Tibero RAW 최대 길이 실측 (localhost:8888): 2000
        return 2000;
    }

    /**
     * NVARCHAR2는 VARCHAR2와 상한이 다름 (실측: 32766 OK / 32767 FAIL).
     * 기본 구현은 getMaxVarcharLength()를 그대로 쓰므로 반드시 분리해야 함
     */
    @Override
    public int getMaxNVarcharLength() {
        return 32766;
    }

    /**
     * Tibero는 빈 문자열 '' 을 NULL로 저장함
     */
    @Override
    public boolean isEmptyStringTreatedAsNull() {
        return true;
    }

    @Override
    public String getDual() {
        return "dual";
    }

    @Override
    public String getFromDualForSelectOnly() {
        return " from " + getDual();
    }


    /**
     * 타입 관련
     */

    @Override
    protected String columnType(int sqlTypeCode) {
        switch ( sqlTypeCode ) {
            case BOOLEAN:
                return "number(1,0)";
            case TINYINT:
                return "number(3,0)";
            case SMALLINT:
                return "number(5,0)";
            case INTEGER:
                return "number(10,0)";
            case BIGINT:
                return "number(19,0)";

            case REAL:
                return "binary_float";
            case DOUBLE:
                return "binary_double";

            case NUMERIC:
            case DECIMAL:
                return "number($p,$s)";

            case TIME_WITH_TIMEZONE:
                return "timestamp($p) with time zone";

            case CHAR:
                return "char($l char)";
            case VARCHAR:
                return "varchar2($l char)";
            case NVARCHAR:
                return "nvarchar2($l)";

            case BINARY:
            case VARBINARY:
                return "raw($l)";

            default:
                return super.columnType( sqlTypeCode );
        }
    }

    // 타입 등록 (DDL 생성용)
    @Override
    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);

        final DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();

        /*
         * Tibero의 float(p)는 이진 정밀도가 아니라 NUMBER로 저장된다.
         * float(53) -> NUMBER(15) 가 되어 IEEE double이 15자리로 잘리고(3.141592653589793 -> 3.14159265358979),
         * Double.MAX_VALUE는 아예 overflow로 거부된다.
         * 또 cast(x as float(53))은 NUMBER 정밀도 상한(38)을 넘어 JDBC-5077로 실패한다.
         *
         * Hibernate는 Float/Double을 모두 FLOAT DDL 코드로 보내고 precision으로만 구분하므로
         * (DoubleJdbcType.getDdlTypeCode() == FLOAT) precision을 보고 Tibero의 IEEE 타입에 매핑한다.
         */
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( FLOAT, "binary_double", null, "binary_double", this ) {
            @Override
            public String getTypeName(Long size, Integer precision, Integer scale) {
                return precision != null && precision <= getFloatPrecision()
                        ? "binary_float"
                        : "binary_double";
            }
        } );

        // xmltype
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( SQLXML, "xmltype", this ) );

        // geometry
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( GEOMETRY, "geometry", this ) );

        // json
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( JSON, "json", this ) );

        // interval day to second
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( INTERVAL_SECOND, "interval day to second", this ) );

        // TODO interval year to month, interval (not in SqlTypes)
    }

    @Override
    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        // number(1,0) boolean + check (0,1)
        typeContributions.contributeJdbcType(OracleBooleanJdbcType.INSTANCE);
        // JSON을 BLOB 바인딩으로 다루는 경로 (resolveSqlTypeDescriptor의 json/BLOB과 정합).
        // Oracle 구현을 그대로 쓰면 Tibero가 지원하지 않는 check (col is json) 이 딸려와
        // JSON 컬럼 엔티티의 create table 이 실패하므로 전용 서브클래스를 쓴다.
        typeContributions.contributeJdbcType(TiberoJsonBlobJdbcType.INSTANCE);

        // 표준 setDouble/setFloat은 binary_double/binary_float 컬럼에도 NUMBER로 바인딩되어
        // IEEE 극값이 거부되고 큰 지수에서 왕복 오차가 생긴다. 드라이버 확장 API로 바인딩한다.
        typeContributions.contributeJdbcType(TiberoBinaryDoubleJdbcType.INSTANCE);
        typeContributions.contributeJdbcType(TiberoBinaryFloatJdbcType.FLOAT_INSTANCE);
        typeContributions.contributeJdbcType(TiberoBinaryFloatJdbcType.REAL_INSTANCE);

        // @Struct 임베더블을 object UDT 컬럼에 담는 경로. Hibernate 의 드라이버 중립 구현으로도
        // 대부분 동작하지만 null 바인딩과 중첩 struct 두 곳에서 tbjdbc 가 표준과 다르게 군다.
        typeContributions.contributeJdbcType(TiberoStructJdbcType.INSTANCE);

        // 배열 필드를 VARRAY 컬럼에 담는 경로.
        // 기본 Dialect 는 supportsStandardArrays() 가 true 일 때만 아래 둘을 등록한다.
        // Tibero 는 ANSI array DDL(`c number array`)을 받지 않으므로 그 플래그는 false 이고,
        // 대신 Oracle 식 VARRAY UDT 로 지원한다 — Oracle 도 같은 이유로 직접 등록한다.
        //
        // Oracle 은 여기서 SqlTypes.TABLE 용 DdlType 도 함께 등록하지만 우리는 하지 않는다 —
        // 그 코드가 도달하려면 nested table(`as table of`) JdbcType 을 만드는 생성자가 필요한데
        // (Oracle 의 OracleNestedTableJdbcTypeConstructor) 우리는 VARRAY 만 지원한다.
        // nested table 을 지원하게 되면 그 생성자와 함께 이 등록도 되살릴 것.
        final DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();
        ddlTypeRegistry.addDescriptor(new ArrayDdlTypeImpl(this, false));
        typeContributions.contributeJdbcTypeConstructor(TiberoArrayJdbcTypeConstructor.INSTANCE);
    }

    /**
     * 배열 요소 타입에서 VARRAY 타입 이름을 만든다 — {@code String[]} → {@code StringArray}.
     *
     * <p>Hibernate 기본값은 {@code null} 이고, 그러면 배열 컬럼 DDL 자체가 만들어지지 않아
     * 배열은 {@code VARBINARY} 이진 덩어리로 떨어진다. Oracle 과 같은 이름 규칙을 쓴다.
     *
     * <p>스키마에 <b>전역 이름</b>으로 만들어지므로 같은 요소 타입을 쓰는 여러 엔티티가
     * 하나의 VARRAY 타입을 공유한다.
     */
    @Override
    public String getArrayTypeName(String javaElementTypeName, String elementTypeName, Integer maxLength) {
        return (javaElementTypeName == null ? elementTypeName : javaElementTypeName) + "Array";
    }

    /**
     * 배열을 어떤 JDBC 타입으로 다룰지.
     *
     * <p>Hibernate 기본값은 {@code VARBINARY} — 배열을 직렬화해 하나의 이진 컬럼에 담는다.
     * 왕복은 되지만 DB 에서 배열로 다룰 수 없어 {@code table()} 언네스트도, {@code array_*}
     * 함수도 쓸 수 없다. {@code ARRAY} 로 바꿔 네이티브 VARRAY 컬럼을 쓴다.
     *
     * <p>⚠️ 이 값을 바꾸면 <b>기존 매핑의 컬럼 타입이 달라진다</b>. 6.6.0 에서 {@code int[]} 를
     * 쓰던 스키마는 {@code raw}/{@code blob} 컬럼인데 6.6.1 은 {@code IntegerArray} 를
     * 기대한다 — 릴리즈 노트에 안내가 필요하다.
     */
    @Override
    public int getPreferredSqlTypeCodeForArray() {
        return SqlTypes.ARRAY;
    }

    /**
     * UDT DDL 을 내는 주체 — object 타입과 array 타입 둘 다.
     *
     * <p>Hibernate 기본 {@code StandardUserDefinedTypeExporter} 는 array UDT 에서 예외를
     * 던지므로 배열을 지원하려면 반드시 교체해야 한다.
     *
     * @see com.tmax.tibero.hibernate.tool.schema.internal.TiberoUserDefinedTypeExporter
     */
    @Override
    public Exporter<UserDefinedType> getUserDefinedTypeExporter() {
        return userDefinedTypeExporter;
    }

    /**
     * 집계(aggregate) 컬럼 지원 — {@code @Struct} 임베더블을 컬럼 하나에 담는 매핑.
     *
     * <p>Hibernate 기본값 {@code AggregateSupportImpl} 은 관련 훅에서 전부 예외를 던지므로,
     * {@code @Struct} 엔티티가 하나라도 있으면 <b>SessionFactory 기동이 실패</b>한다.
     * STRUCT 계열만 다루는 구현을 돌려준다 — JSON 집계는 6.6.1 범위 밖이다.
     */
    @Override
    public AggregateSupport getAggregateSupport() {
        return TiberoAggregateSupport.INSTANCE;
    }

    /**
     * {@code create type ... as <여기>(...)} 의 종류 키워드.
     *
     * <p>Hibernate 기본값은 빈 문자열이라 {@code create type T as (...)} 가 나가는데,
     * 그 문법은 Tibero 가 받지 않는다. Oracle 과 같이 {@code object} 를 쓴다.
     *
     * <pre>create type ADDR_T as object(street varchar2(255 char), city varchar2(255 char))</pre>
     *
     * <p>DDL 자체는 Hibernate 기본 {@code StandardUserDefinedTypeExporter} 가 만든다 —
     * Oracle 이 별도 exporter 를 두는 것은 array UDT({@code varray} / {@code table of})의
     * 비교 함수 때문이고, object UDT 에는 필요 없다.
     */
    @Override
    public String getCreateUserDefinedTypeKindString() {
        return "object";
    }

    @Override
    public int getPreferredSqlTypeCodeForBoolean() {
        return Types.BIT;
    }

    @Override
    public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
        return new StandardSqlAstTranslatorFactory() {
            @Override
            protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(
                    SessionFactoryImplementor sessionFactory, Statement statement) {
                return new TiberoSqlAstTranslator<>(sessionFactory, statement);
            }
        };
    }


    @Override
    public String rowId(String rowId) {
        return "rowid";
    }

    @Override
    public boolean supportsBitType() {
        return false;
    }

    // INSERT DAY TO SECOND fractional_seconds_precision default 6
    @Override
    public int getDefaultIntervalSecondScale(){
        // microseconds
        return 6;
    }

    /**
     * SELECT 해온 ResultSet의 metadata(columnTypename, jdbcTypeCode, precision, scale)를 통해
     * 어떤 jdbcType으로 매핑할지 결정
     */
    @Override
    public JdbcType resolveSqlTypeDescriptor(String columnTypeName,
                                             int jdbcTypeCode,
                                             int precision,
                                             int scale,
                                             JdbcTypeRegistry jdbcTypeRegistry) {
        /**
         * sqlxml : Types.SQLXML, "xmltype"
         * json : Types.BLOB, "json"
         * geometry : Types.GEOMETRY, "geometry"
         * interval day to second : Types.OTHER, "interval day to second"
         * array : Types.ARRAY, "SCHEMA.TYPE_NAME"
         * struct : Types.STRUCT, "SCHEMA.TYPE_NAME"
         * binary_float : TbTypes.BINARY_FLOAT, "binary_float"
         * binary_double : TbTypes.BINARY_DOUBLE, "binary_double"
         *
         * numeric
         * number -> precision 38 scale 0
         * integer -> precision 38 scale 0
         * smallint -> precision 38 scale 0
         * float -> precision 38 scale 0
         * 이런 식으로 저장되기 때문에 columnType()에서 지정한 값(1, 3, 5, 10, 19)일 때만 역매핑하고 이외에는 numeric
         *
         * but, float(10) -> precision 3 scale 0
         *
         * TODO (columnType()/registerColumnTypes(), contributeTypes() 추가 후)
         * interval year to month : Types.OTHER, "interval year to month"
         */
        switch ( jdbcTypeCode ) {
            case BLOB :
                if (columnTypeName != null) {
                    final String typeName = columnTypeName.toLowerCase(Locale.ROOT);

                    if ("json".equals(typeName)) {
                        return jdbcTypeRegistry.getDescriptor(JSON);
                    }
                }
                break;
            case OTHER :
                if (columnTypeName != null) {
                    final String typeName = columnTypeName.toLowerCase(Locale.ROOT);

                    if (typeName.startsWith("interval day") && typeName.contains("to second")) {
                        return jdbcTypeRegistry.getDescriptor(INTERVAL_SECOND);
                    }
                }
                break;
            case TiberoTypes.GEOMETRY:
                return jdbcTypeRegistry.getDescriptor(SqlTypes.GEOMETRY);
            case TiberoTypes.BINARY_FLOAT:
                return jdbcTypeRegistry.getDescriptor(SqlTypes.REAL);
            case TiberoTypes.BINARY_DOUBLE:
                return jdbcTypeRegistry.getDescriptor(SqlTypes.DOUBLE);
            case ARRAY:
            case STRUCT:
                final SqlTypedJdbcType descriptor = jdbcTypeRegistry.findSqlTypedDescriptor(
                    // Skip the schema
                    columnTypeName.substring( columnTypeName.indexOf( '.' ) + 1 )
                );
                if ( descriptor != null ) {
                    return descriptor;
                }
                break;
            case NUMERIC:
                if (scale == 0) {
                    switch (precision) {
                        case 1:  return jdbcTypeRegistry.getDescriptor(SqlTypes.BOOLEAN);
                        case 3:  return jdbcTypeRegistry.getDescriptor(SqlTypes.TINYINT);
                        case 5:  return jdbcTypeRegistry.getDescriptor(SqlTypes.SMALLINT);
                        case 10: return jdbcTypeRegistry.getDescriptor(SqlTypes.INTEGER);
                        case 19: return jdbcTypeRegistry.getDescriptor(SqlTypes.BIGINT);
                        default: return jdbcTypeRegistry.getDescriptor(SqlTypes.NUMERIC); // 안전 fallback
                    }
                }
                /**
                 * TODO
                 * scale == null 에 해당하는 값일 때 (ex : float(30), number) 처리
                 * (단, scale == null 일 때 0으로 리턴되는 jdbc 문제 개선 후)
                 */
        }
        return super.resolveSqlTypeDescriptor(columnTypeName, jdbcTypeCode, precision, scale, jdbcTypeRegistry);
    }


    @Override
    public String currentTimestampWithTimeZone() {
        return "current_timestamp";
    }

    @Override
    public String currentDate() {
        return "current_date";
    }

    @Override
    public String currentTime() {
        return currentTimestamp();
    }

    @Override
    public String currentTimestamp() {
        return currentTimestampWithTimeZone();
    }

    @Override
    public String currentLocalTime() {
        return currentLocalTimestamp();
    }

    @Override
    public String currentLocalTimestamp() {
        return "localtimestamp";
    }

    @Override
    public long getFractionalSecondPrecisionInNanos() {
        return 1_000_000_000L; // seconds
    }

    @Override
    public String castPattern(CastType from, CastType to) {
        String result;
        switch (to) {
            case INTEGER:
            case LONG:
                result = BooleanDecoder.toInteger(from);
                if (result != null) {
                    return result;
                }
                break;
            case INTEGER_BOOLEAN:
                result = from == CastType.STRING
                        ? buildStringToBooleanCastDecode("1", "0")
                        : BooleanDecoder.toIntegerBoolean(from);
                if (result != null) {
                    return result;
                }
                break;
            case YN_BOOLEAN:
                result = from == CastType.STRING
                        ? buildStringToBooleanCastDecode("'Y'", "'N'")
                        : BooleanDecoder.toYesNoBoolean(from);
                if (result != null) {
                    return result;
                }
                break;
            case BOOLEAN:
                result = from == CastType.STRING
                        ? buildStringToBooleanCastDecode("true", "false")
                        : BooleanDecoder.toBoolean(from);
                if (result != null) {
                    return result;
                }
                break;
            case TF_BOOLEAN:
                result = from == CastType.STRING
                        ? buildStringToBooleanCastDecode("'T'", "'F'")
                        : BooleanDecoder.toTrueFalseBoolean(from);
                if (result != null) {
                    return result;
                }
                break;
            case STRING:
                switch (from) {
                    case BOOLEAN:
                    case INTEGER_BOOLEAN:
                    case TF_BOOLEAN:
                    case YN_BOOLEAN:
                        return BooleanDecoder.toString(from);
                    case DATE:
                        return "to_char(?1,'YYYY-MM-DD')";
                    case TIME:
                        return "to_char(?1,'HH24:MI:SS')";
                    case TIMESTAMP:
                        return "to_char(?1,'YYYY-MM-DD HH24:MI:SS.FF9')";
                    case OFFSET_TIMESTAMP:
                        return "to_char(?1,'YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM')";
                    case ZONE_TIMESTAMP:
                        return "to_char(?1,'YYYY-MM-DD HH24:MI:SS.FF9 TZR')";
                }
                break;
            case CLOB:
                return "to_clob(?1)";
            case DATE:
                if (from == CastType.STRING) {
                    return "to_date(?1,'YYYY-MM-DD')";
                }
                break;
            case TIME:
                if (from == CastType.STRING) {
                    return "to_date(?1,'HH24:MI:SS')";
                }
                break;
            case TIMESTAMP:
                if (from == CastType.STRING) {
                    return "to_timestamp(?1,'YYYY-MM-DD HH24:MI:SS.FF9')";
                }
                break;
            case OFFSET_TIMESTAMP:
                if (from == CastType.STRING) {
                    return "to_timestamp_tz(?1,'YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM')";
                }
                break;
            case ZONE_TIMESTAMP:
                if (from == CastType.STRING) {
                    return "to_timestamp_tz(?1,'YYYY-MM-DD HH24:MI:SS.FF9 TZR')";
                }
                break;
        }
        return super.castPattern(from, to);
    }

    @Override
    public String extractPattern(TemporalUnit unit) {
        switch (unit) {
            case DAY_OF_WEEK:
                return "to_number(to_char(?2,'D'))";
            case DAY_OF_MONTH:
                return "to_number(to_char(?2,'DD'))";
            case DAY_OF_YEAR:
                return "to_number(to_char(?2,'DDD'))";
            case WEEK:
                return "to_number(to_char(?2,'IW'))";
            case WEEK_OF_YEAR:
                return "to_number(to_char(?2,'WW'))";
            case QUARTER:
                return "to_number(to_char(?2,'Q'))";
            case HOUR:
                return "to_number(to_char(?2,'HH24'))";
            case MINUTE:
                return "to_number(to_char(?2,'MI'))";
            case SECOND:
                return "to_number(to_char(?2,'SS'))";
            case EPOCH:
                return "trunc((cast(?2 at time zone 'UTC' as date) - date '1970-1-1')*86400)";
            default:
                return super.extractPattern(unit);
        }
    }

    /**
     * month/quarter/year 덧셈 식을 만든다.
     *
     * <p>DATE 는 애초에 시각이 없으므로 {@code add_months} 를 그대로 쓰고,
     * TIMESTAMP 는 {@code add_months} 가 잘라낸 소수 이하 초를 다시 더해 복원한다.
     * 기존 WEEK/DAY 분기와 같은 방식이다.
     *
     * @param months 더할 개월 수 식 (`?2`, `(?2)*3`, `(?2)*12`)
     */
    private static String monthsPattern(TemporalType temporalType, String months) {
        return String.format(
                temporalType == TemporalType.DATE ? ADD_MONTHS_DATE : ADD_MONTHS_TIMESTAMP,
                months, "?3");
    }

    @Override
    public String timestampaddPattern(TemporalUnit unit, TemporalType temporalType, IntervalType intervalType) {
        switch (unit) {
            case YEAR:
                return monthsPattern(temporalType, "(?2)*12");
            case QUARTER:
                return monthsPattern(temporalType, "(?2)*3");
            case MONTH:
                return monthsPattern(temporalType, "?2");
            case WEEK:
                if (temporalType != TemporalType.DATE) {
                    return "(?3+numtodsinterval((?2)*7,'day'))";
                }
                return "(?3+(?2)" + unit.conversionFactor(DAY, this) + ")";
            case DAY:
                if (temporalType == TemporalType.DATE) {
                    return "(?3+(?2))";
                }
                // fall through
            case HOUR:
            case MINUTE:
            case SECOND:
                return "(?3+numtodsinterval(?2,'?1'))";
            case NANOSECOND:
                return "(?3+numtodsinterval((?2)/1e9,'second'))";
            case NATIVE:
                return "(?3+numtodsinterval(?2,'second'))";
            default:
                throw new SemanticException(unit + " is not a legal field");
        }
    }

    @Override
    public String timestampdiffPattern(TemporalUnit unit, TemporalType fromTemporalType, TemporalType toTemporalType) {
        final StringBuilder pattern = new StringBuilder();
        final boolean hasTimePart = toTemporalType != TemporalType.DATE || fromTemporalType != TemporalType.DATE;
        switch (unit) {
            case YEAR:
                extractField(pattern, YEAR, unit);
                break;
            case QUARTER:
            case MONTH:
                pattern.append("(");
                extractField(pattern, YEAR, unit);
                pattern.append("+");
                extractField(pattern, MONTH, unit);
                pattern.append(")");
                break;
            case DAY:
                if (hasTimePart) {
                    pattern.append("(cast(?3 as date)-cast(?2 as date))");
                }
                else {
                    pattern.append("(?3-?2)");
                }
                break;
            case WEEK:
            case MINUTE:
            case SECOND:
            case HOUR:
                if (hasTimePart) {
                    pattern.append("((cast(?3 as date)-cast(?2 as date))");
                }
                else {
                    pattern.append("((?3-?2)");
                }
                pattern.append(TemporalUnit.DAY.conversionFactor(unit, this));
                pattern.append(")");
                break;
            case NATIVE:
            case NANOSECOND:
                if (hasTimePart) {
                    // lateral 미지원 → dual 서브쿼리 없이 extract 합산
                    pattern.append("(");
                    extractField(pattern, DAY, unit);
                    pattern.append("+");
                    extractField(pattern, HOUR, unit);
                    pattern.append("+");
                    extractField(pattern, MINUTE, unit);
                    pattern.append("+");
                    extractField(pattern, SECOND, unit);
                    pattern.append(")");
                }
                else {
                    pattern.append("((?3-?2)");
                    pattern.append(TemporalUnit.DAY.conversionFactor(unit, this));
                    pattern.append(")");
                }
                break;
            default:
                throw new SemanticException("Unrecognized field: " + unit);
        }
        return pattern.toString();
    }

    private void extractField(StringBuilder pattern, TemporalUnit unit, TemporalUnit toUnit) {
        pattern.append("extract(");
        pattern.append(translateExtractField(unit));
        pattern.append(" from (?3-?2)");
        switch (unit) {
            case YEAR:
            case MONTH:
                pattern.append(" year(9) to month");
                break;
            case DAY:
            case HOUR:
            case MINUTE:
            case SECOND:
                break;
            default:
                throw new SemanticException(unit + " is not a legal field");
        }
        pattern.append(")");
        pattern.append(unit.conversionFactor(toUnit, this));
    }

    @Override
    public String getAddColumnString() {
        return "add";
    }

    @Override
    public String getAlterColumnTypeString(String columnName, String columnType, String columnDefinition) {
        return "modify " + columnName + " " + columnType;
    }

    @Override
    public boolean supportsAlterColumnType() {
        return true;
    }

    @Override
    public boolean supportsValuesList() {
        return false;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        // drop table if exists 실측 OK
        return true;
    }

    @Override
    public boolean supportsIfExistsAfterAlterTable() {
        return false;
    }

    @Override
    public SelectItemReferenceStrategy getGroupBySelectItemReferenceStrategy() {
        return SelectItemReferenceStrategy.EXPRESSION;
    }

    @Override
    public String generatedAs(String generatedAs) {
        return " generated always as (" + generatedAs + ")";
    }

    @Override
    public IdentifierHelper buildIdentifierHelper(IdentifierHelperBuilder builder, DatabaseMetaData dbMetaData)
            throws SQLException {
        builder.setAutoQuoteInitialUnderscore(true);
        return super.buildIdentifierHelper(builder, dbMetaData);
    }

    @Override
    public boolean canDisableConstraints() {
        return true;
    }

    @Override
    public String getDisableConstraintStatement(String tableName, String name) {
        return "alter table " + tableName + " disable constraint " + name;
    }

    @Override
    public String getEnableConstraintStatement(String tableName, String name) {
        return "alter table " + tableName + " enable constraint " + name;
    }

    @Override
    public UniqueDelegate getUniqueDelegate() {
        return uniqueDelegate;
    }

    @Override
    public MutationOperation createOptionalTableUpdateOperation(
            EntityMutationTarget mutationTarget,
            OptionalTableUpdate optionalTableUpdate,
            SessionFactoryImplementor factory) {
        final TiberoSqlAstTranslator<?> translator = new TiberoSqlAstTranslator<>(factory, optionalTableUpdate);
        return translator.createMergeOperation(optionalTableUpdate);
    }

    @Override
    public DmlTargetColumnQualifierSupport getDmlTargetColumnQualifierSupport() {
        return DmlTargetColumnQualifierSupport.TABLE_ALIAS;
    }

    @Override
    public void appendBinaryLiteral(SqlAppender appender, byte[] bytes) {
        appender.appendSql("hextoraw('");
        PrimitiveByteArrayJavaType.INSTANCE.appendString(appender, bytes);
        appender.appendSql("')");
    }

    @Override
    public void appendDateTimeLiteral(
            SqlAppender appender,
            java.time.temporal.TemporalAccessor temporalAccessor,
            TemporalType precision,
            java.util.TimeZone jdbcTimeZone) {
        if (precision == TemporalType.TIMESTAMP && temporalAccessor.isSupported(ChronoField.OFFSET_SECONDS)) {
            appender.appendSql("timestamp '");
            appendAsTimestampWithNanos(appender, temporalAccessor, true, jdbcTimeZone, false);
            appender.appendSql('\'');
        }
        else {
            super.appendDateTimeLiteral(appender, temporalAccessor, precision, jdbcTimeZone);
        }
    }

    @Override
    public void appendDatetimeFormat(SqlAppender appender, String format) {
        appender.appendSql(OracleDialect.datetimeFormat(format, true, true).result());
    }


    /**
     * sequence 관련
     */
    @Override
    public SequenceSupport getSequenceSupport() {
        return tiberoSequenceSupport;
    }
    @Override
    public String getQuerySequencesString() {
        return "select * from all_sequences";
    }
    @Override
    public SequenceInformationExtractor getSequenceInformationExtractor() {
        return SequenceInformationExtractorTiberoDatabaseImpl.INSTANCE;
    }

    /**
     * constraints 관련
     */
    // 테이블 drop 시 FK 등 제약조건 함께 삭제하는 구문
    @Override
    public String getCascadeConstraintsString() {
        return " cascade constraints";
    }
    // cascade constraints로 한 번에 처리하니 별도 drop 불필요
    @Override
    public boolean dropConstraints() {
        return false;
    }


    @Override
    public LimitHandler getLimitHandler() {
        return TiberoLimitHandler.INSTANCE;
    }


    /**
     * lock 관련
     */
    @Override
    public String getForUpdateNowaitString() {
        return " for update nowait";
    }
    @Override
    public String getForUpdateString(String aliases) {
        return " for update of " + aliases;
    }
    @Override
    public String getForUpdateNowaitString(String aliases) {
        return " for update of " + aliases + " nowait";
    }
    @Override
    public String getForUpdateSkipLockedString() {
        return " for update skip locked";
    }
    @Override
    public String getForUpdateSkipLockedString(String aliases) {
        return " for update of " + aliases + " skip locked";
    }

    @Override
    public boolean supportsNoWait() {
        return true;
    }
    @Override
    public boolean supportsSkipLocked() {
        return true;
    }

    private String withTimeout(String lockString, int timeout) {
        switch ( timeout ) {
            case LockOptions.NO_WAIT:
                return supportsNoWait() ? lockString + " nowait" : lockString;
            case LockOptions.SKIP_LOCKED:
                return supportsSkipLocked() ? lockString + " skip locked" : lockString;
            case LockOptions.WAIT_FOREVER:
                return lockString;
            default:
                return supportsWait() ? lockString + " wait " + getTimeoutInSeconds( timeout ) : lockString;
        }
    }

    @Override
    public String getWriteLockString(int timeout) {
        return withTimeout(getForUpdateString(), timeout);
    }
    @Override
    public String getWriteLockString(String aliases, int timeout) {
        return withTimeout(getForUpdateString(aliases), timeout);
    }
    @Override
    public String getReadLockString(int timeout) {
        return getWriteLockString(timeout);
    }
    @Override
    public String getReadLockString(String aliases, int timeout) {
        return getWriteLockString(aliases, timeout);
    }


    @Override
    public String getSelectGUIDString() {
        return "select rawtohex(sys_guid()) from dual";
    }


    /**
     * error 관련
     */
    // SQLException 발생 시 violated constraint 이름 추출
    @Override
    public ViolatedConstraintNameExtractor getViolatedConstraintNameExtractor() {
        return EXTRACTOR;
    }

    private static final ViolatedConstraintNameExtractor EXTRACTOR =
        new TemplatedViolatedConstraintNameExtractor(sqle -> {
            switch (JdbcExceptionHelper.extractErrorCode(sqle)) {
                case -10006: // TBR-10006: CHECK constraint violation
                case -10007: // TBR-10007: UNIQUE constraint violation
                case -10008: // TBR-10008: INTEGRITY constraint violation (primary key not found)
                case -10009: // TBR-10009: INTEGRITY constraint violation (foreign key exists)
                    return extractUsingTemplate("(", ")", sqle.getMessage());
                case -10005: // TBR-10005: NOT NULL constraint violation
                    // not null constraint는 이름없는 경우가 많음
                    return null;
                default:
                    return null;
            }
        });

    // 표준 SQLState가 없는 SQLException을 Hibernate 표준 예외(JDBCException)로 변환
    @Override
    public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
        return (sqlException, message, sql) -> {
            final String constraintName;
            int errorCode = JdbcExceptionHelper.extractErrorCode(sqlException);
            switch (errorCode) {
                // lock timeouts
                case -12033: // TBR-12033: Lock acquisition failed in NOWAIT mode
                case -12034: // TBR-12034: Lock acquisition timed out in WAIT mode
                    return new LockTimeoutException(message, sqlException, sql);

                // deadlock
                case -12032: // TBR-12032: Deadlock detected
                    return new LockAcquisitionException(message, sqlException, sql);

                // statement cancelled
                case -12040: // TBR-12040: Statement cancelled
                    return new QueryTimeoutException(message, sqlException, sql);

                // data integrity violation
                case -10007: //TBR-10007: UNIQUE constraint violation
                    // ConstraintKind enum값이 UNIQUE, OTHER인 것으로 보아 UNIQUE constraint violation인 경우에는
                    // 사용자에게 상세한 메시지를 전달하려는 의도로 보임 (정상 사용자 시나리오에서도 발생 가능하기 때문에)
                    constraintName = getViolatedConstraintNameExtractor().extractConstraintName(sqlException);
                    return new ConstraintViolationException(
                        message,
                        sqlException,
                        sql,
                        ConstraintViolationException.ConstraintKind.UNIQUE,
                        constraintName
                    );
                    // tibero는 ORA-01400, ORA-01407 모두 TBR-10005: NOT NULL CONSTRAINT VIOLATION으로 처리
                default:
                    return null;
            }
        };
    }


    /**
     * REF CURSOR 관련
     */
    @Override
    public int registerResultSetOutParameter(CallableStatement statement, int col)
            throws SQLException {
        statement.registerOutParameter(col, TiberoTypes.CURSOR);
        col++;
        return col;
    }

    @Override
    public int registerResultSetOutParameter(CallableStatement statement, String name)
            throws SQLException {
        statement.registerOutParameter(name, TiberoTypes.CURSOR);
        return 1;
    }

    @Override
    public ResultSet getResultSet(CallableStatement ps)
            throws SQLException {
        ps.execute();
        return (ResultSet) ps.getObject(1);
    }

    @Override
    public ResultSet getResultSet(CallableStatement statement, int position)
            throws SQLException {
        return (ResultSet) statement.getObject(position);
    }

    @Override
    public ResultSet getResultSet(CallableStatement statement, String name)
            throws SQLException {
        return (ResultSet) statement.getObject(name);
    }

    // comment on 구문 지원 여부 (@Comment())
    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    /**
     * 상속 관계가 있는 엔티티에 대한 벌크 UPDATE/DELETE 처리 전략
     * - 삭제 대상 ID를 임시 테이블에 저장
     * - 각 테이블에서 UPDATE/DELETE (DELETE는 자식 -> 부모 순서)
     * - 임시 테이블 정리 (ON COMMIT DELETE ROWS로 자동)
     */
    @Override
    public SqmMultiTableMutationStrategy getFallbackSqmMutationStrategy(
            EntityMappingType rootEntityDescriptor,
            RuntimeModelCreationContext runtimeModelCreationContext) {
        return new GlobalTemporaryTableMutationStrategy(
            TemporaryTable.createIdTable(
                rootEntityDescriptor, basename ->
                TemporaryTable.ID_TABLE_PREFIX + basename,
                this,
                runtimeModelCreationContext
            ),
            runtimeModelCreationContext.getSessionFactory()
        );
    }

    /**
     * 상속 관계가 있는 엔티티에 대한 벌크 INSERT 처리 전략
     * - 삽입 대상 전체 데이터를 임시 테이블에 저장
     * - 각 테이블에 INSERT (부모 -> 자식 순서)
     * - 임시 테이블 정리 (ON COMMIT DELETE ROWS로 자동)
     */
    @Override
    public SqmMultiTableInsertStrategy getFallbackSqmInsertStrategy(
            EntityMappingType rootEntityDescriptor,
            RuntimeModelCreationContext runtimeModelCreationContext) {
        return new GlobalTemporaryTableInsertStrategy(
            TemporaryTable.createEntityTable(
                rootEntityDescriptor, name ->
                TemporaryTable.ENTITY_TABLE_PREFIX + name,
                this,
                runtimeModelCreationContext
            ),
            runtimeModelCreationContext.getSessionFactory()
        );
    }

    @Override
    public TemporaryTableKind getSupportedTemporaryTableKind() {
        return TemporaryTableKind.GLOBAL;
    }

    @Override
    public String getTemporaryTableCreateOptions() {
        return "on commit delete rows";
    }


    @Override
    public String getCurrentTimestampSelectString() {
        return "select systimestamp from dual";
    }

    /**
     * 현재 시간을 SELECT로 조회할 수 있는지 여부
      * SELECT SYSTIMESTAMP FROM DUAL
     */
    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    /**
     * 현재 시간 조회 SQL이 callable(프로시저 호출) 형태인지 여부
     * false : SELECT SYSTIMESTAMP FROM DUAL (일반 SELECT)
     * true : {? = call current_timestamp} (프로시저 호츌)
     */
    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    /**
     * SELECT 절에서 EXISTS를 직접 사용할 수 있는지 여부
     * -- true면 가능
     * SELECT EXISTS(SELECT 1 FROM orders WHERE user_id = 1) FROM DUAL
     * -- false면 CASE로 변환
     * SELECT CASE WHEN EXISTS(SELECT 1 FROM orders WHERE user_id = 1) THEN 1 ELSE 0 END FROM DUAL
     */
    @Override
    public boolean supportsExistsInSelect() {
        return false;
    }


    // INSERT/UPDATE 시 LOB 컬럼을 맨 마지막에 배치해야 하는지 여부
    @Override
    public boolean forceLobAsLastValue() {
        return true;
    }

    /**
     * SELECT FOR UPDATE에서 FOR UPDATE를 별도 쿼리로 분리해야 하는지 여부
     * - 조건
     * -- DISTINCT, GROUP BY, UNION, ORDER BY + LIMIT, OFFSET
     * - 이유
     * -- DISTINCT : 중복 제거 후 원본 행 특정 불가
     * -- GROUP BY : 집계 결과는 실제 행이 아님
     * -- UNION : 여러 테이블 합쳐서 출처 불명
     * -- ORDER BY + LIMIT / OFFSET : 서브쿼리로 감싸져서 락 대상 불명확
     *
     * true : hibernate가 Follow-on Locking 방식 사용하여 분리
     */
    @Override
    public boolean useFollowOnLocking(String sql, QueryOptions queryOptions) {
        if (StringHelper.isEmpty(sql) || queryOptions == null ) {
            return true;
        }
        return DISTINCT_KEYWORD_PATTERN.matcher(sql).find()
            || GROUP_BY_KEYWORD_PATTERN.matcher(sql).find()
            || UNION_KEYWORD_PATTERN.matcher(sql).find()
            || ORDER_BY_KEYWORD_PATTERN.matcher(sql).find() && queryOptions.hasLimit()
            || queryOptions.hasLimit() && queryOptions.getLimit().getFirstRow() != null;
    }

    /**
     * @GeneratedValue(strategy = GenerationType.AUTO) 사용 시 기본 자동 증가 ID 생성 전략
     * Tibero는 identity, sequence 둘 다 지원
     */
    @Override
    public String getNativeIdentifierGeneratorStrategy() {
        return "sequence";
    }

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
        return TiberoIdentityColumnSupport.INSTANCE;
    }

    // optimizer hint를 SQL에 삽입
    @Override
    public String getQueryHintString(String sql, String hints) {
        String statementType = statementType(sql); // SELECT, UPDATE, DELETE

        int start = sql.indexOf(statementType);
        if (start < 0) {
            return sql;
        } else {
            int end = start + statementType.length();
            return sql.substring(0, end) + " /*+ " + hints + " */" + sql.substring(end);
        }
    }

    // 2개 이상의 optimezier hint를 SQL에 삽입
    @Override
    public String getQueryHintString(String query, List<String> hintList) {
        if (hintList.isEmpty()) {
            return query;
        } else {
            final String hints = StringHelper.join(" ", hintList);
            return StringHelper.isEmpty(hints)? query : getQueryHintString(query, hints);
        }
    }

    // 최대 식별자 길이
    @Override
    public int getMaxIdentifierLength() {
        return 128;
    }

    // hibernate이 alias 생성 시 고유성 보장을 위해 추가하는 suffix 길이(약 10) 고려
    @Override
    public int getMaxAliasLength() {
        return 118;
    }

    // stored procedure 결과셋을 ref cursor로 반환
    @Override
    public CallableStatementSupport getCallableStatementSupport() {
        // 표준 구현은 이름 파라미터도 '?' 로만 내보내 등록 순서 기반 위치 바인딩이 된다.
        // 선언 순서와 등록 순서가 다르면 예외 없이 값이 뒤바뀌므로 name => ? 표기를 쓴다.
        return TiberoCallableStatementSupport.REF_CURSOR_INSTANCE;
    }

    // CREATE SCHEMA 구문 지원 여부
    // 사용자 == 스키마 이기 때문에 별도 스키마 생성 개념x
    @Override
    public boolean canCreateSchema() {
        return false;
    }

    // 현재 스키마 조회하는 SQL
    @Override
    public String getCurrentSchemaCommand() {
        return "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
    }

    /**
     * 윈도우 함수에서 PARTITION BY 지원 여부
     * -- true면 이런 쿼리 사용 가능
     * SELECT
     *     name,
     *     dept,
     *     SUM(salary) OVER (PARTITION BY dept) as dept_total,
     *     ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
     * FROM employees;
     */
    @Override
    public boolean supportsPartitionBy() {
        return true;
    }

    @Override
    public boolean supportsWindowFunctions() {
        return true;
    }

    @Override
    public boolean supportsFetchClause(FetchClauseType type) {
        // ROWS ONLY 실측 OK. WITH TIES / PERCENT 는 미구현
        return type == FetchClauseType.ROWS_ONLY;
    }

    @Override
    public boolean supportsOffsetInSubquery() {
        return true;
    }

    @Override
    public boolean supportsRecursiveCTE() {
        return true;
    }

    @Override
    public boolean supportsLateral() {
        // LATERAL / CROSS APPLY 실측 실패 → 정직하게 off
        return false;
    }

    @Override
    public boolean supportsInsertReturningGeneratedKeys() {
        return true;
    }

    @Override
    public boolean supportsFromClauseInUpdate() {
        // raw UPDATE..FROM 은 실패하지만 translator가 inline-view로 에뮬레이션
        return true;
    }

    @Override
    public RowLockStrategy getWriteRowLockStrategy() {
        return RowLockStrategy.COLUMN;
    }

    @Override
    public TimeZoneSupport getTimeZoneSupport() {
        return TimeZoneSupport.NATIVE;
    }

    @Override
    public boolean supportsTemporalLiteralOffset() {
        // ANSI timestamp'...' +09:00 은 가능하나 JDBC escape 경로와의 정합을 위해 false
        return false;
    }

    /**
     * 튜플(여러 컬럼)에 대한 COUNT DISTINCT를 지원하는지 여부
     * -- true면 이런 쿼리 가능
     * SELECT COUNT(DISTINCT (col1, col2)) FROM table;
     * -- false면 이렇게 해야 함
     * SELECT COUNT(*) FROM (
     *     SELECT DISTINCT col1, col2 FROM table
     * );
     */
    @Override
    public boolean supportsTupleDistinctCounts() {
        return false;
    }

    private String statementType(String sql) {
        final Matcher matcher = SQL_STATEMENT_TYPE_PATTERN.matcher(sql);
        if ((matcher.matches()) && (matcher.groupCount() == 1)) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Can't determine SQL statement type for statement: " + sql);
    }
}
