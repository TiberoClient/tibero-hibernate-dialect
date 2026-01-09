package com.tmax.tibero.hibernate.dialect;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.hibernate.cfg.BatchSettings.BATCH_VERSIONED_DATA;
import static org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtractor.extractUsingTemplate;
import static org.hibernate.type.SqlTypes.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tmax.tibero.hibernate.dialect.identity.TiberoIdentityColumnSupport;
import com.tmax.tibero.hibernate.dialect.pagination.TiberoLimitHandler;
import com.tmax.tibero.hibernate.dialect.sequence.TiberoSequenceSupport;
import com.tmax.tibero.hibernate.tool.schema.extract.internal.SequenceInformationExtractorTiberoDatabaseImpl;
import org.hibernate.LockOptions;
import org.hibernate.QueryTimeoutException;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableKind;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
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
import org.hibernate.query.sqm.produce.function.StandardFunctionReturnTypeResolvers;
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

public class TiberoDialect extends Dialect {
    private static final Pattern DISTINCT_KEYWORD_PATTERN = Pattern.compile("\\bdistinct\\b", CASE_INSENSITIVE);
    private static final Pattern GROUP_BY_KEYWORD_PATTERN = Pattern.compile("\\bgroup\\s+by\\b", CASE_INSENSITIVE);
    private static final Pattern ORDER_BY_KEYWORD_PATTERN = Pattern.compile("\\border\\s+by\\b", CASE_INSENSITIVE);
    private static final Pattern UNION_KEYWORD_PATTERN = Pattern.compile("\\bunion\\b", CASE_INSENSITIVE);

    private static final Pattern SQL_STATEMENT_TYPE_PATTERN =
        Pattern.compile("^(?:/\\*.*?\\*/)?\\s*(select|insert|update|delete)\\s+.*?", CASE_INSENSITIVE);

    private final SequenceSupport tiberoSequenceSupport = TiberoSequenceSupport.getInstance(this);

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


    // 함수 등록
    @Override
    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);

        SqmFunctionRegistry registry = functionContributions.getFunctionRegistry();
        TypeConfiguration typeConfig = functionContributions.getTypeConfiguration();
        BasicTypeRegistry basicTypeRegistry = typeConfig.getBasicTypeRegistry();

        // 수학 함수들
        registry.register("abs", new StandardSQLFunction("abs"));
        registry.register("sign", new StandardSQLFunction("sign", StandardBasicTypes.INTEGER));
        registry.register("exp", new StandardSQLFunction("exp", StandardBasicTypes.DOUBLE));
        registry.register("ln", new StandardSQLFunction("ln", StandardBasicTypes.DOUBLE));
        registry.register("stddev", new StandardSQLFunction("stddev", StandardBasicTypes.DOUBLE));
        registry.register("sqrt", new StandardSQLFunction("sqrt", StandardBasicTypes.DOUBLE));
        registry.register("variance", new StandardSQLFunction("variance", StandardBasicTypes.DOUBLE));
        registry.register("round", new StandardSQLFunction("round"));
        registry.register("trunc", new StandardSQLFunction("trunc"));
        registry.register("ceil", new StandardSQLFunction("ceil"));
        registry.register("floor", new StandardSQLFunction("floor"));

        // 삼각 함수들
        registry.register("acos", new StandardSQLFunction("acos", StandardBasicTypes.DOUBLE));
        registry.register("asin", new StandardSQLFunction("asin", StandardBasicTypes.DOUBLE));
        registry.register("atan", new StandardSQLFunction("atan", StandardBasicTypes.DOUBLE));
        registry.register("cos", new StandardSQLFunction("cos", StandardBasicTypes.DOUBLE));
        registry.register("cosh", new StandardSQLFunction("cosh", StandardBasicTypes.DOUBLE));
        registry.register("sin", new StandardSQLFunction("sin", StandardBasicTypes.DOUBLE));
        registry.register("sinh", new StandardSQLFunction("sinh", StandardBasicTypes.DOUBLE));
        registry.register("tan", new StandardSQLFunction("tan", StandardBasicTypes.DOUBLE));
        registry.register("tanh", new StandardSQLFunction("tanh", StandardBasicTypes.DOUBLE));

        // 비트 연산
        registry.register("bitand", new StandardSQLFunction("bitand"));

        // 문자열 함수들
        registry.register("chr", new StandardSQLFunction("chr", StandardBasicTypes.CHARACTER));
        registry.register("initcap", new StandardSQLFunction("initcap"));
        registry.register("lower", new StandardSQLFunction("lower"));
        registry.register("ltrim", new StandardSQLFunction("ltrim"));
        registry.register("rtrim", new StandardSQLFunction("rtrim"));
        registry.register("soundex", new StandardSQLFunction("soundex"));
        registry.register("upper", new StandardSQLFunction("upper"));
        registry.register("ascii", new StandardSQLFunction("ascii", StandardBasicTypes.INTEGER));
        registry.register("instr", new StandardSQLFunction("instr", StandardBasicTypes.INTEGER));
        registry.register("instrb", new StandardSQLFunction("instrb", StandardBasicTypes.INTEGER));
        registry.register("lpad", new StandardSQLFunction("lpad", StandardBasicTypes.STRING));
        registry.register("replace", new StandardSQLFunction("replace", StandardBasicTypes.STRING));
        registry.register("rpad", new StandardSQLFunction("rpad", StandardBasicTypes.STRING));
        registry.register("substr", new StandardSQLFunction("substr", StandardBasicTypes.STRING));
        registry.register("substrb", new StandardSQLFunction("substrb", StandardBasicTypes.STRING));
        registry.register("translate", new StandardSQLFunction("translate", StandardBasicTypes.STRING));

        // 형변환 함수들
        registry.register("to_char", new StandardSQLFunction("to_char", StandardBasicTypes.STRING));
        registry.register("to_date", new StandardSQLFunction("to_date", StandardBasicTypes.TIMESTAMP));

        // 날짜/시간 함수들
        registry.noArgsBuilder("current_date")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.DATE))
                .setUseParenthesesWhenNoArgs(false)
                .register();
        registry.noArgsBuilder("current_time")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.TIME))
                .setUseParenthesesWhenNoArgs(false)
                .register();
        registry.noArgsBuilder("current_timestamp")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.TIMESTAMP))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.noArgsBuilder("sysdate")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.DATE))
                .setUseParenthesesWhenNoArgs(false)
                .register();
        registry.noArgsBuilder("systimestamp")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.TIMESTAMP))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.noArgsBuilder("uid")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.INTEGER))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.noArgsBuilder("user")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.register("last_day", new StandardSQLFunction("last_day", StandardBasicTypes.DATE));

        // 특수 함수들
        registry.noArgsBuilder("rowid")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.noArgsBuilder("rownum")
                .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.LONG))
                .setUseParenthesesWhenNoArgs(false)
                .register();

        registry.namedDescriptorBuilder("concat")
            .setExactArgumentCount(2)
            .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
            // 인자 타입이 모호하면 문자열로 처리
            .setArgumentTypeResolver(StandardFunctionArgumentTypeResolvers.impliedOrInvariant(typeConfig, FunctionParameterType.STRING))
            .register();

        // 별칭 및 SQLFunctionTemplate 대체
        registry.register("substring", new StandardSQLFunction("substr", StandardBasicTypes.STRING));

        registry.patternDescriptorBuilder("locate", "instr(?2,?1)")
            .setReturnTypeResolver(StandardFunctionReturnTypeResolvers.invariant(
                basicTypeRegistry.resolve(StandardBasicTypes.INTEGER)))
            .setExactArgumentCount(2)
            .register();

        registry.patternDescriptorBuilder("bit_length", "vsize(?1)*8")
            .setReturnTypeResolver(StandardFunctionReturnTypeResolvers.invariant(
                basicTypeRegistry.resolve(StandardBasicTypes.INTEGER)))
            .setExactArgumentCount(1)
            .register();

        // NvlFunction 대체 - coalesce
        registry.register("coalesce", new StandardSQLFunction("coalesce"));

        // 추가 수학 함수들
        registry.register("atan2", new StandardSQLFunction("atan2", StandardBasicTypes.FLOAT));
        registry.register("log", new StandardSQLFunction("log", StandardBasicTypes.INTEGER));
        registry.register("mod", new StandardSQLFunction("mod", StandardBasicTypes.INTEGER));
        registry.register("nvl", new StandardSQLFunction("nvl"));
        registry.register("nvl2", new StandardSQLFunction("nvl2"));
        registry.register("power", new StandardSQLFunction("power", StandardBasicTypes.FLOAT));

        // 날짜 연산 함수들
        registry.register("add_months", new StandardSQLFunction("add_months", StandardBasicTypes.DATE));
        registry.register("months_between", new StandardSQLFunction("months_between", StandardBasicTypes.FLOAT));
        registry.register("next_day", new StandardSQLFunction("next_day", StandardBasicTypes.DATE));

        // str 별칭
        registry.register("str", new StandardSQLFunction("to_char", StandardBasicTypes.STRING));
    }

    @Override
    public int getMaxVarcharLength() {
        return 65532;
    }

    @Override
    public int getMaxVarbinaryLength() {
        return 2000;
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
    public String getAddColumnString() {
        return "add";
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
        return StandardCallableStatementSupport.REF_CURSOR_INSTANCE;
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
