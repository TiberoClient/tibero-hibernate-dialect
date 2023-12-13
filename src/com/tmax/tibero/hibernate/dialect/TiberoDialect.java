package com.tmax.tibero.hibernate.dialect;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tmax.tibero.hibernate.dialect.identity.TiberoIdentityColumnSupport;
import com.tmax.tibero.hibernate.tool.schema.extract.internal.SequenceInformationExtractorTiberoDatabaseImpl;
import org.hibernate.JDBCException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.NvlFunction;
import org.hibernate.dialect.function.SQLFunctionTemplate;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtracter;
import org.hibernate.exception.spi.ViolatedConstraintNameExtracter;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.global.GlobalTemporaryTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.AfterUseAction;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.procedure.internal.StandardCallableStatementSupport;
import org.hibernate.procedure.spi.CallableStatementSupport;
import org.hibernate.sql.*;
import org.hibernate.tool.schema.extract.spi.SequenceInformationExtractor;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.descriptor.sql.BitTypeDescriptor;
import org.hibernate.type.descriptor.sql.SqlTypeDescriptor;

public class TiberoDialect extends Dialect {
    private static final Pattern DISTINCT_KEYWORD_PATTERN = Pattern.compile("\\bdistinct\\b");
    private static final Pattern GROUP_BY_KEYWORD_PATTERN = Pattern.compile("\\bgroup\\sby\\b");
    private static final Pattern ORDER_BY_KEYWORD_PATTERN = Pattern.compile("\\border\\sby\\b");
    private static final Pattern UNION_KEYWORD_PATTERN = Pattern.compile("\\bunion\\b");
    private static final Pattern SQL_STATEMENT_TYPE_PATTERN = Pattern.compile("^(?:\\/\\*.*?\\*\\/)?\\s*(select|insert|update|delete)\\s+.*?");
    private static final AbstractLimitHandler LIMIT_HANDLER = new AbstractLimitHandler() {
        public String processSql(String sql, RowSelection selection) {
            boolean hasOffset = LimitHelper.hasFirstRow(selection);
            sql = sql.trim();
            String forUpdateClause = null;
            boolean isForUpdate = false;
            int forUpdateIndex = sql.toLowerCase(Locale.ROOT).lastIndexOf("for update");
            if (forUpdateIndex > -1) {
                forUpdateClause = sql.substring(forUpdateIndex);
                sql = sql.substring(0, forUpdateIndex - 1);
                isForUpdate = true;
            }
            StringBuilder pagingSelect = new StringBuilder(sql.length() + 100);
            if (hasOffset) {
                pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
            } else {
                pagingSelect.append("select * from ( ");
            }
            pagingSelect.append(sql);
            if (hasOffset) {
                pagingSelect.append(" ) row_ where rownum <= ?) where rownum_ > ?");
            } else {
                pagingSelect.append(" ) where rownum <= ?");
            }
            if (isForUpdate) {
                pagingSelect.append(" ");
                pagingSelect.append(forUpdateClause);
            }
            return pagingSelect.toString();
        }

        public boolean supportsLimit() {
            return true;
        }

        public boolean bindLimitParametersInReverseOrder() {
            return true;
        }

        public boolean useMaxForLimit() {
            return true;
        }
    };
    private static final int PARAM_LIST_SIZE_LIMIT = 1000;

    public TiberoDialect() {
        registerCharacterTypeMappings();
        registerNumericTypeMappings();
        registerDateTimeTypeMappings();
        registerLargeObjectTypeMappings();
        registerReverseHibernateTypeMappings();
        registerFunctions();
        registerDefaultProperties();
    }

    protected void registerCharacterTypeMappings() {
        registerColumnType(1, "char(1 char)");
        registerColumnType(12, 4000L, "varchar2($l char)");
        registerColumnType(12, "long");
        registerColumnType(-9, "nvarchar2($l)");
        registerColumnType(-16, "nvarchar2($l)");
    }

    protected void registerNumericTypeMappings() {
        registerColumnType(-7, "number(1,0)");
        registerColumnType(-5, "number(19,0)");
        registerColumnType(5, "number(5,0)");
        registerColumnType(-6, "number(3,0)");
        registerColumnType(4, "number(10,0)");

        registerColumnType(6, "float");
        registerColumnType(8, "double precision");
        registerColumnType(2, "number($p,$s)");
        registerColumnType(3, "number($p,$s)");

        registerColumnType(16, "number(1,0)");
    }

    protected void registerDateTimeTypeMappings() {
        registerColumnType(91, "date");
        registerColumnType(92, "date");
        registerColumnType(93, "timestamp");
    }

    protected void registerLargeObjectTypeMappings() {
        registerColumnType(-2, 2000L, "raw($l)");
        registerColumnType(-2, "long raw");

        registerColumnType(-3, 2000L, "raw($l)");
        registerColumnType(-3, "long raw");

        registerColumnType(2004, "blob");
        registerColumnType(2005, "clob");

        registerColumnType(-1, "long");
        registerColumnType(-4, "long raw");
    }

    protected void registerReverseHibernateTypeMappings() {
    }

    protected void registerFunctions() {
        registerFunction("abs", new StandardSQLFunction("abs"));
        registerFunction("sign", new StandardSQLFunction("sign", StandardBasicTypes.INTEGER));

        registerFunction("acos", new StandardSQLFunction("acos", StandardBasicTypes.DOUBLE));
        registerFunction("asin", new StandardSQLFunction("asin", StandardBasicTypes.DOUBLE));
        registerFunction("atan", new StandardSQLFunction("atan", StandardBasicTypes.DOUBLE));
        registerFunction("bitand", new StandardSQLFunction("bitand"));
        registerFunction("cos", new StandardSQLFunction("cos", StandardBasicTypes.DOUBLE));
        registerFunction("cosh", new StandardSQLFunction("cosh", StandardBasicTypes.DOUBLE));
        registerFunction("exp", new StandardSQLFunction("exp", StandardBasicTypes.DOUBLE));
        registerFunction("ln", new StandardSQLFunction("ln", StandardBasicTypes.DOUBLE));
        registerFunction("sin", new StandardSQLFunction("sin", StandardBasicTypes.DOUBLE));
        registerFunction("sinh", new StandardSQLFunction("sinh", StandardBasicTypes.DOUBLE));
        registerFunction("stddev", new StandardSQLFunction("stddev", StandardBasicTypes.DOUBLE));
        registerFunction("sqrt", new StandardSQLFunction("sqrt", StandardBasicTypes.DOUBLE));
        registerFunction("tan", new StandardSQLFunction("tan", StandardBasicTypes.DOUBLE));
        registerFunction("tanh", new StandardSQLFunction("tanh", StandardBasicTypes.DOUBLE));
        registerFunction("variance", new StandardSQLFunction("variance", StandardBasicTypes.DOUBLE));

        registerFunction("round", new StandardSQLFunction("round"));
        registerFunction("trunc", new StandardSQLFunction("trunc"));
        registerFunction("ceil", new StandardSQLFunction("ceil"));
        registerFunction("floor", new StandardSQLFunction("floor"));

        registerFunction("chr", new StandardSQLFunction("chr", StandardBasicTypes.CHARACTER));
        registerFunction("initcap", new StandardSQLFunction("initcap"));
        registerFunction("lower", new StandardSQLFunction("lower"));
        registerFunction("ltrim", new StandardSQLFunction("ltrim"));
        registerFunction("rtrim", new StandardSQLFunction("rtrim"));
        registerFunction("soundex", new StandardSQLFunction("soundex"));
        registerFunction("upper", new StandardSQLFunction("upper"));
        registerFunction("ascii", new StandardSQLFunction("ascii", StandardBasicTypes.INTEGER));

        registerFunction("to_char", new StandardSQLFunction("to_char", StandardBasicTypes.STRING));
        registerFunction("to_date", new StandardSQLFunction("to_date", StandardBasicTypes.TIMESTAMP));

        registerFunction("current_date", new NoArgSQLFunction("current_date", StandardBasicTypes.DATE, false));
        registerFunction("current_time", new NoArgSQLFunction("current_timestamp", StandardBasicTypes.TIME, false));
        registerFunction("current_timestamp", new NoArgSQLFunction("current_timestamp", StandardBasicTypes.TIMESTAMP, false));

        registerFunction("last_day", new StandardSQLFunction("last_day", StandardBasicTypes.DATE));
        registerFunction("sysdate", new NoArgSQLFunction("sysdate", StandardBasicTypes.DATE, false));
        registerFunction("systimestamp", new NoArgSQLFunction("systimestamp", StandardBasicTypes.TIMESTAMP, false));
        registerFunction("uid", new NoArgSQLFunction("uid", StandardBasicTypes.INTEGER, false));
        registerFunction("user", new NoArgSQLFunction("user", StandardBasicTypes.STRING, false));

        registerFunction("rowid", new NoArgSQLFunction("rowid", StandardBasicTypes.LONG, false));
        registerFunction("rownum", new NoArgSQLFunction("rownum", StandardBasicTypes.LONG, false));

        registerFunction("concat", new VarArgsSQLFunction(StandardBasicTypes.STRING, "", "||", ""));
        registerFunction("instr", new StandardSQLFunction("instr", StandardBasicTypes.INTEGER));
        registerFunction("instrb", new StandardSQLFunction("instrb", StandardBasicTypes.INTEGER));
        registerFunction("lpad", new StandardSQLFunction("lpad", StandardBasicTypes.STRING));
        registerFunction("replace", new StandardSQLFunction("replace", StandardBasicTypes.STRING));
        registerFunction("rpad", new StandardSQLFunction("rpad", StandardBasicTypes.STRING));
        registerFunction("substr", new StandardSQLFunction("substr", StandardBasicTypes.STRING));
        registerFunction("substrb", new StandardSQLFunction("substrb", StandardBasicTypes.STRING));
        registerFunction("translate", new StandardSQLFunction("translate", StandardBasicTypes.STRING));

        registerFunction("substring", new StandardSQLFunction("substr", StandardBasicTypes.STRING));
        registerFunction("locate", new SQLFunctionTemplate(StandardBasicTypes.INTEGER, "instr(?2,?1)"));
        registerFunction("bit_length", new SQLFunctionTemplate(StandardBasicTypes.INTEGER, "vsize(?1)*8"));
        registerFunction("coalesce", new NvlFunction());

        registerFunction("atan2", new StandardSQLFunction("atan2", StandardBasicTypes.FLOAT));
        registerFunction("log", new StandardSQLFunction("log", StandardBasicTypes.INTEGER));
        registerFunction("mod", new StandardSQLFunction("mod", StandardBasicTypes.INTEGER));
        registerFunction("nvl", new StandardSQLFunction("nvl"));
        registerFunction("nvl2", new StandardSQLFunction("nvl2"));
        registerFunction("power", new StandardSQLFunction("power", StandardBasicTypes.FLOAT));

        registerFunction("add_months", new StandardSQLFunction("add_months", StandardBasicTypes.DATE));
        registerFunction("months_between", new StandardSQLFunction("months_between", StandardBasicTypes.FLOAT));
        registerFunction("next_day", new StandardSQLFunction("next_day", StandardBasicTypes.DATE));

        registerFunction("str", new StandardSQLFunction("to_char", StandardBasicTypes.STRING));
    }

    protected void registerDefaultProperties() {

        getDefaultProperties().setProperty("hibernate.jdbc.use_streams_for_binary", "true");
        getDefaultProperties().setProperty("hibernate.jdbc.batch_size", "15");

        getDefaultProperties().setProperty("hibernate.jdbc.use_get_generated_keys", "false");
        getDefaultProperties().setProperty("hibernate.jdbc.batch_versioned_data", "false");

        getDefaultProperties().setProperty("hibernate.jdbc.use_get_generated_keys", "true");
    }

    protected SqlTypeDescriptor getSqlTypeDescriptorOverride(int sqlCode) {
        return sqlCode == 16 ? BitTypeDescriptor.INSTANCE : super.getSqlTypeDescriptorOverride(sqlCode);
    }

    public JoinFragment createOuterJoinFragment() {
        return new ANSIJoinFragment();
    }

    public String getCrossJoinSeparator() {
        return " cross join ";
    }

    public CaseFragment createCaseFragment() {
        return new ANSICaseFragment();
    }

    public LimitHandler getLimitHandler() {
        return LIMIT_HANDLER;
    }

    public String getLimitString(String sql, boolean hasOffset) {
        sql = sql.trim();
        String forUpdateClause = null;
        boolean isForUpdate = false;
        int forUpdateIndex = sql.toLowerCase(Locale.ROOT).lastIndexOf("for update");
        if (forUpdateIndex > -1) {
            forUpdateClause = sql.substring(forUpdateIndex);
            sql = sql.substring(0, forUpdateIndex - 1);
            isForUpdate = true;
        }
        StringBuilder pagingSelect = new StringBuilder(sql.length() + 100);
        if (hasOffset) {
            pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
        } else {
            pagingSelect.append("select * from ( ");
        }
        pagingSelect.append(sql);
        if (hasOffset) {
            pagingSelect.append(" ) row_ where rownum <= ?) where rownum_ > ?");
        } else {
            pagingSelect.append(" ) where rownum <= ?");
        }
        if (isForUpdate) {
            pagingSelect.append(" ");
            pagingSelect.append(forUpdateClause);
        }
        return pagingSelect.toString();
    }

    public String getBasicSelectClauseNullString(int sqlType) {
        return super.getSelectClauseNullString(sqlType);
    }

    public String getSelectClauseNullString(int sqlType) {
        return getBasicSelectClauseNullString(sqlType);
    }

    public String getCurrentTimestampSelectString() {
        return "select systimestamp from dual";
    }

    public String getCurrentTimestampSQLFunctionName() {
        return "current_timestamp";
    }

    public String getAddColumnString() {
        return "add";
    }

    public String getSequenceNextValString(String sequenceName) {
        return "select " + getSelectSequenceNextValString(sequenceName) + " from dual";
    }

    public String getSelectSequenceNextValString(String sequenceName) {
        return sequenceName + ".nextval";
    }

    public String getCreateSequenceString(String sequenceName) {
        return "create sequence " + sequenceName;
    }

    protected String getCreateSequenceString(String sequenceName, int initialValue, int incrementSize) {
        if ((initialValue < 0) && (incrementSize > 0)) {
            return
                    String.format("%s minvalue %d start with %d increment by %d", new Object[]{

                            getCreateSequenceString(sequenceName),
                            Integer.valueOf(initialValue),
                            Integer.valueOf(initialValue),
                            Integer.valueOf(incrementSize)});
        }
        if ((initialValue > 0) && (incrementSize < 0)) {
            return
                    String.format("%s maxvalue %d start with %d increment by %d", new Object[]{

                            getCreateSequenceString(sequenceName),
                            Integer.valueOf(initialValue),
                            Integer.valueOf(initialValue),
                            Integer.valueOf(incrementSize)});
        }
        return
                String.format("%s start with %d increment by  %d", new Object[]{

                        getCreateSequenceString(sequenceName),
                        Integer.valueOf(initialValue),
                        Integer.valueOf(incrementSize)});
    }

    public String getDropSequenceString(String sequenceName) {
        return "drop sequence " + sequenceName;
    }

    public String getCascadeConstraintsString() {
        return " cascade constraints";
    }

    public boolean dropConstraints() {
        return false;
    }

    public String getForUpdateNowaitString() {
        return " for update nowait";
    }

    public boolean supportsSequences() {
        return true;
    }

    public boolean supportsPooledSequences() {
        return true;
    }

    public boolean supportsLimit() {
        return true;
    }

    public String getForUpdateString(String aliases) {
        return " for update";
    }

    public String getForUpdateNowaitString(String aliases) {
        return getForUpdateString() + " of " + aliases + " nowait";
    }

    public String getWriteLockString(int timeout) {
        if (timeout == -2) {
            return getForUpdateSkipLockedString();
        }
        return super.getWriteLockString(timeout);
    }

    public String getWriteLockString(String aliases, int timeout) {
        if (timeout == -2) {
            return getForUpdateSkipLockedString(aliases);
        }
        return super.getWriteLockString(aliases, timeout);
    }

    public String getForUpdateSkipLockedString() {
        return " for update skip locked";
    }

    public String getForUpdateSkipLockedString(String aliases) {
        return getForUpdateString() + " of " + aliases + " skip locked";
    }

    public String getReadLockString(int timeout) {
        return getWriteLockString(timeout);
    }

    public boolean bindLimitParametersInReverseOrder() {
        return true;
    }

    public boolean useMaxForLimit() {
        return true;
    }

    public boolean forUpdateOfColumns() {
        return true;
    }

    public String getQuerySequencesString() {
        return "select * from all_sequences";
    }

    public SequenceInformationExtractor getSequenceInformationExtractor() {
        return SequenceInformationExtractorTiberoDatabaseImpl.INSTANCE;
    }

    public String getSelectGUIDString() {
        return "select rawtohex(sys_guid()) from dual";
    }

    public ViolatedConstraintNameExtracter getViolatedConstraintNameExtracter() {
        return EXTRACTER;
    }

    private static final ViolatedConstraintNameExtracter EXTRACTER = new TemplatedViolatedConstraintNameExtracter() {
        protected String doExtractConstraintName(SQLException sqle)
                throws NumberFormatException {
            int errorCode = JdbcExceptionHelper.extractErrorCode(sqle);
            if ((errorCode == 1) || (errorCode == 2291) || (errorCode == 2292)) {
                return extractUsingTemplate("(", ")", sqle.getMessage());
            }
            if (errorCode == 1400) {
                return null;
            }
            return null;
        }
    };

    public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
        return new SQLExceptionConversionDelegate() {
            public JDBCException convert(SQLException sqlException, String message, String sql) {
                int errorCode = JdbcExceptionHelper.extractErrorCode(sqlException);
                if (errorCode == 30006) {
                    throw new LockTimeoutException(message, sqlException, sql);
                }
                if (errorCode == 54) {
                    throw new LockTimeoutException(message, sqlException, sql);
                }
                if (4021 == errorCode) {
                    throw new LockTimeoutException(message, sqlException, sql);
                }
                if (60 == errorCode) {
                    return new LockAcquisitionException(message, sqlException, sql);
                }
                if (4020 == errorCode) {
                    return new LockAcquisitionException(message, sqlException, sql);
                }
                if (1013 == errorCode) {
                    throw new QueryTimeoutException(message, sqlException, sql);
                }
                if (1407 == errorCode) {
                    String constraintName = TiberoDialect.this.getViolatedConstraintNameExtracter().extractConstraintName(sqlException);
                    return new ConstraintViolationException(message, sqlException, sql, constraintName);
                }
                return null;
            }
        };
    }

    public int registerResultSetOutParameter(CallableStatement statement, int col)
            throws SQLException {
        statement.registerOutParameter(col, -17);
        col++;
        return col;
    }

    public int registerResultSetOutParameter(CallableStatement statement, String name)
            throws SQLException {
        statement.registerOutParameter(name, -17);
        return 1;
    }

    public ResultSet getResultSet(CallableStatement ps)
            throws SQLException {
        ps.execute();
        return (ResultSet) ps.getObject(1);
    }

    public ResultSet getResultSet(CallableStatement statement, int position)
            throws SQLException {
        return (ResultSet) statement.getObject(position);
    }

    public ResultSet getResultSet(CallableStatement statement, String name)
            throws SQLException {
        return (ResultSet) statement.getObject(name);
    }

    public boolean supportsUnionAll() {
        return true;
    }

    public boolean supportsCommentOn() {
        return true;
    }

    public MultiTableBulkIdStrategy getDefaultMultiTableBulkIdStrategy() {
        return new GlobalTemporaryTableBulkIdStrategy(new IdTableSupportStandardImpl() {
            public String generateIdTableName(String baseName) {
                String name = super.generateIdTableName(baseName);
                return name.length() > 30 ? name.substring(0, 30) : name;
            }

            public String getCreateIdTableCommand() {
                return "create global temporary table";
            }

            public String getCreateIdTableStatementOptions() {
                return "on commit delete rows";
            }
        }, AfterUseAction.CLEAN);
    }

    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    public boolean supportsEmptyInList() {
        return false;
    }

    public boolean supportsExistsInSelect() {
        return false;
    }

    public int getInExpressionCountLimit() {
        return 1000;
    }

    public boolean forceLobAsLastValue() {
        return true;
    }

    public boolean useFollowOnLocking(QueryParameters parameters) {
        if (parameters != null) {
            String lowerCaseSQL = parameters.getFilteredSQL().toLowerCase();
            if ((!DISTINCT_KEYWORD_PATTERN.matcher(lowerCaseSQL).find()) &&
                    (!GROUP_BY_KEYWORD_PATTERN.matcher(lowerCaseSQL).find()) &&
                    (!UNION_KEYWORD_PATTERN.matcher(lowerCaseSQL).find())) {
                if (!parameters.hasRowSelection()) {
                    return (ORDER_BY_KEYWORD_PATTERN.matcher(lowerCaseSQL).find()) ||
                                    (parameters.getRowSelection().getFirstRow() != null);
                }
            }

        }
        return true;
    }

    public String getNotExpression(String expression) {
        return "not (" + expression + ")";
    }

    public String getNativeIdentifierGeneratorStrategy() {
        return "sequence";
    }

    public IdentityColumnSupport getIdentityColumnSupport() {
        return new TiberoIdentityColumnSupport();
    }

    public String getQueryHintString(String sql, String hints) {
        String statementType = statementType(sql);

        int pos = sql.indexOf(statementType);
        if (pos > -1) {
            StringBuilder buffer = new StringBuilder(sql.length() + hints.length() + 8);
            if (pos > 0) {
                buffer.append(sql.substring(0, pos));
            }
            buffer.append(statementType).append(" /*+ ").append(hints).append(" */").append(sql.substring(pos + statementType.length()));
            sql = buffer.toString();
        }
        return sql;
    }

    public int getMaxAliasLength() {
        return 20;
    }

    public CallableStatementSupport getCallableStatementSupport() {
        return StandardCallableStatementSupport.REF_CURSOR_INSTANCE;
    }

    public boolean canCreateSchema() {
        return false;
    }

    public String getCurrentSchemaCommand() {
        return "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
    }

    public boolean supportsPartitionBy() {
        return true;
    }

    protected String statementType(String sql) {
        Matcher matcher = SQL_STATEMENT_TYPE_PATTERN.matcher(sql);
        if ((matcher.matches()) && (matcher.groupCount() == 1)) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Can't determine SQL statement type for statement: " + sql);
    }

    public boolean supportsNoWait() {
        return true;
    }

    public boolean supportsRowValueConstructorSyntaxInInList() {
        return true;
    }

    public boolean supportsTupleDistinctCounts() {
        return false;
    }

    public boolean supportsSkipLocked() {
        return true;
    }


}
