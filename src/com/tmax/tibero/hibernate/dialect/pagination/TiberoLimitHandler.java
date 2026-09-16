package com.tmax.tibero.hibernate.dialect.pagination;

import java.util.Locale;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryOptions;

/**
 * 페이징(`setFirstResult` / `setMaxResults`)을 SQL 로 옮긴다.
 *
 * <p>⚠️ <b>rownum 분기를 잉여로 보고 걷어내면 안 된다.</b> {@code TiberoSqlAstTranslator} 의
 * locking wrapper 는 HQL/SQM 경로만 덮는다. 네이티브 질의는 이 클래스가 유일한 방어선이고,
 * 무상태 offset/fetch 구현으로 바꾸면 아래가 나가 깨진다(실측).
 *
 * <pre>
 * select id from T order by id for update fetch first 2 rows only   FAIL  JDBC-8022
 * </pre>
 */
public class TiberoLimitHandler extends AbstractLimitHandler {

    private boolean bindLimitParametersInReverseOrder;
    private boolean useMaxForLimit;
    private boolean supportOffset;

    /**
     * @deprecated 공유 싱글턴은 쓰지 말 것. 이 클래스는 {@code processSql} 에서 위 세 플래그를
     *             매번 덮어쓰고, 호출자({@code DeferredResultSetAccess})는 SQL 을 만든 뒤
     *             <b>나중에</b> 바인딩 시점에 그 값을 읽는다. 그 사이 다른 스레드가 같은
     *             인스턴스에 {@code processSql} 을 부르면 플래그가 뒤집혀 예외 없이 다른
     *             페이지가 나온다. {@code TiberoDialect.getLimitHandler()} 는 호출마다
     *             새 인스턴스를 돌려준다.
     */
    @Deprecated
    public static final TiberoLimitHandler INSTANCE = new TiberoLimitHandler();

    public TiberoLimitHandler() {
    }

    @Override
    public String processSql(String sql, Limit limit, QueryOptions queryOptions) {
        final boolean hasFirstRow = hasFirstRow( limit );
        final boolean hasMaxRows = hasMaxRows( limit );

        if ( !hasFirstRow && !hasMaxRows ) {
            return sql;
        }

        return processSql(
                sql,
                hasFirstRow,
                hasMaxRows,
                queryOptions.getLockOptions()
        );
    }

    /**
     * @deprecated Use {@link #processSql(String, boolean, boolean, LockOptions)} instead
     */
    @Deprecated(forRemoval = true)
    protected String processSql(String sql, boolean hasFirstRow, LockOptions lockOptions) {
        return processSql( sql, hasFirstRow, true, lockOptions );
    }

    protected String processSql(String sql, boolean hasFirstRow, boolean hasMaxRows, LockOptions lockOptions) {
        if ( lockOptions != null ) {
            final LockMode lockMode = lockOptions.getLockMode();
            switch ( lockMode ) {
                case PESSIMISTIC_READ:
                case PESSIMISTIC_WRITE:
                case UPGRADE_NOWAIT:
                case PESSIMISTIC_FORCE_INCREMENT:
                case UPGRADE_SKIPLOCKED: {
                    return processSql( sql, getForUpdateIndex( sql ), hasFirstRow, hasMaxRows );
                }
                default: {
                    return processSqlOffsetFetch( sql, hasFirstRow, hasMaxRows );
                }
            }
        }
        return processSqlOffsetFetch( sql, hasFirstRow, hasMaxRows );
    }

    /**
     * @deprecated Use {@link #processSqlOffsetFetch(String, boolean, boolean)} instead
     */
    @Deprecated(forRemoval = true)
    protected String processSqlOffsetFetch(String sql, boolean hasFirstRow) {
        return processSqlOffsetFetch( sql, hasFirstRow, true );
    }

    protected String processSqlOffsetFetch(String sql, boolean hasFirstRow, boolean hasMaxRows) {

        final int forUpdateLastIndex = getForUpdateIndex( sql );

        if ( forUpdateLastIndex > -1 ) {
            return processSql( sql, forUpdateLastIndex, hasFirstRow, hasMaxRows );
        }

        bindLimitParametersInReverseOrder = false;
        useMaxForLimit = false;
        supportOffset = true;

        final String offsetFetchString;
        if ( hasFirstRow && hasMaxRows ) {
            offsetFetchString = " offset ? rows fetch next ? rows only";
        }
        else if ( hasFirstRow ) {
            offsetFetchString = " offset ? rows";
        }
        else {
            offsetFetchString = " fetch first ? rows only";
        }

        return insertAtEnd(offsetFetchString, sql);
    }

    /**
     * @deprecated Use {@link #processSql(String, int, boolean, boolean)} instead
     */
    @Deprecated(forRemoval = true)
    protected String processSql(String sql, int forUpdateIndex, boolean hasFirstRow) {
        return processSql( sql, forUpdateIndex, hasFirstRow, true );
    }

    protected String processSql(String sql, int forUpdateIndex, boolean hasFirstRow, boolean hasMaxRows) {
        bindLimitParametersInReverseOrder = true;
        useMaxForLimit = true;
        supportOffset = false;

        String forUpdateClause = null;
        boolean isForUpdate = false;
        if ( forUpdateIndex > -1 ) {
            // save 'for update ...' and then remove it
            forUpdateClause = sql.substring( forUpdateIndex );
            sql = sql.substring( 0, forUpdateIndex - 1 );
            isForUpdate = true;
        }

        final StringBuilder pagingSelect;

        final int forUpdateClauseLength;
        if ( forUpdateClause == null ) {
            forUpdateClauseLength = 0;
        }
        else {
            forUpdateClauseLength = forUpdateClause.length() + 1;
        }

        if ( hasFirstRow && hasMaxRows ) {
            pagingSelect = new StringBuilder( sql.length() + forUpdateClauseLength + 98 );
            pagingSelect.append( "select * from (select row_.*,rownum rownum_ from (" );
            pagingSelect.append( sql );
            pagingSelect.append( ") row_ where rownum<=?) where rownum_>?" );
        }
        else if ( hasFirstRow ) {
            pagingSelect = new StringBuilder( sql.length() + forUpdateClauseLength + 98 );
            pagingSelect.append( "select * from (" );
            pagingSelect.append( sql );
            pagingSelect.append( ") row_ where rownum>?" );
        }
        else {
            pagingSelect = new StringBuilder( sql.length() + forUpdateClauseLength + 37 );
            pagingSelect.append( "select * from (" );
            pagingSelect.append( sql );
            pagingSelect.append( ") where rownum<=?" );
        }

        if ( isForUpdate ) {
            pagingSelect.append( " " );
            pagingSelect.append( forUpdateClause );
        }

        return pagingSelect.toString();
    }

    private int getForUpdateIndex(String sql) {
        final int forUpdateLastIndex = sql.toLowerCase( Locale.ROOT ).lastIndexOf( "for update" );
        // We need to recognize cases like : select a from t where b = 'for update';
        final int lastIndexOfQuote = sql.lastIndexOf( '\'' );
        if ( forUpdateLastIndex > -1 ) {
            if ( lastIndexOfQuote == -1 ) {
                return forUpdateLastIndex;
            }
            if ( lastIndexOfQuote > forUpdateLastIndex ) {
                return -1;
            }
            return forUpdateLastIndex;
        }
        return forUpdateLastIndex;
    }

    @Override
    public final boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsOffset() {
        return supportOffset;
    }

    @Override
    public boolean bindLimitParametersInReverseOrder() {
        return bindLimitParametersInReverseOrder;
    }

    @Override
    public boolean useMaxForLimit() {
        return useMaxForLimit;
    }
}
