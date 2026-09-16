package com.tmax.tibero.hibernate.dialect;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.dialect.SqlAstTranslatorWithUpsert;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EmbeddableValuedModelPart;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.sqm.FetchClauseType;
import org.hibernate.query.sqm.FrameExclusion;
import org.hibernate.query.sqm.FrameKind;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SortSpecification;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.FunctionExpression;
import org.hibernate.sql.ast.tree.expression.Over;
import org.hibernate.sql.ast.tree.expression.SqlTuple;
import org.hibernate.sql.ast.tree.expression.SqlTupleContainer;
import org.hibernate.sql.ast.tree.from.FromClause;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.UnionTableGroup;
import org.hibernate.sql.ast.tree.insert.ConflictClause;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.ast.ColumnValueBinding;
import org.hibernate.sql.results.internal.SqlSelectionImpl;
import org.hibernate.metamodel.mapping.JdbcMappingContainer;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.tree.expression.BinaryArithmeticExpression;
import org.hibernate.sql.ast.tree.expression.Literal;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.from.ValuesTableReference;
import org.hibernate.sql.ast.tree.select.QueryGroup;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.from.QueryPartTableReference;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;

/**
 * Tibero SqlAstTranslator (Hibernate 6.6).
 * - recursive CTE: WITH only
 * - empty row_number() OVER 보정
 * - INSERT conflict / optional table update → MERGE
 * - UPDATE with non-trivial FROM → inline view 에뮬레이션
 * - 정수 나눗셈 floor 보정 · LOB 비교 · UNION 가지의 order by · VALUES 테이블 참조 ·
 *   partition by 리터럴 (§5.3 실측으로 확인된 5건)
 */
public class  TiberoSqlAstTranslator<T extends JdbcOperation> extends SqlAstTranslatorWithUpsert<T> {



    public TiberoSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
        super(sessionFactory, statement);
    }

    @Override
    protected void visitInsertStatementOnly(InsertSelectStatement statement) {
        if (statement.getConflictClause() == null || statement.getConflictClause().isDoNothing()) {
            super.visitInsertStatementOnly(statement);
        }
        else {
            visitInsertStatementEmulateMerge(statement);
        }
    }

    @Override
    protected void visitUpdateStatementOnly(UpdateStatement statement) {
        if (hasNonTrivialFromClause(statement.getFromClause())) {
            visitUpdateStatementEmulateInlineView(statement);
        }
        else {
            renderUpdateClause(statement);
            renderSetClause(statement.getAssignments());
            visitWhereClause(statement.getRestriction());
            visitReturningColumns(statement.getReturningColumns());
        }
    }

    @Override
    protected void renderMergeUpdateClause(List<Assignment> assignments, Predicate wherePredicate) {
        appendSql(" then update");
        renderSetClause(assignments);
        visitWhereClause(wherePredicate);
    }

    @Override
    protected void renderDmlTargetTableExpression(NamedTableReference tableReference) {
        super.renderDmlTargetTableExpression(tableReference);
        if (getClauseStack().getCurrent() != Clause.INSERT) {
            renderTableReferenceIdentificationVariable(tableReference);
        }
    }

    @Override
    protected void visitConflictClause(ConflictClause conflictClause) {
        if (conflictClause != null) {
            if (conflictClause.isDoUpdate() && conflictClause.getConstraintName() != null) {
                throw new IllegalQueryOperationException(
                        "Insert conflict 'do update' clause with constraint name is not supported");
            }
        }
    }

    @Override
    protected boolean needsRecursiveKeywordInWithClause() {
        return false;
    }

    @Override
    protected boolean supportsWithClauseInSubquery() {
        return false;
    }

    @Override
    protected boolean supportsRecursiveSearchClause() {
        return true;
    }

    @Override
    protected boolean supportsRecursiveCycleClause() {
        return true;
    }

    /**
     * Tibero는 OFFSET/FETCH 와 FOR UPDATE 를 같은 쿼리 블록에 함께 쓸 수 없다.
     * {@code … offset ? rows fetch first ? rows only for update} 는 JDBC-8004 로 거부된다.
     *
     * <p>다만 페이징을 서브쿼리로 밀고 {@code for update} 를 바깥에 붙이면 받아들이므로,
     * 가능한 모양이면 {@link #visitQuerySpec} 의 locking wrapper 로 한 문장에 처리한다.
     * 래퍼를 적용할 수 없는 모양일 때만 follow-on locking 으로 넘긴다.
     *
     * <p>{@code TiberoDialect.useFollowOnLocking()} 이 비슷한 판단을 갖고 있지만
     * Hibernate 6 의 SQM 경로는 그 훅을 보지 않고 이 메서드의 결과를 쓴다.
     * 기본 구현에는 GROUP BY / HAVING / DISTINCT 규칙만 있고 OFFSET/FETCH 규칙이 없어
     * 여기서 보충한다.
     */
    @Override
    protected LockStrategy determineLockingStrategy(
            QuerySpec querySpec,
            ForUpdateClause forUpdateClause,
            Boolean followOnLocking) {
        LockStrategy strategy = super.determineLockingStrategy(querySpec, forUpdateClause, followOnLocking);
        if (strategy != LockStrategy.FOLLOW_ON
                && needsLockingWrapper(querySpec)
                && !canApplyLockingWrapper(querySpec)) {
            if (Boolean.FALSE.equals(followOnLocking)) {
                throw new IllegalQueryOperationException("Locking with OFFSET/FETCH is not supported");
            }
            strategy = LockStrategy.FOLLOW_ON;
        }
        return strategy;
    }

    /**
     * 페이징 + 비관적 락이면 locking wrapper 로 감싼다.
     *
     * <pre>
     * select … from T t
     * where t.id in (select id from T order by … offset ? rows fetch first ? rows only)
     * for update
     * </pre>
     *
     * <p>조회와 잠금이 한 문장에서 끝나므로 follow-on locking 의 1+N 왕복이 사라지고,
     * "락 없이 먼저 읽고 나중에 잠그는" 사이의 갱신 갭도 없어진다.
     * 적용할 수 없는 모양은 {@link #identifierMappingForLockingWrapper} 가 걸러내고
     * {@link #determineLockingStrategy} 가 follow-on 으로 폴백시킨다.
     */
    @Override
    public void visitQuerySpec(QuerySpec querySpec) {
        final EntityIdentifierMapping identifierMapping = identifierMappingForLockingWrapper(querySpec);
        if (identifierMapping == null) {
            super.visitQuerySpec(querySpec);
            return;
        }
        final Expression offsetExpression;
        final Expression fetchExpression;
        final FetchClauseType fetchClauseType;
        if (querySpec.isRoot() && hasLimit()) {
            prepareLimitOffsetParameters();
            offsetExpression = getOffsetParameter();
            fetchExpression = getLimitParameter();
            fetchClauseType = FetchClauseType.ROWS_ONLY;
        }
        else {
            offsetExpression = querySpec.getOffsetClauseExpression();
            fetchExpression = querySpec.getFetchClauseExpression();
            fetchClauseType = querySpec.getFetchClauseType();
        }
        super.visitQuerySpec(createLockingWrapper(
                querySpec, offsetExpression, fetchExpression, fetchClauseType, identifierMapping));
        // 래퍼는 non-root 로 만들었으므로 for update 는 원본 querySpec 기준으로 직접 렌더한다.
        visitForUpdateClause(querySpec);
    }

    private QuerySpec createLockingWrapper(
            QuerySpec querySpec,
            Expression offsetExpression,
            Expression fetchExpression,
            FetchClauseType fetchClauseType,
            EntityIdentifierMapping identifierMapping) {

        final TableGroup rootTableGroup = querySpec.getFromClause().getRoots().get(0);
        final List<ColumnReference> idColumnReferences = new ArrayList<>(identifierMapping.getJdbcTypeCount());
        identifierMapping.forEachSelectable(
                0,
                (selectionIndex, selectableMapping) -> idColumnReferences.add(
                        new ColumnReference(rootTableGroup.getPrimaryTableReference(), selectableMapping))
        );
        final Expression idExpression = identifierMapping instanceof EmbeddableValuedModelPart
                ? new SqlTuple(idColumnReferences, identifierMapping)
                : idColumnReferences.get(0);

        // 페이징은 서브쿼리 쪽으로 옮긴다.
        final QuerySpec subquery = new QuerySpec(false, 1);
        for (ColumnReference idColumnReference : idColumnReferences) {
            subquery.getSelectClause().addSqlSelection(new SqlSelectionImpl(idColumnReference));
        }
        subquery.getFromClause().addRoot(rootTableGroup);
        subquery.applyPredicate(querySpec.getWhereClauseRestrictions());
        if (querySpec.hasSortSpecifications()) {
            for (SortSpecification sortSpecification : querySpec.getSortSpecifications()) {
                subquery.addSortSpecification(sortSpecification);
            }
        }
        subquery.setOffsetClauseExpression(offsetExpression);
        subquery.setFetchClauseExpression(fetchExpression, fetchClauseType);

        // 래퍼는 root 가 아닌 것으로 만들어 여기에 페이징이 다시 붙지 않게 한다.
        final QuerySpec lockingWrapper = new QuerySpec(false, 1);
        lockingWrapper.getFromClause().addRoot(rootTableGroup);
        for (SqlSelection sqlSelection : querySpec.getSelectClause().getSqlSelections()) {
            lockingWrapper.getSelectClause().addSqlSelection(sqlSelection);
        }
        lockingWrapper.applyPredicate(new InSubQueryPredicate(idExpression, subquery, false));

        if (querySpec.hasSortSpecifications()) {
            for (SortSpecification sortSpecification : querySpec.getSortSpecifications()) {
                lockingWrapper.addSortSpecification(sortSpecification);
            }
        }
        return lockingWrapper;
    }

    @Override
    public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
        emulateQueryPartTableReferenceColumnAliasing(tableReference);
    }

    /**
     * 래퍼를 씌워야 하고 씌울 수 있는 경우에만 식별자 매핑을 돌려준다. 아니면 {@code null}.
     */
    private EntityIdentifierMapping identifierMappingForLockingWrapper(QuerySpec querySpec) {
        if (canApplyLockingWrapper(querySpec)
                // 이 쿼리에 실제로 락이 필요해야 한다.
                && needsLocking(querySpec)
                // 페이징 때문에 래퍼가 필요한 상황이어야 한다.
                && needsLockingWrapper(querySpec)
                // GROUP BY / HAVING / DISTINCT / 집계가 있으면 어차피 follow-on 으로 간다.
                && querySpec.getGroupByClauseExpressions().isEmpty()
                && querySpec.getHavingClauseRestrictions() == null
                && !querySpec.getSelectClause().isDistinct()
                && !hasAggregateFunctions(querySpec)) {
            return ((EntityMappingType) querySpec.getFromClause().getRoots().get(0).getModelPart())
                    .getIdentifierMapping();
        }
        return null;
    }

    /**
     * 아주 단순한 쿼리에만 래퍼를 씌운다 — 조인 없는 단일 엔티티 루트여야 한다.
     * 집합 연산이 섞이면 서브쿼리로 분리한 id 목록이 원본과 대응되지 않으므로 제외한다.
     */
    private boolean canApplyLockingWrapper(QuerySpec querySpec) {
        final FromClause fromClause;
        return querySpec.isRoot()
                && !hasSetOperations(querySpec)
                // Tibero 가 서브쿼리에서 네이티브로 렌더할 수 있는 것은 ROWS ONLY 뿐이다.
                // WITH TIES / PERCENT 는 supportsFetchClause() 가 false 이므로 follow-on 으로 보낸다.
                && querySpec.getFetchClauseType() == FetchClauseType.ROWS_ONLY
                && (fromClause = querySpec.getFromClause()).getRoots().size() == 1
                && !fromClause.hasJoins()
                && fromClause.getRoots().get(0).getModelPart() instanceof EntityMappingType;
    }

    /** 페이징이 걸려 있어 {@code for update} 를 그대로 붙일 수 없는 상황인가. */
    private boolean needsLockingWrapper(QuerySpec querySpec) {
        return querySpec.getFetchClauseType() != FetchClauseType.ROWS_ONLY
                || hasOffset(querySpec)
                || hasLimit(querySpec);
    }

    private boolean hasSetOperations(QuerySpec querySpec) {
        return querySpec.getFromClause()
                .queryTableGroups(group -> group instanceof UnionTableGroup ? group : null) != null;
    }

    @Override
    public void visitOver(Over<?> over) {
        final Expression expression = over.getExpression();
        if (expression instanceof FunctionExpression
                && "row_number".equals(((FunctionExpression) expression).getFunctionName())) {
            if (over.getPartitions().isEmpty() && over.getOrderList().isEmpty()
                    && over.getStartKind() == FrameKind.UNBOUNDED_PRECEDING
                    && over.getEndKind() == FrameKind.CURRENT_ROW
                    && over.getExclusion() == FrameExclusion.NO_OTHERS) {
                append("row_number() over(order by 1)");
                return;
            }
        }
        super.visitOver(over);
    }

    @Override
    protected void renderMergeSource(OptionalTableUpdate optionalTableUpdate) {
        final List<ColumnValueBinding> valueBindings = optionalTableUpdate.getValueBindings();
        final List<ColumnValueBinding> keyBindings = optionalTableUpdate.getKeyBindings();

        appendSql("(select ");

        for (int i = 0; i < keyBindings.size(); i++) {
            final ColumnValueBinding keyBinding = keyBindings.get(i);
            if (i > 0) {
                appendSql(", ");
            }
            renderCasted(keyBinding.getValueExpression());
            appendSql(" ");
            appendSql(keyBinding.getColumnReference().getColumnExpression());
        }
        for (ColumnValueBinding valueBinding : valueBindings) {
            appendSql(", ");
            renderCasted(valueBinding.getValueExpression());
            appendSql(" ");
            appendSql(valueBinding.getColumnReference().getColumnExpression());
        }

        appendSql(getFromDualForSelectOnly());
        appendSql(")");

        renderMergeSourceAlias();
    }

    /**
     * UPDATE 의 SET 왼쪽을 렌더할 때 <b>테이블 별칭을 함께</b> 낸다.
     *
     * <p>Hibernate 기본 구현({@code AbstractSqlAstTranslator.appendAssignmentColumn})은
     * {@code appendColumnForWrite(this, null)} 로 <b>한정자를 버리고</b> 컬럼명만 낸다.
     * 보통 컬럼이라면 그래도 되지만, {@code @Struct} 집계 컬럼의 필드를 갱신할 때는
     * Tibero 가 별칭 없는 점 표기를 식별자로 해석하지 못한다.
     *
     * <pre>
     * update PERSON p1_0 set addr.city='Busan'        FAIL  JDBC-8026 Invalid identifier
     * update PERSON p1_0 set p1_0.addr.city='Busan'   OK
     * </pre>
     *
     * <p>Hibernate {@code OracleSqlAstTranslator} 도 <b>같은 이유로 이 메서드를 재정의</b>하며,
     * 인자 없는 {@code appendColumnForWrite(this)} 오버로드를 써서 컬럼이 자기 한정자를
     * 쓰도록 한다. 본문은 기본 구현과 그 한 줄만 다르다.
     *
     * <p>집계 컬럼이 없는 보통 UPDATE 에도 별칭이 붙지만 의미는 같다 —
     * {@code update T t set t.c=?} 는 Tibero 가 정상 수용한다.
     *
     * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport#aggregateComponentAssignmentExpression
     *      {@code addr.city} 까지를 만드는 쪽. 앞의 별칭은 여기서 붙는다
     */
    @Override
    protected void visitSetAssignment(Assignment assignment) {
        final List<ColumnReference> columnReferences = assignment.getAssignable().getColumnReferences();
        if (columnReferences.size() == 1) {
            columnReferences.get(0).appendColumnForWrite(this);
            appendSql('=');
            final Expression assignedValue = assignment.getAssignedValue();
            final SqlTuple sqlTuple = SqlTupleContainer.getSqlTuple(assignedValue);
            if (sqlTuple != null) {
                assert sqlTuple.getExpressions().size() == 1;
                sqlTuple.getExpressions().get(0).accept(this);
            }
            else {
                assignedValue.accept(this);
            }
        }
        else {
            char separator = OPEN_PARENTHESIS;
            for (ColumnReference columnReference : columnReferences) {
                appendSql(separator);
                columnReference.appendColumnForWrite(this);
                separator = COMMA_SEPARATOR_CHAR;
            }
            appendSql(")=");
            assignment.getAssignedValue().accept(this);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §5.3 실측으로 확인된 5건.
    //
    // 아래 다섯은 Oracle 번역기도 재정의하는 자리다. 처음에는 "Oracle 이 재정의하니
    // 우리도 필요할 것"이라는 추정만 있었고, 문법 수용 여부만 보고 불필요로 판단했었다.
    // 그런데 Oracle 이 이 메서드들을 재정의하는 목적은 문법이 아니라 타입 의미였다.
    // 목적에 해당하는 상황을 재현해 ps06 에 실측한 결과 다섯 건 모두 Tibero 에서도
    // 깨지는 것이 확인됐다. 각 메서드 주석에 재현 SQL 과 오류 코드를 남긴다.
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * 정수 나눗셈을 {@code floor} 로 감싼다.
     *
     * <p>Tibero 는 Oracle 과 같이 {@code 5/2} 를 <b>2.5</b> 로 돌려준다(실측).
     * HQL 에서 양쪽이 정수 타입이면 결과도 정수여야 하므로 그대로 두면
     * <b>예외 없이 조용히 틀린 값</b>이 쌓인다 — 이번 6.6 작업에서 찾은 결함 중
     * 성격이 가장 나쁜 유형이다.
     *
     * <pre>
     * select 5/2 from dual         → 2.5   (그대로 두면)
     * select floor(5/2) from dual  → 2     (보정 후)
     * </pre>
     *
     * <p>{@code isIntegerDivisionEmulationRequired} 가 양쪽 피연산자의 JDBC 타입이
     * 모두 정수인 {@code DIVIDE_PORTABLE} 연산만 골라내므로, 실수 나눗셈은 영향받지 않는다.
     * Oracle 번역기와 같은 형태다.
     */
    @Override
    public void visitBinaryArithmeticExpression(BinaryArithmeticExpression arithmeticExpression) {
        if (isIntegerDivisionEmulationRequired(arithmeticExpression)) {
            appendSql("floor");
        }
        super.visitBinaryArithmeticExpression(arithmeticExpression);
    }

    /**
     * LOB 컬럼 비교를 {@code dbms_lob.compare} 로 바꾼다.
     *
     * <p>Tibero 는 CLOB · NCLOB · BLOB 을 {@code =} 나 {@code <} 로 비교할 수 없다.
     *
     * <pre>
     * where cl = 'hello'                        → JDBC-11023 Values are from data types
     *                                             that cannot be compared.
     * where 0 = dbms_lob.compare(cl, 'hello')   → OK   (실측)
     * </pre>
     *
     * <p>{@code dbms_lob.compare} 는 <b>-1 / 0 / +1</b> 을 돌려준다(실측). 그래서 원래
     * 비교식 {@code lhs <op> rhs} 를 <b>{@code 0 <역방향op> compare(lhs, rhs)}</b> 로
     * 바꾸면 모든 비교 연산자를 그대로 표현할 수 있다.
     *
     * <pre>
     * lhs =  rhs   →   0 =  compare(lhs, rhs)
     * lhs &lt;&gt; rhs   →   0 &lt;&gt; compare(lhs, rhs)
     * lhs &lt;  rhs   →   0 &gt;  compare(lhs, rhs)     (compare 가 음수)
     * lhs &gt;  rhs   →   0 &lt;  compare(lhs, rhs)     (compare 가 양수)
     * </pre>
     *
     * <p><b>Oracle 번역기와 일부러 다른 지점</b> — Oracle 은 {@code NOT_EQUAL} 을
     * {@code -1=} 로 낸다. 그런데 앞쪽이 <i>더 큰</i> 경우 {@code compare} 가 {@code +1}
     * 을 돌려주므로 그 행을 놓친다. 실제로 {@code 'world' <> 'hello'} 가 0건으로 나오는
     * 것을 실측으로 확인했다. {@code 0<>} 로 바꾸면 양쪽 모두 잡힌다.
     *
     * <p><b>다루지 않는 것</b>
     * <ul>
     *   <li>{@code DISTINCT_FROM} / {@code NOT_DISTINCT_FROM} — {@code compare} 는 한쪽이
     *       null 이면 null 을 돌려줘서 널 안전 비교를 이 관용구만으로는 만들 수 없다.
     *       기본 구현에 맡긴다.</li>
     *   <li><b>SQLXML</b> — Oracle 은 {@code existsnode(xmldiff(…))} 로 우회하지만
     *       Tibero 에는 두 함수가 없다({@code JDBC-8036} 실측).</li>
     *   <li><b>ARRAY</b> — Oracle 은 exporter 가 만든 {@code <타입>_cmp} PL/SQL 을
     *       호출하는데, 그 헬퍼 계열이 Tibero 서버를 멈추게 해 우리는 생성하지 않는다.</li>
     * </ul>
     * 뒤의 둘은 우리가 만들 수 있는 우회가 없어 남겨둔 한계이며 문서에 기록돼 있다.
     */
    @Override
    protected void renderComparison(Expression lhs, ComparisonOperator operator, Expression rhs) {
        final JdbcMappingContainer lhsExpressionType = lhs.getExpressionType();
        if (lhsExpressionType == null || lhsExpressionType.getJdbcTypeCount() != 1) {
            super.renderComparison(lhs, operator, rhs);
            return;
        }
        switch (lhsExpressionType.getSingleJdbcMapping().getJdbcType().getDdlTypeCode()) {
            case SqlTypes.CLOB:
            case SqlTypes.NCLOB:
            case SqlTypes.BLOB:
                final String zeroSideOperator = lobCompareOperator(operator);
                if (zeroSideOperator == null) {
                    super.renderComparison(lhs, operator, rhs);
                    return;
                }
                appendSql('0');
                appendSql(zeroSideOperator);
                appendSql("dbms_lob.compare(");
                lhs.accept(this);
                appendSql(',');
                rhs.accept(this);
                appendSql(')');
                break;
            default:
                super.renderComparison(lhs, operator, rhs);
        }
    }

    /**
     * {@code 0 <?> dbms_lob.compare(lhs, rhs)} 의 {@code <?>} 자리에 올 연산자.
     *
     * <p>비교식의 좌우가 뒤집히므로 부등호도 함께 뒤집힌다. 널 안전 비교
     * ({@code DISTINCT_FROM} 계열)는 이 관용구로 표현할 수 없어 {@code null} 을 돌려준다.
     */
    private static String lobCompareOperator(ComparisonOperator operator) {
        switch (operator) {
            case EQUAL:                    return "=";
            case NOT_EQUAL:                return "<>";
            case LESS_THAN:                return ">";
            case LESS_THAN_OR_EQUAL:       return ">=";
            case GREATER_THAN:             return "<";
            case GREATER_THAN_OR_EQUAL:    return "<=";
            default:                       return null;
        }
    }

    /**
     * UNION 가지 안에 {@code order by} 만 있을 때 {@code offset 0 rows} 를 끼운다.
     *
     * <p>집합 연산의 각 가지에 정렬만 붙으면 Tibero 가 문장을 거부한다.
     *
     * <pre>
     * (select a from T order by a) union all (select a from U)
     *   → JDBC-8013 Missing SELECT keyword.
     *
     * (select a from T order by a offset 0 rows) union all (select a from U)
     *   → OK   (실측)
     * </pre>
     *
     * <p>{@code offset 0 rows} 는 행을 하나도 건너뛰지 않으므로 결과가 달라지지 않는다.
     * 파서에게 "이 정렬은 이 가지에 속한다"고 알려 주는 역할만 한다. Oracle 도 같은 이유로
     * 같은 보정을 넣는다.
     *
     * <p>보정 대상은 <b>부모가 QueryGroup 이고, 정렬은 있는데 offset/fetch 는 없는</b>
     * 가지로 한정한다. 루트 쿼리이면서 {@code setMaxResults} 가 걸린 경우는 바깥에서
     * 페이징이 렌더되므로 제외한다.
     */
    @Override
    public void visitOffsetFetchClause(QueryPart queryPart) {
        if (isRowNumberingCurrentQueryPart()) {
            return;
        }
        if (!getDialect().supportsFetchClause(FetchClauseType.ROWS_ONLY)) {
            assertRowsOnlyFetchClauseType(queryPart);
            return;
        }
        if (getQueryPartStack().depth() > 1
                && queryPart.hasSortSpecifications()
                && getQueryPartStack().peek(1) instanceof QueryGroup
                && (queryPart.isRoot() && !hasLimit() || !queryPart.hasOffsetOrFetchClause())) {
            appendSql(" offset 0 rows");
        }
        else {
            renderOffsetFetchClause(queryPart, true);
        }
    }

    /**
     * {@code (values …)} 테이블 참조를 {@code select … from dual union all …} 로 편다.
     *
     * <p>Tibero 는 VALUES 를 테이블처럼 참조하지 못한다.
     *
     * <pre>
     * select * from (values (1,2),(3,4))   → JDBC-8013 Missing SELECT keyword.
     * merge into T t using (values (1)) s  → JDBC-8013
     * </pre>
     *
     * <p>흔히 오해하는 지점 — <b>INSERT 의 다중 VALUES 는 Tibero 가 그대로 받는다</b>
     * ({@code insert into T values (1),(2)} 실측 OK). 못 받는 것은 <b>테이블 참조로 쓰는
     * VALUES</b> 뿐이라 {@code visitValuesList} 는 손대지 않고 이쪽만 재정의한다.
     *
     * <p>기본 구현 {@code emulateValuesTableReferenceColumnAliasing} 이 각 행을
     * {@code select … from dual} 로 바꾸고 {@code union all} 로 잇는다. Oracle 도
     * 23c 미만에서 같은 함수를 쓴다.
     *
     * <p>MERGE 의 {@code using} 절은 이미 {@code renderMergeSource} 가 직접 렌더하고
     * 있어 이 경로를 타지 않는다. 남은 노출 범위는 그 밖의 자리다.
     */
    @Override
    public void visitValuesTableReference(ValuesTableReference tableReference) {
        emulateValuesTableReferenceColumnAliasing(tableReference);
    }

    /**
     * {@code partition by} 에 리터럴이 오면 빈 괄호 대신 리터럴을 그대로 낸다.
     *
     * <p>Hibernate 기본 구현(과 Oracle 재정의)은 파티션 식이 리터럴이면 {@code ()} 를
     * 내보낸다. Oracle 은 그 형태를 받지만 Tibero 는 받지 않는다.
     *
     * <pre>
     * over (partition by ())   → JDBC-8013 Missing SELECT keyword.
     * over (partition by 1)    → OK   (실측)
     * </pre>
     *
     * <p>그래서 리터럴 분기만 빼고 나머지는 기본 구현과 같게 둔다.
     * {@code rollup}/{@code cube} 같은 {@link Summarization} 은 기본 구현과 동일하게 처리한다.
     */
    @Override
    protected void renderPartitionItem(Expression expression) {
        if (expression instanceof Literal) {
            // 기본 구현은 여기서 "()" 를 내는데 Tibero 가 거부한다
            expression.accept(this);
        }
        else if (expression instanceof Summarization) {
            final Summarization summarization = (Summarization) expression;
            appendSql(summarization.getKind().sqlText());
            appendSql(OPEN_PARENTHESIS);
            renderCommaSeparated(summarization.getGroupings());
            appendSql(CLOSE_PARENTHESIS);
        }
        else {
            expression.accept(this);
        }
    }
}
