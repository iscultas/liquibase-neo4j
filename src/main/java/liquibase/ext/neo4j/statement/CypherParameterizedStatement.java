package liquibase.ext.neo4j.statement;

import liquibase.database.PreparedStatementFactory;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/*
 * Prepared Cypher statement.
 *
 * This assumes parameters are not named but indexed.
 *
 * The following query works:
 * ```cypher
 * MATCH (p:Person {firstName: $1, lastName: $2}) RETURN p
 * ```
 *
 * The following query does NOT work:
 * ```cypher
 * MATCH (p:Person {firstName: $firstName, lastName: $lastName}) RETURN p
 * ```
 */
public class CypherParameterizedStatement implements ExecutablePreparedStatement {

    private final String cypher;
    private final List<Object> parameterValues;

    public CypherParameterizedStatement(String cypher, List<Object> parameterValues) {
        this.cypher = cypher;
        this.parameterValues = parameterValues;
    }

    @Override
    public void execute(PreparedStatementFactory factory) throws DatabaseException {
        try {
            PreparedStatement preparedStatement = factory.create(this.cypher);
            attachParameters(preparedStatement);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean skipOnUnsupported() {
        return false;
    }

    @Override
    public boolean continueOnError() {
        return false;
    }

    private void attachParameters(PreparedStatement preparedStatement) throws SQLException {
        for (int i = 0; i < parameterValues.size(); i++) {
            preparedStatement.setObject(i + 1, parameterValues.get(i));
        }
    }
}
