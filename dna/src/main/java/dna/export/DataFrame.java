package dna.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A flexible tabular data structure similar to R's DataFrame or Python's Pandas DataFrame.
 * Supports mixed-type columns, dynamic row and column manipulation, and safe metadata handling.
 * It is based on a row-major storage format, where each row is an ArrayList of Objects.
 * Columns are identified by unique variable names, and data types can be specified for each column.
 * This class provides methods for adding, deleting, and retrieving rows and columns, as well as converting
 * the DataFrame to a 2D Object array format.
 */
public class DataFrame {
    private ArrayList<ArrayList<Object>> data;
    private ArrayList<String> variableNames;
    private ArrayList<String> dataTypes;
    private Map<String, Integer> columnIndexMap;

    /**
     * Constructs a DataFrame from a list of rows and associated metadata.
     *
     * @param data           The table data as a list of rows.
     * @param variableNames  The names of the columns (must be unique).
     * @param dataTypes      The data types for each column (e.g., "int", "String").
     * @throws IllegalArgumentException if column names are not unique.
     */
    public DataFrame(ArrayList<ArrayList<Object>> data, ArrayList<String> variableNames, ArrayList<String> dataTypes) {
        ensureUniqueVariableNames(variableNames);
        this.data = data;
        this.variableNames = variableNames;
        this.dataTypes = dataTypes;
        buildColumnIndexMap();
    }

    /**
     * Constructs an empty DataFrame to be populated later with columns.
     */
    public DataFrame() {
        this.data = new ArrayList<>();
        this.variableNames = new ArrayList<>();
        this.dataTypes = new ArrayList<>();
        this.columnIndexMap = new HashMap<>();
    }

    /**
     * Constructs a DataFrame from a legacy 2D Object array and parallel metadata arrays.
     *
     * @param data           A 2D Object array where each row is a record.
     * @param variableNames  Column names (must be unique).
     * @param dataTypes      Data types corresponding to each column.
     * @throws IllegalArgumentException if column names are not unique.
     */
    public DataFrame(Object[][] data, String[] variableNames, String[] dataTypes) {
        ArrayList<String> variableNamesList = new ArrayList<>();
        ArrayList<String> dataTypesList = new ArrayList<>();

        for (int i = 0; i < variableNames.length; i++) {
            variableNamesList.add(variableNames[i]);
            dataTypesList.add(dataTypes[i]);
        }

        ensureUniqueVariableNames(variableNamesList);

        this.variableNames = variableNamesList;
        this.dataTypes = dataTypesList;
        this.data = new ArrayList<>();

        for (Object[] rowArray : data) {
            ArrayList<Object> row = new ArrayList<>();
            Collections.addAll(row, rowArray);
            this.data.add(row);
        }

        buildColumnIndexMap();
    }

    /**
     * Constructs a deep copy of another DataFrame.
     *
     * @param other The DataFrame to copy.
     */
    public DataFrame(DataFrame other) {
        this.variableNames = new ArrayList<>(other.variableNames);
        this.dataTypes = new ArrayList<>(other.dataTypes);
        this.data = new ArrayList<>();
        for (ArrayList<Object> row : other.data) {
            this.data.add(new ArrayList<>(row));
        }
        buildColumnIndexMap();
    }

    /** Builds or rebuilds the internal map from variable names to column indices. */
    private void buildColumnIndexMap() {
        columnIndexMap = new HashMap<>();
        for (int i = 0; i < variableNames.size(); i++) {
            columnIndexMap.put(variableNames.get(i), i);
        }
    }

    /**
     * Ensures that all variable (column) names are unique.
     *
     * @param names List of variable names to check.
     * @throws IllegalArgumentException if duplicates are found.
     */
    private void ensureUniqueVariableNames(ArrayList<String> names) {
        HashMap<String, Integer> seen = new HashMap<>();
        for (String name : names) {
            if (seen.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate variable name detected: " + name);
            }
            seen.put(name, 1);
        }
    }

    /**
     * Returns the value at a given row and column index.
     *
     * @param row    Row index.
     * @param column Column index.
     * @return The object stored at the specified position.
     */
    public Object getValue(int row, int column) {
        return data.get(row).get(column);
    }

    /**
     * Returns the value at a given row and column name.
     *
     * @param row          Row index.
     * @param variableName The name of the column.
     * @return The object stored at the specified position, or null if the column name does not exist.
     */
    public Object getValue(int row, String variableName) {
        Integer columnIndex = columnIndexMap.get(variableName);
        if (columnIndex == null) return null;
        return getValue(row, columnIndex);
    }

    /**
     * Sets a value in the DataFrame.
     *
     * @param row Row index
     * @param col Column index
     * @param value New value
     */
    public void setValue(int row, int col, Object value) {
        data.get(row).set(col, value);
    }

    /**
     * Returns the full data table as a list of rows.
     *
     * @return List of rows.
     */
    public ArrayList<ArrayList<Object>> getData() {
        return data;
    }

    /**
     * Returns the data type of a specific column.
     *
     * @param column The column index.
     * @return The data type string.
     */
    public String getDataType(int column) {
        return dataTypes.get(column);
    }

    /**
     * Returns the data types for all columns.
     *
     * @return List of data type strings.
     */
    public ArrayList<String> getDataTypes() {
        return dataTypes;
    }

    /**
     * Returns the data types for all columns as a String array.
     *
     * @return An array of data type strings.
     */
    public String[] getDataTypesArray() {
        return dataTypes.toArray(new String[0]);
    }

    /**
     * Returns the name of a specific column.
     *
     * @param column The column index.
     * @return The variable name.
     */
    public String getVariableName(int column) {
        return variableNames.get(column);
    }

    /**
     * Returns all variable (column) names.
     *
     * @return List of column names.
     */
    public ArrayList<String> getVariableNames() {
        return variableNames;
    }

    /**
     * Returns the variable (column) names as a String array.
     *
     * @return An array of variable names.
     */
    public String[] getVariableNamesArray() {
        return variableNames.toArray(new String[0]);
    }

    /**
     * Returns all values from a column specified by index.
     *
     * @param column The column index.
     * @return List of values in the column.
     */
    public ArrayList<Object> getVariable(int column) {
        ArrayList<Object> columnData = new ArrayList<>();
        for (ArrayList<Object> row : data) {
            columnData.add(row.get(column));
        }
        return columnData;
    }

    /**
     * Returns all values from a column specified by variable name.
     *
     * @param variableName The name of the column.
     * @return List of values, or an empty list if the column does not exist.
     */
    public ArrayList<Object> getVariable(String variableName) {
        Integer columnIndex = columnIndexMap.get(variableName);
        if (columnIndex == null) return new ArrayList<>();
        return getVariable(columnIndex);
    }

    /**
     * Returns a specific row from the DataFrame.
     *
     * @param row The row index.
     * @return A copy of the row's data.
     */
    public ArrayList<Object> getRow(int row) {
        return new ArrayList<>(data.get(row));
    }

    /**
     * Returns the number of columns.
     *
     * @return Column count.
     */
    public int ncol() {
        return variableNames.size();
    }

    /**
     * Returns the number of rows.
     *
     * @return Row count.
     */
    public int nrow() {
        return data.size();
    }

    /**
     * Deletes rows from the DataFrame by their indices.
     *
     * @param rowsToDelete A list of row indices to delete.
     */
    public void deleteRows(ArrayList<Integer> rowsToDelete) {
        rowsToDelete.sort((a, b) -> b - a); // Descending order
        for (int row : rowsToDelete) {
            if (row >= 0 && row < data.size()) {
                data.remove(row);
            }
        }
    }

    /**
     * Adds a new row to the DataFrame.
     *
     * @param rowData A list of values. Must match the number of columns.
     * @throws IllegalArgumentException if row length doesn't match column count.
     */
    public void addRow(ArrayList<Object> rowData) {
        if (rowData.size() != ncol()) {
            throw new IllegalArgumentException("Row length must match number of columns.");
        }
        data.add(rowData);
    }

    /**
     * Adds a new column to the DataFrame.
     *
     * @param variableName Name of the new column (must be unique).
     * @param dataType     Data type of the column.
     * @param columnData   List of values. Must match row count or define it if empty.
     * @throws IllegalArgumentException if column name is duplicate or length mismatch.
     */
    public void addColumn(String variableName, String dataType, ArrayList<Object> columnData) {
        if (columnIndexMap.containsKey(variableName)) {
            throw new IllegalArgumentException("Duplicate column name: " + variableName);
        }

        if (nrow() == 0) {
            for (Object value : columnData) {
                ArrayList<Object> newRow = new ArrayList<>();
                newRow.add(value);
                data.add(newRow);
            }
        } else {
            if (columnData.size() != nrow()) {
                throw new IllegalArgumentException("Column length must match number of rows.");
            }
            for (int i = 0; i < data.size(); i++) {
                data.get(i).add(columnData.get(i));
            }
        }

        variableNames.add(variableName);
        dataTypes.add(dataType);
        columnIndexMap.put(variableName, variableNames.size() - 1);
    }

    /**
     * Deletes a column by index.
     *
     * @param columnIndex The index of the column to delete.
     * @throws IndexOutOfBoundsException if index is invalid.
     */
    public void deleteColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= ncol()) {
            throw new IndexOutOfBoundsException("Column index out of bounds.");
        }

        for (ArrayList<Object> row : data) {
            row.remove(columnIndex);
        }

        String removedVar = variableNames.remove(columnIndex);
        dataTypes.remove(columnIndex);
        columnIndexMap.remove(removedVar);

        buildColumnIndexMap();
    }

    /**
     * Deletes multiple columns by index.
     *
     * @param columnIndices A list of column indices to delete.
     */
    public void deleteColumns(ArrayList<Integer> columnIndices) {
        columnIndices.sort((a, b) -> b - a);
        for (int columnIndex : columnIndices) {
            deleteColumn(columnIndex);
        }
    }

    /**
     * Deletes a column by name.
     *
     * @param variableName The name of the column to delete.
     * @throws IllegalArgumentException if the column name doesn't exist.
     */
    public void deleteColumn(String variableName) {
        Integer columnIndex = columnIndexMap.get(variableName);
        if (columnIndex == null) {
            throw new IllegalArgumentException("Column '" + variableName + "' does not exist.");
        }
        deleteColumn(columnIndex);
    }

    /**
     * Deletes multiple columns by name.
     *
     * @param variableNamesToDelete List of column names to delete.
     * @throws IllegalArgumentException if any column name doesn't exist.
     */
    public void deleteColumnsByName(ArrayList<String> variableNamesToDelete) {
        ArrayList<Integer> indices = new ArrayList<>();
        for (String varName : variableNamesToDelete) {
            Integer index = columnIndexMap.get(varName);
            if (index == null) {
                throw new IllegalArgumentException("Column '" + varName + "' does not exist.");
            }
            indices.add(index);
        }
        deleteColumns(indices);
    }

    /**
     * Converts the DataFrame to a 2D Object array.
     *
     * @return A new Object[][] representing the table.
     */
    public Object[][] toObjectArray() {
        int rows = nrow();
        int cols = ncol();
        Object[][] array = new Object[rows][cols];

        for (int i = 0; i < rows; i++) {
            ArrayList<Object> row = data.get(i);
            for (int j = 0; j < cols; j++) {
                array[i][j] = row.get(j);
            }
        }

        return array;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DataFrame with ").append(nrow()).append(" rows and ").append(ncol()).append(" columns:\n");
        for (int j = 0; j < ncol(); j++) {
            if (this.getVariableName(j).equals("ID")) {
                sb.append(String.format("%3s", this.getVariableName(j))).append("  ");
            } else if (this.getVariableName(j).equals("color")) {
                sb.append(String.format("%7s", this.getVariableName(j))).append("  ");
            } else {
                sb.append(String.format("%-12s", this.getVariableName(j))).append("  ");
            }
            sb.append("\n");
            for (int i = 0; i < Math.min(nrow(), 10); i++) { // show up to first 10 rows
                Object value = this.getValue(i, j);
                if (this.getVariableName(j).equals("ID")) {
                    sb.append(String.format("%3s", String.valueOf(value))).append("  ");
                } else if (this.getVariableName(j).equals("color")) {
                    sb.append(String.format("%7s", String.valueOf(value))).append("  ");
                } else {
                    sb.append(String.format("%-12.12s", String.valueOf(value))).append("  ");
                }
            }
            sb.append("\n");
        }
        if (nrow() > 10) {
            sb.append("... (").append(nrow() - 10).append(" more rows)\n");
        }
        return sb.toString();
    }
}