package sql;

import dna.Dna;
import dna.export.DataFrame;
import logger.LogEvent;
import logger.Logger;
import me.tongfei.progressbar.ProgressBar;
import model.Color;
import model.Entity;
import model.Statement;
import model.StatementType;
import model.Value;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class DataExchange {

	/* =================================================================================================================
	 * Functions for managing attributes
	 * =================================================================================================================
	 */

    /**
     * Get a data frame of entities (with ID, value, and color) and their attribute values.
     *
     * @param variableId The variable ID for which the entities should be queried.
     * @return A {@link DataFrame} object containing {@code m + 3} columns, with the first three columns representing
     *   the entity ID (int), entity value (String), and the entity color (hex RGB String) and the remaining {@code m}
     *   columns representing the String values of the attribute variables. The number of rows is the number of entities
     *   available for the variable being queried.
     */
    static public DataFrame getAttributes(int variableId) {
        Object[][] data = null;
        ArrayList<String> attributeVariableNames = new ArrayList<String>();
        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s1 = conn.prepareStatement("SELECT AttributeVariable FROM ATTRIBUTEVARIABLES WHERE VariableId = ? ORDER BY ATTRIBUTEVARIABLES.ID ASC;");
             PreparedStatement s2 = conn.prepareStatement("SELECT COUNT(ID) FROM ENTITIES WHERE VariableId = ?;");
             PreparedStatement s3 = conn.prepareStatement("SELECT ID, Value, Red, Green, Blue FROM ENTITIES WHERE VariableId = ?;");
             PreparedStatement s4 = conn.prepareStatement("SELECT ATTRIBUTEVARIABLES.AttributeVariable, ATTRIBUTEVALUES.AttributeValue FROM ATTRIBUTEVALUES INNER JOIN ATTRIBUTEVARIABLES ON ATTRIBUTEVARIABLES.ID = AttributeVariableId WHERE VariableId = ? AND EntityId = ? ORDER BY ATTRIBUTEVARIABLES.ID ASC;")) {

            // get attribute variable names
            s1.setInt(1, variableId);
            ResultSet r1 = s1.executeQuery();
            while (r1.next()) {
                attributeVariableNames.add(r1.getString("AttributeVariable"));
            }

            // get number of entities and initialize 2D array for the data
            int numEntities = 0;
            s2.setInt(1, variableId);
            r1 = s2.executeQuery();
            while (r1.next()) {
                numEntities = r1.getInt(1);
            }
            data = new Object[numEntities][attributeVariableNames.size() + 3]; // + entity ID, value, color

            // populate the data array; go through entities in outer loop
            s3.setInt(1, variableId);
            r1 = s3.executeQuery();
            ResultSet r2;
            int entityId;
            int rowCounter = 0;
            int columnCounter;
            while(r1.next()) {
                entityId = r1.getInt("ID");
                data[rowCounter][0] = entityId; // entity ID
                data[rowCounter][1] = r1.getString("Value"); // entity value
                data[rowCounter][2] = String.format("#%02X%02X%02X", r1.getInt("Red"), r1.getInt("Green"), r1.getInt("Blue")); // entity color as hex RGB value with leading hashtag

                // go through attribute values in inner loop
                columnCounter = 0;
                s4.setInt(1, variableId);
                s4.setInt(2, entityId);
                r2 = s4.executeQuery();
                while (r2.next()) {
                    data[rowCounter][columnCounter + 3] = r2.getString("AttributeValue");
                    columnCounter++;
                }
                rowCounter++;
            }
        } catch (SQLException ex) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Could not retrieve entities and their attributes.",
                    "Tried to fetch entities and attributes for Variable " + variableId + ", but something went wrong. Perhaps a connection issue? See error log.",
                    ex);
            Dna.logger.log(l);
        }

        // create data frame and return it
        attributeVariableNames.add(0, "color");
        attributeVariableNames.add(0, "value");
        attributeVariableNames.add(0, "ID");
        String[] variableNames = attributeVariableNames.stream().toArray(String[]::new);
        String[] dataTypes = new String[variableNames.length];
        dataTypes[0] = "int";
        dataTypes[1] = "String";
        dataTypes[2] = "String";
        for (int i = 3; i < variableNames.length; i++) {
            dataTypes[i] = "String";
        }
        DataFrame df = new DataFrame(data, variableNames, dataTypes);
        return df;
    }

    /**
     * A wrapper for {@link #getAttributes(int)} that first retrieves the variable ID based on statement type ID and
     * variable name.
     *
     * @param statementTypeId The statement type ID in which the variable is defined.
     * @param variable The name of the variable.
     * @return A data frame as returned by {@link #getAttributes(int)}.
     */
    static public DataFrame getAttributes(int statementTypeId, String variable) {
        int variableId = -1;
        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s = conn.prepareStatement("SELECT ID FROM VARIABLES WHERE Variable = ? AND StatementTypeId = ?;")) {
            s.setString(1, variable);
            s.setInt(2, statementTypeId);
            ResultSet r = s.executeQuery();
            while (r.next()) {
                variableId = r.getInt(1);
            }
        } catch (SQLException ex) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Could not retrieve variable ID for variable \"" + variable + ".",
                    "Could not retrieve the variable ID for variable \"" + variable + " (statement type ID: " + statementTypeId + ") while trying to retrieve entities and attributes. Check if the statement type ID and variable are valid.",
                    ex);
            Dna.logger.log(l);
        }
        return getAttributes(variableId);
    }

    /**
     * A wrapper for {@link #getAttributes(int)} that first retrieves the variable ID based on statement type and
     * variable names.
     *
     * @param statementType The statement type in which the variable is defined.
     * @param variable The name of the variable.
     * @return A data frame as returned by {@link #getAttributes(int)}.
     */
    static public DataFrame getAttributes(String statementType, String variable) {
        int variableId = -1;
        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s = conn.prepareStatement("SELECT ID FROM VARIABLES WHERE Variable = ? AND StatementTypeId = (SELECT ID FROM STATEMENTTYPES WHERE Label = ?);")) {
            s.setString(1, variable);
            s.setString(2, statementType);
            ResultSet r = s.executeQuery();
            while (r.next()) {
                variableId = r.getInt(1);
            }
        } catch (SQLException ex) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Could not retrieve variable ID for variable \"" + variable + ".",
                    "Could not retrieve the variable ID for variable \"" + variable + " (statement type: \"" + statementType + "\") while trying to retrieve entities and attributes. Check if the statement type and variable are valid.",
                    ex);
            Dna.logger.log(l);
        }
        return getAttributes(variableId);
    }

    /**
     * Set entities and attributes for a variable by comparing with a supplied data frame.
     *
     * @param variableId The ID of the variable for which entities should be set.
     * @param df A {@link DataFrame} object containing {@code m + 3} columns, with the first three columns representing
     *   the entity ID (int), entity value (String), and the entity color (hex RGB String) and the remaining {@code m}
     *   columns representing the String values of the attribute variables. The number of rows is the number of entities
     * 	 available for the variable.
     * @param simulate If {@code true}, the changes are rolled back. If {@code false}, the changes are committed to the
     *   database.
     */
    static public void setAttributes(int variableId, DataFrame df, boolean simulate) {
        String s = "";
        try (ProgressBar pb = new ProgressBar("Setting attributes...", 15)) {
            pb.stepTo(1);
            pb.setExtraMessage("Setting up parameters...");
            try (Connection conn = Dna.sql.getDataSource().getConnection();
                 PreparedStatement s1 = conn.prepareStatement("SELECT ID AS AttributeVariableId, AttributeVariable FROM ATTRIBUTEVARIABLES WHERE VariableId = ? ORDER BY ID ASC;");
                 PreparedStatement s2 = conn.prepareStatement("SELECT ID AS EntityId, Value, Red, Green, Blue FROM ENTITIES WHERE VariableId = ?;");
                 PreparedStatement s3 = conn.prepareStatement("DELETE FROM ATTRIBUTEVARIABLES WHERE VariableId = ? AND AttributeVariable = ?;");
                 PreparedStatement s4 = conn.prepareStatement("INSERT INTO ATTRIBUTEVARIABLES (VariableId, AttributeVariable) VALUES(?, ?);", PreparedStatement.RETURN_GENERATED_KEYS);
                 PreparedStatement s5 = conn.prepareStatement("DELETE FROM ENTITIES WHERE ID = ?;");
                 PreparedStatement s6 = conn.prepareStatement("UPDATE ENTITIES SET Value = ? WHERE ID = ?;");
                 PreparedStatement s7 = conn.prepareStatement("INSERT INTO ATTRIBUTEVALUES (EntityId, AttributeVariableId, AttributeValue) VALUES (?, ?, ?);");
                 PreparedStatement s8 = conn.prepareStatement("UPDATE DATASHORTTEXT SET Entity = ? WHERE Entity = ?;");
                 PreparedStatement s9 = conn.prepareStatement("INSERT INTO ENTITIES (VariableId, Value, Red, Green, Blue) VALUES (?, ?, ?, ?, ?);", PreparedStatement.RETURN_GENERATED_KEYS);
                 PreparedStatement s10 = conn.prepareStatement("UPDATE ENTITIES SET Value = ?, Red = ?, Green = ?, Blue = ? WHERE ID = ?;");
                 PreparedStatement s11 = conn.prepareStatement("UPDATE ATTRIBUTEVALUES SET AttributeValue = ? WHERE EntityId = ? AND AttributeVariableId = (SELECT ID FROM ATTRIBUTEVARIABLES WHERE VariableId = ? AND AttributeVariable = ?);");
                 PreparedStatement s12 = conn.prepareStatement("SELECT EntityId, AttributeVariable, AttributeValue FROM ATTRIBUTEVALUES INNER JOIN ATTRIBUTEVARIABLES ON ATTRIBUTEVARIABLES.ID = AttributeVariableId WHERE ATTRIBUTEVARIABLES.VariableID = ?;");
                 PreparedStatement s13 = conn.prepareStatement("SELECT COUNT(ID) FROM DATASHORTTEXT WHERE Entity = ?;");
                 PreparedStatement s14 = conn.prepareStatement("SELECT COUNT(ID) FROM ENTITIES WHERE VariableId = ?;");
                 PreparedStatement s15 = conn.prepareStatement("SELECT COUNT(ID) FROM STATEMENTS WHERE StatementTypeId = (SELECT StatementTypeId FROM VARIABLES WHERE ID = ?);");
                 PreparedStatement s16 = conn.prepareStatement("INSERT INTO ATTRIBUTEVALUES (EntityId, AttributeVariableId, AttributeValue) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM ATTRIBUTEVALUES WHERE EntityId = ? AND AttributeVariableId = ?);");
                 PreparedStatement s17 = conn.prepareStatement("SELECT StatementTypeId FROM VARIABLES WHERE ID = ?");
                 PreparedStatement s18 = conn.prepareStatement("SELECT COUNT(*) FROM VARIABLES WHERE StatementTypeId = ? AND DataType = 'short text'");
                 PreparedStatement s19 = conn.prepareStatement("SELECT StatementId, COUNT(*) FROM DATASHORTTEXT GROUP BY StatementId");
                 PreparedStatement s20 = conn.prepareStatement("DELETE FROM STATEMENTS WHERE ID = ?");
                 PreparedStatement s21 = conn.prepareStatement("SELECT ID FROM STATEMENTS WHERE StatementTypeId = ?");

                 Sql.SQLCloseable finish = conn::rollback) {
                conn.setAutoCommit(false);

                // record keeping for simulation reporting
                int counterAddedAttributeVariables = 0;
                int counterRemovedAttributeVariables = 0;
                int previousStateAttributeVariables = 0;
                int currentStateAttributeVariables = df.ncol() - 3; // record keeping
                int previousStateEntities = 0;
                int currentStateEntities = 0;
                int counterAddedEntities = 0;
                int counterRemovedEntities = 0;
                int counterRecodedEntities = 0;
                int counterRenamedEntities = 0;
                int counterUpdatedColors = 0;
                int counterStatementsRenamed = 0;
                int previousStateStatements = 0;
                int currentStateStatements = 0;
                int counterUpdatedAttributeValues = 0;

                // count how many statements are in this variable type after the changes (for record keeping)
                pb.stepTo(2);
                pb.setExtraMessage("Count statem.");
                s15.setInt(1, variableId);
                ResultSet r1 = s15.executeQuery();
                while (r1.next()) {
                    previousStateStatements = r1.getInt(1); // record keeping
                }

                // retrieve existing entity IDs to be able to add a new attribute variable for them if necessary and check if an entity already exists before considering to add it
                pb.stepTo(3);
                pb.setExtraMessage("Load entities");
                ArrayList<Integer> entityIds = new ArrayList<Integer>();
                s2.setInt(1, variableId);
                r1 = s2.executeQuery();
                while (r1.next()) {
                    entityIds.add(r1.getInt("EntityId"));
                }
                previousStateEntities = entityIds.size(); // record keeping

                // remove attribute variables from database that were removed from the data frame
                pb.stepTo(4);
                pb.setExtraMessage("Remove attr var");
                ArrayList<String> dfVarNames = df.getVariableNames();
                s1.setInt(1,  variableId);
                r1 = s1.executeQuery();
                ArrayList<String> attributeVariableNames = new ArrayList<String>();
                while (r1.next()) {
                    attributeVariableNames.add(r1.getString("AttributeVariable"));
                }
                previousStateAttributeVariables = attributeVariableNames.size(); // record keeping
                for (int i = 0; i < attributeVariableNames.size(); i++) {
                    if (!dfVarNames.subList(3, dfVarNames.size()).contains((String) attributeVariableNames.get(i))) {
                        s3.setInt(1, variableId);
                        s3.setString(2, (String) attributeVariableNames.get(i));
                        s3.executeUpdate();
                        counterRemovedAttributeVariables++; // record keeping
                    }
                }

                // add attribute variables to database if they were added to the data frame
                pb.stepTo(5);
                pb.setExtraMessage("Add attr. vars.");
                ResultSet generatedKeysResultSet;
                for (int i = 3; i < dfVarNames.size(); i++) {
                    if (!attributeVariableNames.contains(dfVarNames.get(i))) {
                        // add to ATTRIBUTEVARIABLES and get its new ID
                        s4.setInt(1, variableId);
                        s4.setString(2, dfVarNames.get(i));
                        s4.executeUpdate();
                        generatedKeysResultSet = s4.getGeneratedKeys();
                        int attributeVariableId = -1;
                        while (generatedKeysResultSet.next()) {
                            attributeVariableId = generatedKeysResultSet.getInt(1);
                        }

                        // also add to ATTRIBUTEVALUES, not just ATTRIBUTEVARIABLES table
                        for (int j = 0; j < entityIds.size(); j++) {
                            s7.setInt(1, entityIds.get(j));
                            s7.setInt(2, attributeVariableId);
                            s7.setString(3, "");
                            s7.executeUpdate();
                        }

                        counterAddedAttributeVariables++; // record keeping
                    }
                }

                // get current attribute variables for the given variable ID from the database
                pb.stepTo(6);
                pb.setExtraMessage("Reload attr var");
                ArrayList<Integer> attributeVariableIds = new ArrayList<Integer>();
                attributeVariableNames.clear();
                s1.setInt(1, variableId);
                r1 = s1.executeQuery();
                while (r1.next()) {
                    attributeVariableIds.add(r1.getInt("AttributeVariableId"));
                    attributeVariableNames.add(r1.getString("AttributeVariable"));
                }

                // read entity IDs and values from database; necessary for aggregating duplicate rows below
                pb.stepTo(7);
                pb.setExtraMessage("Reload entities");
                HashMap<Integer, String> entityValues = new HashMap<Integer, String>();
                s2.setInt(1, variableId);
                r1 = s2.executeQuery();
                while (r1.next()) {
                    entityValues.put(r1.getInt("EntityId"), r1.getString("Value"));
                }

                // aggregate/unite duplicate entity values in df into a single entity ID in the database, and adjust df accordingly
                pb.stepTo(8);
                pb.setExtraMessage("Unify dupl ent.");
                ArrayList<Entity> duplicates = new ArrayList<Entity>();
                HashMap<String, String> av = new HashMap<String, String>();
                Entity e;
                ArrayList<Integer> completed = new ArrayList<Integer>();
                ArrayList<Integer> rowIndicesToBeDiscarded = new ArrayList<Integer>();
                ArrayList<Integer> rowIndicesOfDuplicates = new ArrayList<Integer>();
                for (int i = 0; i < df.nrow(); i++) {
                    if (!completed.contains(i)) {
                        // add Entity i to array list
                        duplicates.clear();
                        rowIndicesOfDuplicates.clear();
                        av.clear();
                        for (int k = 3; k < df.ncol(); k++) {
                            av.put(df.getVariableName(k), (String) df.getValue(i, k));
                        }
                        Color color = new Color((String) df.getValue(i, 2));
                        int entityId = (Integer) df.getValue(i, 0);
                        e = new Entity(entityId, variableId, (String) df.getValue(i, 1), color, -1, entityValues.containsKey((int) df.getValue(i, 0)), av);
                        duplicates.add(e);
                        rowIndicesOfDuplicates.add(i);
                        completed.add(i);

                        // check for duplicate entity values and add duplicate entities to array list
                        for (int j = 0; j < df.nrow(); j++) {
                            if (i != j && !completed.contains(j) && ((String) df.getValue(j, 1)).equals((String) df.getValue(i, 1))) { // check if duplicate value
                                av.clear();
                                for (int k = 3; k < df.ncol(); k++) {
                                    av.put(df.getVariableName(k), (String) df.getValue(j, k));
                                }
                                color = new Color((String) df.getValue(j, 2));
                                entityId = (Integer) df.getValue(j, 0);
                                e = new Entity(entityId, variableId, (String) df.getValue(j, 1), color, -1, entityValues.containsKey((int) df.getValue(j, 0)), av);
                                duplicates.add(e);
                                rowIndicesOfDuplicates.add(j);
                                completed.add(j);
                            }
                        }

                        // find an entity that contains the richest set of information (the target entity to keep); ideally one that is in the database, which would trump number of attribute values and presence of color other than the default black
                        int targetId = -1;
                        boolean targetColor = false;
                        int targetAttributes = 0;
                        boolean targetInDatabase = false;
                        for (int j = 0; j < duplicates.size(); j++) {
                            if (targetId == -1) { // pick the first duplicate as initial target ID
                                targetId = duplicates.get(j).getId();
                                targetColor = !duplicates.get(j).getColor().equals(new Color(0, 0, 0));
                                targetAttributes = 0;
                                for (int k = 3; k < df.ncol(); k++) {
                                    if (!duplicates.get(j).getAttributeValues().get(df.getVariableName(k)).equals("")) {
                                        targetAttributes++;
                                    }
                                }
                                targetInDatabase = duplicates.get(j).isInDatabase();
                            } else { // if not the first duplicate, compare with the previous target based on database presence, attribute values, and color
                                boolean candidateColor = !duplicates.get(j).getColor().equals(new Color(0, 0, 0));
                                int candidateAttributes = 0;
                                for (int k = 3; k < df.ncol(); k++) {
                                    if (!duplicates.get(j).getAttributeValues().get(df.getVariableName(k)).equals("")) {
                                        candidateAttributes++;
                                    }
                                }
                                boolean accept = false;
                                if (duplicates.get(j).isInDatabase() && !targetInDatabase) {
                                    accept = true;
                                } else if (!duplicates.get(j).isInDatabase() && targetInDatabase) {
                                    accept = false;
                                } else if (candidateAttributes > targetAttributes) {
                                    accept = true;
                                } else if (candidateAttributes == targetAttributes && candidateColor && !targetColor) {
                                    accept = true;
                                }
                                if (accept) {
                                    targetInDatabase = duplicates.get(j).isInDatabase();
                                    targetId = duplicates.get(j).getId();
                                    targetColor = candidateColor;
                                    targetAttributes = candidateAttributes;
                                }
                            }
                        }
                        final int acceptedTargetId = targetId;

                        // if the target does not exist in the database, add it to the ENTITIES and ATTRIBUTEVALUES tables
                        boolean targetEntityAdded = false;
                        if (duplicates.size() > 1 && !entityValues.containsKey(targetId)) {
                            Entity targetEntity = duplicates.stream().filter(d -> d.getId() == acceptedTargetId).findFirst().get();
                            s9.setInt(1, variableId);
                            s9.setString(2, targetEntity.getValue());
                            s9.setInt(3, targetEntity.getColor().getRed());
                            s9.setInt(4, targetEntity.getColor().getGreen());
                            s9.setInt(5, targetEntity.getColor().getBlue());
                            s9.executeUpdate();
                            generatedKeysResultSet = s9.getGeneratedKeys();
                            while (generatedKeysResultSet.next()) {
                                // add entries for each attribute variable to ATTRIBUTEVALUES
                                int generatedEntityId = generatedKeysResultSet.getInt(1);
                                for (int j = 0; j < attributeVariableIds.size(); j++) {
                                    s7.setInt(1, generatedEntityId);
                                    s7.setInt(2, attributeVariableIds.get(j));
                                    s7.setString(3, targetEntity.getAttributeValues().get(attributeVariableNames.get(j)));
                                    s7.executeUpdate();
                                }
                                counterAddedEntities++; // record keeping
                            }
                            targetEntityAdded = true;
                        }

                        // go through all duplicates and make changes to database
                        if (duplicates.size() > 1) {
                            for (int j = 0; j < duplicates.size(); j++) {
                                // if target duplicate and if already existing but renamed in data frame, rename value in the database
                                if (duplicates.get(j).getId() == targetId && !targetEntityAdded && !entityValues.get(duplicates.get(j).getId()).equals(duplicates.get(j).getValue())) {
                                    s6.setString(1, duplicates.get(j).getValue());
                                    s6.setInt(2, duplicates.get(j).getId());
                                    s6.executeUpdate();
                                    counterStatementsRenamed++;
                                } else if (duplicates.get(j).getId() != targetId) { // if not the target but a regular duplicate...
                                    // change ID of the duplicate in the DATASHORTTEXT table to the target entity ID
                                    s8.setInt(1, acceptedTargetId);
                                    s8.setInt(2, duplicates.get(j).getId());
                                    s8.executeUpdate();

                                    // remove from ENTITIES (cascading to ATTRIBUTEVALUES)
                                    s5.setInt(1, duplicates.get(j).getId());
                                    s5.executeUpdate();
                                    counterRecodedEntities++;

                                    // discard duplicate from df to avoid it being added later
                                    rowIndicesToBeDiscarded.add(rowIndicesOfDuplicates.get(j));
                                }
                            }
                        }
                    }
                }
                df.deleteRows(rowIndicesToBeDiscarded);

                // refresh entity IDs from database in memory
                pb.stepTo(9);
                pb.setExtraMessage("Reload entities");
                entityIds.clear();
                entityValues.clear();
                s2.setInt(1, variableId);
                r1 = s2.executeQuery();
                while (r1.next()) {
                    int eid = r1.getInt("EntityId");
                    entityIds.add(eid);
                    entityValues.put(eid, r1.getString("Value"));
                }
                
                // remove entities from database that were removed from data frame
                pb.stepTo(10);
                pb.setExtraMessage("Remove entities");
                ArrayList<Integer> dfEntityIds = new ArrayList<Integer>();
                for (int i = 0; i < df.nrow(); i++) {
                    int entityId = (Integer) df.getValue(i, 0);
                    dfEntityIds.add(entityId);
                }
                for (int i = 0; i < entityIds.size(); i++) {
                    int eid = entityIds.get(i);
                    String val = entityValues.get(eid);
                    if (!dfEntityIds.contains(entityIds.get(i)) && val != null && !val.equals("")) { // check to make sure that entity ID is present in data frame but not in database and has a non-empty ("") value
                        s5.setInt(1, entityIds.get(i));
                        s5.executeUpdate();
                        counterRemovedEntities++; // record keeping
                    }
                }

                // add entities to database if they were added to the data frame
                pb.stepTo(11);
                pb.setExtraMessage("Add entities");
                HashMap<Integer, Integer> tempIdToGeneratedId = new HashMap<>();
                String entityValue;
                Color color;
                for (int i = 0; i < df.nrow(); i++) {
                    // check if the entity ID exists in ENTITIES and proceed if not (e.g., if -1)
                    // no need to check whether value already exists because duplicates have already been aggregated
                    if (!entityIds.contains((int) df.getValue(i, 0))) {
                        int tempId = (int) df.getValue(i, 0); // e.g., -1
                        // add into ENTITIES
                        s9.setInt(1, variableId);
                        s9.setString(2, (String) df.getValue(i, 1));
                        color = new Color((String) df.getValue(i, 2));
                        s9.setInt(3, color.getRed());
                        s9.setInt(4, color.getGreen());
                        s9.setInt(5, color.getBlue());
                        s9.executeUpdate();
                        generatedKeysResultSet = s9.getGeneratedKeys();
                        while (generatedKeysResultSet.next()) {
                            // add entries for each attribute variable to ATTRIBUTEVALUES
                            int generatedEntityId = generatedKeysResultSet.getInt(1);
                            tempIdToGeneratedId.put(tempId, generatedEntityId); // keep a record of temp ID to generated ID mapping
                            for (int j = 0; j < attributeVariableIds.size(); j++) {
                                s7.setInt(1, generatedEntityId);
                                s7.setInt(2, attributeVariableIds.get(j));
                                s7.setString(3, (String) df.getValue(i, attributeVariableNames.get(j)));
                                s7.executeUpdate();
                            }
                            counterAddedEntities++; // record keeping
                        }
                    }
                }

                if (!simulate) {
                    for (int i = 0; i < df.nrow(); i++) {
                        int id = (int) df.getValue(i, 0);
                        if (tempIdToGeneratedId.containsKey(id)) {
                            df.setValue(i, 0, tempIdToGeneratedId.get(id));
                        }
                    }
                }


                // refresh entity IDs and values from database in memory (no entityMap yet)
                entityIds.clear();
                entityValues.clear();
                s2.setInt(1, variableId);
                r1 = s2.executeQuery();
                while (r1.next()) {
                    int eid = r1.getInt("EntityId");
                    entityIds.add(eid);
                    entityValues.put(eid, r1.getString("Value"));
                }

                // ensure ATTRIBUTEVALUES completeness after adding entities
                for (int eid : entityIds) {
                    for (int k = 0; k < attributeVariableIds.size(); k++) {
                        int avid = attributeVariableIds.get(k);
                        s16.setInt(1, eid);
                        s16.setInt(2, avid);
                        s16.setString(3, "");
                        s16.setInt(4, eid);
                        s16.setInt(5, avid);
                        s16.executeUpdate();
                    }
                }

                // build authoritative entity snapshot (used in step 12)
                HashMap<Integer, Entity> entityMap = new HashMap<>();
                s2.setInt(1, variableId);
                r1 = s2.executeQuery();
                while (r1.next()) {
                    int eid = r1.getInt("EntityId");
                    entityMap.put(
                        eid,
                        new Entity(
                            eid,
                            variableId,
                            r1.getString("Value"),
                            new Color(
                                r1.getInt("Red"),
                                r1.getInt("Green"),
                                r1.getInt("Blue")
                            )
                        )
                    );
                }

                s12.setInt(1, variableId);
                r1 = s12.executeQuery();
                while (r1.next()) {
                    Entity ent = entityMap.get(r1.getInt("EntityId"));
                    if (ent != null) {
                        ent.getAttributeValues().put(
                            r1.getString("AttributeVariable"),
                            r1.getString("AttributeValue")
                        );
                    }
                }

                // update entities and attribute values
                pb.stepTo(12);
                pb.setExtraMessage("Update values");
                int entityId;
                Color entityColor;
                for (int i = 0; i < df.nrow(); i++) {
                    // update ENTITIES table if necessary
                    entityId = (int) df.getValue(i, 0);
                    if (!entityMap.containsKey(entityId)) {
                        continue;
                    }
                    entityValue = (String) df.getValue(i, 1);
                    entityColor = new Color((String) df.getValue(i, 2));
                    Entity existing = entityMap.get(entityId);
                    if (existing == null) {
                        continue; // entity was newly added or not yet in DB
                    }
                    if (!entityValue.equals(entityMap.get(entityId).getValue())
                            || entityMap.get(entityId).getColor().getRed() != entityColor.getRed()
                            || entityMap.get(entityId).getColor().getGreen() != entityColor.getGreen()
                            || entityMap.get(entityId).getColor().getBlue() != entityColor.getBlue()) {
                        s10.setString(1, entityValue);
                        s10.setInt(2, entityColor.getRed());
                        s10.setInt(3, entityColor.getGreen());
                        s10.setInt(4, entityColor.getBlue());
                        s10.setInt(5, entityId);
                        s10.executeUpdate();
                        if (!entityValue.equals(entityMap.get(entityId).getValue())) {
                            counterRenamedEntities++; // record keeping
                            s13.setInt(1, entityId);
                            r1 = s13.executeQuery();
                            while (r1.next()) {
                                counterStatementsRenamed = counterStatementsRenamed + r1.getInt(1); // record keeping
                            }
                        } else {
                            counterUpdatedColors++; // record keeping
                        }
                    }
                    // update ATTRIBUTEVALUES table if necessary
                    for (int j = 3; j < df.ncol(); j++) {
                        if (!entityMap.containsKey(entityId)) continue; // just a safety check; probably not necessary
                        if (!Objects.equals(
                            entityMap.get(entityId).getAttributeValues().get(df.getVariableName(j)),
                            (String) df.getValue(i, j)
                        )) {
                            s11.setString(1, (String) df.getValue(i, j));
                            s11.setInt(2, entityId);
                            s11.setInt(3, variableId);
                            s11.setString(4, df.getVariableName(j));
                            s11.executeUpdate();
                            counterUpdatedAttributeValues++; // record keeping
                        }
                    }
                }

                // check how many entities are in this variable type after the changes (for record keeping)
                pb.stepTo(13);
                pb.setExtraMessage("Count entities");
                s14.setInt(1, variableId);
                r1 = s14.executeQuery();
                while (r1.next()) {
                    currentStateEntities = r1.getInt(1); // record keeping
                }

                // remove orphan statements (statements without DATASHORTTEXT rows) and count how many statements are in this variable type after the changes (for record keeping)
                pb.stepTo(14);
                pb.setExtraMessage("Remove statem.");
                int deletedStatements = 0;
                s17.setInt(1, variableId);
                r1 = s17.executeQuery();
                int statementTypeId = -1;
                if (r1.next()) {
                   statementTypeId = r1.getInt(1);
                }
                if (statementTypeId != -1) {
                    // how many short-text variables are required?
                    s18.setInt(1, statementTypeId);
                    r1 = s18.executeQuery();
                    int requiredVars = 0;
                    if (r1.next()) {
                        requiredVars = r1.getInt(1);
                    }
                    // count DATASHORTTEXT rows per statement
                    HashMap<Integer, Integer> presentCounts = new HashMap<>();
                    r1 = s19.executeQuery();
                    while (r1.next()) {
                        presentCounts.put(r1.getInt(1), r1.getInt(2));
                    }
                    // iterate statements of this type
                    s21.setInt(1, statementTypeId);
                    r1 = s21.executeQuery();
                    while (r1.next()) {
                        int sid = r1.getInt(1);
                        int present = presentCounts.getOrDefault(sid, 0);
                        if (present < requiredVars) {
                            s20.setInt(1, sid);
                            s20.executeUpdate();
                            deletedStatements++;
                        }
                    }
                    currentStateStatements = previousStateStatements - deletedStatements; // record keeping
                }

                // commit the changes to the database or roll back if simulation
                pb.stepTo(15);
                if (simulate) {
                    pb.setExtraMessage("Rolling back");
                    conn.rollback();
                } else {
                    pb.setExtraMessage("Committing");
                    conn.commit();
                }

                // print console report
                s = "\nRecorded changes in entities and attributes:\n" +
                        "               Attribute variables  Attributes  Entities  Statements\n" +
                        "Added         " + String.format("%20d", counterAddedAttributeVariables) + "           - " + String.format("%9d", counterAddedEntities) + "           -\n" +
                        "Removed       " + String.format("%20d", counterRemovedAttributeVariables) + "           - " + String.format("%9d", counterRemovedEntities) + " " + String.format("%11d", deletedStatements) + "\n" +
                        "Value updates                    - " + String.format("%11d", counterUpdatedAttributeValues) + " " + String.format("%9d", counterRenamedEntities) + " " + String.format("%11d", counterStatementsRenamed) + "\n" +
                        "Color updates                    -           - " + String.format("%9d", counterUpdatedColors) + "           -\n" +
                        "Num before    " + String.format("%20d", previousStateAttributeVariables) + "           - " + String.format("%9d", previousStateEntities) + " " + String.format("%11d", previousStateStatements) + "\n" +
                        "Num after     " + String.format("%20d", currentStateAttributeVariables) + "           - " + String.format("%9d", currentStateEntities) + " " + String.format("%11d", currentStateStatements) + "\n";
                if (simulate) {
                    s = s + "All changes were only simulated. The database remains unchanged.";
                } else {
                    s = s + "All changes have been written into the database.";
                }
            } catch (SQLException e) {
                LogEvent l = new LogEvent(Logger.ERROR,
                        "Setting entities and attributes failed.",
                        "Tried to set attributes by supplying a data frame with entities and attribute values, but failed. No changes were made to the database. See the error stack for details.",
                        e);
                Dna.logger.log(l);
                e.printStackTrace(); // show real SQL error in console
                throw new RuntimeException("setAttributes failed (see stack trace above)", e);
            }
        }
        System.out.println(s);
    }

    /**
     * Set entities and attributes for a variable identified by statement type ID and variable name.
     *
     * @param statementTypeId The ID of the statement type in which the variable is defined.
     * @param variable        The name of the variable.
     * @param df              A DataFrame containing entity IDs, values, colors, and attribute values.
     * @param simulate        If true, changes are rolled back; otherwise they are committed.
     */
    static public void setAttributes(
            int statementTypeId,
            String variable,
            DataFrame df,
            boolean simulate) {
        int variableId = -1;

        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s = conn.prepareStatement("SELECT ID FROM VARIABLES WHERE Variable = ? AND StatementTypeId = ?;")) {

            s.setString(1, variable);
            s.setInt(2, statementTypeId);

            ResultSet r = s.executeQuery();
            while (r.next()) {
                variableId = r.getInt(1);
            }

        } catch (SQLException ex) {
            LogEvent l = new LogEvent(
                    Logger.ERROR,
                    "Could not retrieve variable ID.",
                    "Failed to retrieve variable ID for variable \"" + variable + "\" (statement type ID: " + statementTypeId + ").",
                    ex);
            Dna.logger.log(l);
            return;
        }

        setAttributes(variableId, df, simulate);
    }

    /**
     * Set entities and attributes for a variable identified by statement type name and variable name.
     *
     * @param statementType The name of the statement type in which the variable is defined.
     * @param variable      The name of the variable.
     * @param df            A DataFrame containing entity IDs, values, colors, and attribute values.
     * @param simulate      If true, changes are rolled back; otherwise they are committed.
     */
    static public void setAttributes(
            String statementType,
            String variable,
            DataFrame df,
            boolean simulate) {
        int variableId = -1;

        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s = conn.prepareStatement("SELECT ID FROM VARIABLES WHERE Variable = ? AND StatementTypeId = (SELECT ID FROM STATEMENTTYPES WHERE Label = ?);")) {

            s.setString(1, variable);
            s.setString(2, statementType);

            ResultSet r = s.executeQuery();
            while (r.next()) {
                variableId = r.getInt(1);
            }

        } catch (SQLException ex) {
            LogEvent l = new LogEvent(
                    Logger.ERROR,
                    "Could not retrieve variable ID.",
                    "Failed to retrieve variable ID for variable \"" + variable + "\" (statement type: \"" + statementType + "\").",
                    ex);
            Dna.logger.log(l);
            return;
        }

        setAttributes(variableId, df, simulate);
    }

	/* =================================================================================================================
	 * Functions for managing statements
	 * =================================================================================================================
	 */

    /**
     * Get a data frame with all statements of a specific type (based on the
     * statement type ID) and with specific statement IDs.
     *
     * @param statementTypeId The ID of the statement type to retrieve statements
     *                        for.
     * @param statementIds    An array of statement IDs to retrieve. If this array
     *                        is empty, all statements of the statement type are
     *                        retrieved.
     * @return A data frame with the following columns: ID, Document ID, Start,
     *         Stop, Coder, and all variables of the statement type.
     */
    public static DataFrame getStatements(int statementTypeId, int[] statementIds) {
        ArrayList<Statement> statements = Dna.sql.getStatements(statementIds, statementTypeId, null, null, null, false,
                null, false, null, false, null, false);

        if (statements.isEmpty())
            return new DataFrame();

        DataFrame df = new DataFrame();

        // Standard columns
        ArrayList<Object> ids = new ArrayList<>();
        ArrayList<Object> statementTypeIds = new ArrayList<>();
        ArrayList<Object> docIds = new ArrayList<>();
        ArrayList<Object> starts = new ArrayList<>();
        ArrayList<Object> stops = new ArrayList<>();
        ArrayList<Object> coders = new ArrayList<>();

        // Add variable name -> values map for dynamic columns
        ArrayList<String> variableNames = new ArrayList<>();
        ArrayList<String> dataTypes = new ArrayList<>();
        ArrayList<ArrayList<Object>> variableColumns = new ArrayList<>();

        Statement first = statements.get(0);
        for (int i = 0; i < first.getValues().size(); i++) {
            String name = first.getValues().get(i).getKey();
            String type = first.getValues().get(i).getDataType();
            if (type.equals("short text") || type.equals("long text")) {
                type = "String";
            } else if (type.equals("integer") || type.equals("boolean")) {
                type = "int";
            }
            variableNames.add(name);
            dataTypes.add(type);
            variableColumns.add(new ArrayList<>());
        }

        for (Statement s : statements) {
            ids.add(s.getId());
            statementTypeIds.add(statementTypeId);
            docIds.add(s.getDocumentId());
            starts.add(s.getStart());
            stops.add(s.getStop());
            coders.add(s.getCoderId());

            for (int j = 0; j < s.getValues().size(); j++) {
                Object value = s.getValues().get(j).getValue();
                if ("short text".equals(s.getValues().get(j).getDataType())) {
                    value = ((Entity) value).getValue();
                }
                variableColumns.get(j).add(value);
            }
        }

        // Add standard columns
        df.addColumn("ID", "int", ids);
        df.addColumn("statement_type_id", "int", statementTypeIds);
        df.addColumn("document_id", "int", docIds);
        df.addColumn("start", "int", starts);
        df.addColumn("stop", "int", stops);
        df.addColumn("coder_id", "int", coders);

        // Add variable columns
        for (int i = 0; i < variableNames.size(); i++) {
            df.addColumn(variableNames.get(i), dataTypes.get(i), variableColumns.get(i));
        }

        return df;
    }

    /**
     * Get a data frame with all statements of a specific type (based on the statement type label) and with specific statement IDs.
     * 
     * @param statementType The label of the statement type to retrieve statements for.
     * @param statementIds An array of statement IDs to retrieve. If this array is empty, all statements of the statement type are retrieved.
     * @return A data frame with the following columns: ID, Document ID, Start, Stop, Coder, and all variables of the statement type.
     */
    static public DataFrame getStatements(String statementType, int[] statementIds) {
        int statementTypeId = -1;
        try (Connection conn = Dna.sql.getDataSource().getConnection();
             PreparedStatement s = conn.prepareStatement("SELECT ID FROM STATEMENTTYPES WHERE Label = ?;")) {
            s.setString(1, statementType);
            ResultSet r = s.executeQuery();
            while (r.next()) {
                statementTypeId = r.getInt(1);
            }
        } catch (SQLException ex) {
            LogEvent l = new LogEvent(Logger.ERROR,
                    "Could not retrieve statement type ID for statement type \"" + statementType + ".",
                    "Could not retrieve the statement type ID for statement type \"" + statementType + "\" while trying to retrieve statements. Check if the statement type is valid.",
                    ex);
            Dna.logger.log(l);
        }
        return getStatements(statementTypeId, statementIds);
    }

	/**
	 * Add a new statement with custom contents to the database. Entities can be null. Entity IDs will be disregarded; entities are matched with the database using their values.
	 * 
	 * @param documentId       The document ID of the document to which the statement should be added.
	 * @param startCaret       The start position of the statement in the document.
	 * @param endCaret         The stop position of the statement in the document.
	 * @param statementTypeId  The ID of the statement type of which the statement to be created is an instance.
	 * @param coderId          The ID of the coder that adds the current statement.
	 * @param varNames         The variable names to which the values should be added, in the same order as the values.
	 * @param values           The values to be added to the statement variables as an {@link Object} array.
	 * @return                 A new ID of the statement that was added.
	 */
	static public int addStatement(int documentId, int startCaret, int endCaret, int statementTypeId, int coderId, String[] varNames, Object[] values) {
        StatementType st = Dna.sql.getStatementType(statementTypeId);

        HashSet<String> varNamesAdded = new HashSet<>();
		
		ArrayList<Value> valuesList = new ArrayList<Value>();
		for (int i = 0; i < varNames.length; i++) {
            boolean found = false;
			for (int j = 0; j < st.getVariables().size(); j++) {
                if (st.getVariables().get(j).getKey().equals(varNames[i])) {
                    found = true;
                    if (varNamesAdded.contains(varNames[i])) {
                        Dna.logger.log(new LogEvent(Logger.WARNING, "Variable '" + varNames[i] + "' already added.", "The variable '" + varNames[i] + "' with value '" + values[i] + "' was already added to the statement. This variable will be ignored."));
                        break;
                    }
                    String dataType = st.getVariables().get(j).getDataType();
                    int variableId = st.getVariables().get(j).getVariableId();
                    Value v = null;
                    if (dataType.equals("short text")) {
                        Entity entity = new Entity((String) values[i]);
                        v = new Value(variableId, varNames[i], dataType, entity);
                    } else if (dataType.equals("long text")) {
                        v = new Value(variableId, varNames[i], dataType, (String) values[i]);
                    } else if (dataType.equals("integer") || dataType.equals("boolean")) {
                        if (dataType.equals("boolean") && (int) values[i] > 1) {
                            Dna.logger.log(new LogEvent(Logger.WARNING, "Invalid boolean value for variable '" + varNames[i] + "'.", "The variable '" + varNames[i] + "' is of type 'boolean' but was assigned the invalid value '" + values[i] + "'. Replacing value by 1 (true)."));
                            values[i] = 1;
                        } else if (dataType.equals("boolean") && (int) values[i] < 0) {
                            Dna.logger.log(new LogEvent(Logger.WARNING, "Invalid boolean value for variable '" + varNames[i] + "'.", "The variable '" + varNames[i] + "' is of type 'boolean' but was assigned the invalid value '" + values[i] + "'. Replacing value by 0 (false)."));
                            values[i] = 0;
                        }
                        v = new Value(variableId, varNames[i], dataType, (int) values[i]);
                    }
                    valuesList.add(v);
                    varNamesAdded.add(varNames[i]);
                    break;
                }
            }
            if (!found) {
                Dna.logger.log(new LogEvent(Logger.WARNING, "Variable '" + varNames[i] + "' not found in statement type '" + st.getLabel() + "'.", "The variable '" + varNames[i] + "' was not found in the statement type '" + st.getLabel() + "'. This variable will be ignored."));
            }
		}
        for (int i = 0; i < st.getVariables().size(); i++) {
            if (!varNamesAdded.contains(st.getVariables().get(i).getKey())) {
                Value v = new Value(st.getVariables().get(i));
                valuesList.add(v);
            }
        }
		Statement s = new Statement(startCaret, endCaret, statementTypeId, coderId, valuesList, documentId);
        int statementId = Dna.sql.addStatement(s);
        return statementId;
	}
}