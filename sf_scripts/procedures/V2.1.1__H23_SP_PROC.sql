USE SCHEMA H23_SCH;

CREATE PROCEDURE H23_SP_PROC()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var sql_CREATE_TEMP_command = "create or replace TEMPORARY TABLE H23_TEST_TEMP(ID INT,FNAME VARCHAR,LNAME VARCHAR,LOC VARCHAR)";
  var sql_INSERT_TEMP_command = "INSERT INTO H23_TEST_TEMP VALUES(10,'ABE','XYZ','REMOTE')";
  var sql_MERGE_command = "MERGE INTO H23_FINAL Tgt USING H23_TEST_TEMP Src ON (Src.ID = Tgt.ID) WHEN MATCHED THEN UPDATE SET Tgt.FNAME=Src.FNAME ,Tgt.LNAME=Src.LNAME ,Tgt.LOC=Src.LOC WHEN NOT MATCHED THEN INSERT (ID ,FNAME ,LNAME ,LOC ) VALUES (Src.ID ,Src.FNAME ,Src.LNAME ,Src.LOC)";			  
  var sql_DROP_TEMP_command = "DROP TABLE H23_TEST_TEMP";
     
  try{
	 snowflake.execute({sqlText: sql_CREATE_TEMP_command});
     
	 snowflake.execute({sqlText: sql_INSERT_TEMP_command});
	 
	 snowflake.execute({sqlText: sql_MERGE_command});
     
     snowflake.execute({sqlText: sql_DROP_TEMP_command});
     
	 return "Succeeded.";
     
	 }
	 
  catch (err) {
		return "Failed: " +err;
		}
		
$$;

CALL H23_SP_PROC();
